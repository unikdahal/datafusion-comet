// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Exact membership filtering of decoded batches for non-Iceberg join inputs.

use std::fmt::Formatter;
use std::sync::Arc;

use arrow::array::Array;
use arrow::compute::filter_record_batch;
use datafusion::common::cast::as_boolean_array;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_err, Result, ScalarValue};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::expressions::{lit, Column, DynamicFilterPhysicalExpr};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    apply_expression_roots, ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
    PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream,
};
use futures::StreamExt;

/// Observe eight typical 8,192-row batches before deciding that evaluation and
/// copying cost more than the avoided probes. Rows keep the window independent
/// of the input batch size, and small selective inputs are always evaluated.
const SELECTIVITY_WINDOW_ROWS: usize = 65_536;

/// Require at least 10% pruning to keep evaluating every batch. This conservative
/// cutoff avoids paying membership and copy costs for almost unchanged inputs
/// while retaining filtering for selective joins, including 25% pruning.
const MIN_PRUNED_FRACTION: f64 = 0.10;

/// Evaluate every sixteenth batch while bypassing: about 6.25% of the evaluation
/// work, with at most fifteen skipped batches before detecting a selective suffix
/// of sorted input. A producer update forces evaluation without waiting to sample.
const BYPASS_SAMPLE_INTERVAL: usize = 16;

/// Selectivity feedback belongs to one partition stream, never to the plan.
#[derive(Default)]
struct SelectivityState {
    rows: usize,
    pruned: usize,
    bypassing: bool,
    batches_since_sample: usize,
}

impl SelectivityState {
    fn should_bypass(&mut self) -> bool {
        if !self.bypassing {
            return false;
        }
        self.batches_since_sample += 1;
        if self.batches_since_sample < BYPASS_SAMPLE_INTERVAL {
            return true;
        }
        self.batches_since_sample = 0;
        false
    }

    /// Returns true only when entering bypass, for the transition counter.
    fn record(&mut self, rows: usize, pruned: usize) -> bool {
        if rows == 0 {
            return false;
        }
        if self.bypassing {
            if (pruned as f64 / rows as f64) < MIN_PRUNED_FRACTION {
                return false;
            }
            // A selective sample starts a fresh evaluation window.
            *self = Self::default();
        }
        self.rows += rows;
        self.pruned += pruned;
        if self.rows < SELECTIVITY_WINDOW_ROWS {
            return false;
        }
        self.bypassing = (self.pruned as f64 / self.rows as f64) < MIN_PRUNED_FRACTION;
        self.rows = 0;
        self.pruned = 0;
        self.bypassing
    }
}

/// A task-local consumer of a live runtime predicate.
#[derive(Debug)]
pub(crate) struct DynamicFilterExec {
    input: Arc<dyn ExecutionPlan>,
    predicate: Arc<DynamicFilterPhysicalExpr>,
    metrics: ExecutionPlanMetricsSet,
    metric_prefix: &'static str,
}

impl DynamicFilterExec {
    pub(super) fn new(
        input: Arc<dyn ExecutionPlan>,
        predicate: Arc<DynamicFilterPhysicalExpr>,
        metrics: ExecutionPlanMetricsSet,
        metric_prefix: &'static str,
    ) -> Self {
        Self {
            input,
            predicate,
            metrics,
            metric_prefix,
        }
    }
}

impl DisplayAs for DynamicFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CometDynamicFilterExec")
    }
}

impl ExecutionPlan for DynamicFilterExec {
    fn name(&self) -> &str {
        "CometDynamicFilterExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        // Removing rows preserves the input's schema, ordering and partitioning.
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots([Arc::clone(&self.predicate) as Arc<dyn PhysicalExpr>], f)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("CometDynamicFilterExec requires one child");
        }
        Ok(Arc::new(Self {
            input: children.remove(0),
            predicate: Arc::clone(&self.predicate),
            metrics: ExecutionPlanMetricsSet::new(),
            metric_prefix: self.metric_prefix,
        }))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        // HashJoinExec resets its producer on reexecution. Never retain a previous
        // build's domain in the consumer. A reset plan safely bypasses filtering;
        // ordinary Spark task attempts each construct a fresh, connected plan.
        let predicate = Arc::new(DynamicFilterPhysicalExpr::new(
            self.predicate.children().into_iter().cloned().collect(),
            lit(true),
        ));
        Ok(Arc::new(Self {
            input: super::parquet_reader::reset_parquet_reader_filter(
                Arc::clone(&self.input),
                &self.predicate,
            )?,
            predicate,
            metrics: ExecutionPlanMetricsSet::new(),
            metric_prefix: self.metric_prefix,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let children = self.predicate.children();
        let [key] = children.as_slice() else {
            return internal_err!("CometDynamicFilterExec requires one join-key column");
        };
        let Some(key) = key.downcast_ref::<Column>() else {
            return internal_err!("CometDynamicFilterExec requires a direct join-key column");
        };
        let key_index = key.index();
        let predicate = Arc::clone(&self.predicate)
            .with_new_children(vec![Arc::new(Column::new(key.name(), 0))])?;
        let input = self.input.execute(partition, context)?;
        let evaluated = MetricBuilder::new(&self.metrics)
            .counter(format!("{}_rows_evaluated", self.metric_prefix), partition);
        let pruned = MetricBuilder::new(&self.metrics)
            .counter(format!("{}_rows_pruned", self.metric_prefix), partition);
        let bypassed = MetricBuilder::new(&self.metrics)
            .counter(format!("{}_rows_bypassed", self.metric_prefix), partition);
        let bypass_switches = MetricBuilder::new(&self.metrics)
            .counter(format!("{}_bypass_switches", self.metric_prefix), partition);
        // Only dedicated metrics: merging this helper into the Spark join must not
        // add its input/output counts or elapsed time to the join's existing metrics.
        let eval_time = MetricBuilder::new(&self.metrics)
            .subset_time(format!("{}_eval_time", self.metric_prefix), partition);
        let mut selectivity = SelectivityState::default();
        let mut generation = predicate.snapshot_generation();
        let stream = input.map(move |batch| {
            let batch = batch?;
            // This is a single dynamic expression: its generation is shared by
            // the producer and the remapped consumer. Never let old feedback hide
            // a newly populated or tightened predicate. A concurrent update after
            // this check is observed on the next batch; bypass remains advisory.
            let current_generation = predicate.snapshot_generation();
            if current_generation != generation {
                selectivity = SelectivityState::default();
                generation = current_generation;
            }
            if selectivity.should_bypass() {
                bypassed.add(batch.num_rows());
                return Ok(batch);
            }
            let _timer = eval_time.timer();
            // AND may prefilter its input before evaluating hash membership. A
            // zero-copy key projection keeps payload columns out of that temporary
            // batch. The remapped expression still observes live producer updates.
            let key_batch = batch.project(&[key_index])?;
            match predicate.evaluate(&key_batch)? {
                // DataFusion leaves this placeholder unchanged until the complete
                // build is available, or if it declines to populate the filter.
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => {
                    bypassed.add(batch.num_rows());
                    Ok(batch)
                }
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(false) | None)) => {
                    evaluated.add(batch.num_rows());
                    pruned.add(batch.num_rows());
                    if selectivity.record(batch.num_rows(), batch.num_rows()) {
                        bypass_switches.add(1);
                    }
                    Ok(batch.slice(0, 0))
                }
                ColumnarValue::Array(mask) => {
                    let mask = as_boolean_array(&mask)?;
                    let rows = batch.num_rows();
                    let true_count = mask.true_count();
                    let filtered = if mask.null_count() == 0 && true_count == rows {
                        batch
                    } else if true_count == 0 {
                        batch.slice(0, 0)
                    } else {
                        filter_record_batch(&batch, mask)?
                    };
                    let rows_pruned = rows - filtered.num_rows();
                    evaluated.add(rows);
                    pruned.add(rows_pruned);
                    if selectivity.record(rows, rows_pruned) {
                        bypass_switches.add(1);
                    }
                    Ok(filtered)
                }
                _ => internal_err!("Join dynamic filter must evaluate to a Boolean"),
            }
        });
        // Return even empty batches. Each poll consumes at most one input batch,
        // so a selective filter cannot drain a ready input in an unbounded loop.
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests;
