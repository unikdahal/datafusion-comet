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

//! Connect a hash join's completed build domain to its probe input.
//!
//! Comet does not run DataFusion's physical optimizer, which normally connects
//! dynamic-filter producers and consumers. This targeted wiring filters probe
//! batches and lets direct Parquet and Iceberg readers use safe constraints for
//! pruning. The original join verifies matches, including hash collisions.
//! This leaves Spark's operator tree and partitioning intact and does
//! not cross Spark exchanges or JVM/Arrow boundaries.

use std::fmt::Formatter;
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{JoinType, NullEquality, Result, Statistics};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::{lit, Column, DynamicFilterPhysicalExpr};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::distribution_requirements::InputDistributionRequirements;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::statistics::{ChildStats, StatisticsArgs};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream,
};
use datafusion_comet_operators::CometFilterExec;
use futures::StreamExt;

use super::iceberg_reader::{reaches_iceberg_reader, try_attach_iceberg_join_filter};
use super::parquet_reader::{is_parquet_reader_key, try_attach_parquet_reader_filter};
use super::DynamicFilterExec;

/// A permanent plan must not own a completed join's filter or build accumulator:
/// those can retain the hash map after its stream-owned reservation is released.
/// Keep an unexecuted template here and create the producer and consumer together
/// for each stream. Only their metric handles are retained by the Spark plan.
#[derive(Debug)]
pub(crate) struct DynamicFilterJoinExec {
    template: HashJoinExec,
    config: ConfigOptions,
    metrics: ExecutionPlanMetricsSet,
}

/// Per-execution join state. The permanent plan keeps no live filter; this value
/// records whether this execution also connected its filter to a native reader.
struct RuntimeDynamicFilterJoin {
    join: HashJoinExec,
    reader_filter_attached: bool,
}

impl DynamicFilterJoinExec {
    /// Return no wrapper when the join cannot safely use a runtime filter.
    pub(crate) fn try_new(join: &HashJoinExec, config: &ConfigOptions) -> Result<Option<Self>> {
        if let Some(reason) = ineligible_reason(join, config)? {
            log::debug!("Join dynamic filter skipped: {reason}");
            return Ok(None);
        }
        Ok(Some(Self::new(join, config.clone())?))
    }

    fn new(join: &HashJoinExec, config: ConfigOptions) -> Result<Self> {
        Ok(Self {
            template: join.builder().reset_state().build()?,
            config,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn build_runtime_join(&self) -> Result<RuntimeDynamicFilterJoin> {
        let predicate = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&self.template.on()[0].1)],
            lit(true),
        ));
        let probe_key = &self.template.on()[0].1;
        let parquet_reader = if is_parquet_reader_key(probe_key, &self.template.right().schema()) {
            try_attach_parquet_reader_filter(
                self.template.right(),
                Arc::clone(&predicate),
                &self.config,
            )?
        } else {
            None
        };
        let (reader, iceberg_reader) = match parquet_reader {
            Some(reader) => (Some(reader), false),
            None => {
                let reader =
                    try_attach_iceberg_join_filter(self.template.right(), Arc::clone(&predicate))?;
                let attached = reader.is_some();
                (reader, attached)
            }
        };
        let reader_filter_attached = reader.is_some();
        let input = reader.unwrap_or_else(|| Arc::clone(self.template.right()));
        let consumer: Arc<dyn ExecutionPlan> = if iceberg_reader {
            // The reader rejects files, row groups and pages before decoding.
            // HashJoinExec then verifies the surviving rows exactly. A second
            // membership lookup on decoded batches cannot save any further IO
            // and would copy payload arrays only to probe the hash table again.
            input
        } else {
            Arc::new(DynamicFilterExec::new(
                input,
                Arc::clone(&predicate),
                self.metrics.clone(),
                "dynamic_filter_join",
            ))
        };
        // In particular, do not share CollectLeft's cached build future with the
        // template, another execution, or a reset plan.
        let join = self
            .template
            .builder()
            .reset_state()
            .with_new_children(vec![Arc::clone(self.template.left()), consumer])?
            .build()?
            .with_dynamic_filter_expr(predicate)?;
        Ok(RuntimeDynamicFilterJoin {
            join,
            reader_filter_attached,
        })
    }

    fn execute_runtime_join(
        &self,
        join: HashJoinExec,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // DataFusion materializes one IN-list literal per build row, although it admits the
        // list by packed-array bytes and distinct-key count. Admit only small lists, which
        // the Iceberg reader can turn into a membership predicate; a larger build side keeps
        // reusing the join's already-reserved hash table and preserves duplicates.
        let mut config = context.session_config().clone();
        config
            .options_mut()
            .optimizer
            .hash_join_inlist_pushdown_max_size = IN_LIST_PUSHDOWN_MAX_BYTES;
        config
            .options_mut()
            .optimizer
            .hash_join_inlist_pushdown_max_distinct_values = IN_LIST_PUSHDOWN_MAX_DISTINCT;
        let context = Arc::new(TaskContext::new(
            context.task_id(),
            context.session_id(),
            config,
            context.scalar_functions().clone(),
            context.higher_order_functions().clone(),
            context.aggregate_functions().clone(),
            context.window_functions().clone(),
            context.runtime_env(),
        ));
        let result = join.execute(partition, context);
        // HashJoinExec registers its metrics synchronously in execute(). Keep the
        // live counters, including on error, without retaining the producer plan.
        for metric in join.metrics().unwrap_or_default().iter() {
            self.metrics.register(Arc::clone(metric));
        }
        drop(join);
        let input = result?;
        // Drop execution state at EOF or error even if the caller retains the
        // exhausted stream. Dropping a pending stream also drops all of its state.
        let stream = futures::stream::unfold(Some(input), |input| async move {
            let mut input = input?;
            let batch = input.next().await?;
            let remaining = if batch.is_ok() { Some(input) } else { None };
            Some((batch, remaining))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

impl DisplayAs for DynamicFilterJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CometDynamicFilterJoinExec: ")?;
        self.template.fmt_as(t, f)
    }
}

impl ExecutionPlan for DynamicFilterJoinExec {
    fn name(&self) -> &str {
        "CometDynamicFilterJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.template.properties()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        self.template.input_distribution_requirements()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.template.maintains_input_order()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.template.children()
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.template.apply_expressions(f)
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
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let join = self
            .template
            .builder()
            .reset_state()
            .with_new_children(children)?
            .build()?;
        match Self::try_new(&join, &self.config)? {
            Some(wrapper) => Ok(Arc::new(wrapper)),
            None => Ok(Arc::new(join)),
        }
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(&self.template, self.config.clone())?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let runtime = self.build_runtime_join()?;
        let attachment_metric = if runtime.reader_filter_attached {
            "dynamic_filter_join_filters_attached"
        } else {
            "dynamic_filter_join_filters_skipped"
        };
        MetricBuilder::new(&self.metrics)
            .counter(attachment_metric, partition)
            .add(1);
        self.execute_runtime_join(runtime.join, partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        self.template.child_stats_requests(partition)
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.template.statistics_from_inputs(input_stats, args)
    }
}

/// Largest packed build-side key array, and most distinct keys, DataFusion may turn into an
/// IN list. At most a few thousand literals per task, so the unreserved allocation stays small.
const IN_LIST_PUSHDOWN_MAX_BYTES: usize = 16 * 1024;
const IN_LIST_PUSHDOWN_MAX_DISTINCT: usize = 1024;

fn ineligible_reason(join: &HashJoinExec, config: &ConfigOptions) -> Result<Option<&'static str>> {
    if !config.optimizer.enable_dynamic_filter_pushdown
        || !config.optimizer.enable_join_dynamic_filter_pushdown
    {
        return Ok(Some("disabled by DataFusion session options"));
    }
    // The build side is the left input here. A probe row without a build match never reaches
    // the output of these join types, so dropping it early cannot change the result. Outer
    // and anti joins on the probe side emit such rows and stay unfiltered.
    if !matches!(
        join.join_type(),
        JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
    ) || join.null_equality() != NullEquality::NullEqualsNothing
    {
        return Ok(Some("only inner and semi equijoins are supported"));
    }
    if !matches!(
        join.partition_mode(),
        PartitionMode::Partitioned | PartitionMode::CollectLeft
    ) {
        return Ok(Some("unresolved hash join partition mode"));
    }
    if config.optimizer.preserve_file_partitions > 0
        && matches!(join.partition_mode(), PartitionMode::Partitioned)
    {
        return Ok(Some("DataFusion preserve_file_partitions is enabled"));
    }
    // Spark, not DataFusion, routes rows across tasks. Restrict the filter to the
    // single native partition executed by this Spark task: no shared domains or
    // assumptions about DataFusion's repartition hash across Spark partitions.
    if join.left().output_partitioning().partition_count() != 1
        || join.right().output_partitioning().partition_count() != 1
    {
        return Ok(Some("requires one native partition per input"));
    }
    let [(build_key, probe_key)] = join.on() else {
        return Ok(Some("requires one join key"));
    };
    if !build_key.is::<Column>() || !probe_key.is::<Column>() {
        return Ok(Some("computed join keys are not supported"));
    }
    let build_type = build_key.data_type(join.left().schema().as_ref())?;
    let probe_type = probe_key.data_type(join.right().schema().as_ref())?;
    if build_type != probe_type
        || !(is_string_key_type(&build_type)
            || matches!(
                build_type,
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Date32
                    | DataType::Timestamp(TimeUnit::Microsecond, _)
                    | DataType::Decimal128(_, _)
            ))
    {
        return Ok(Some(
            "requires matching integer, date, timestamp, decimal or string keys",
        ));
    }
    if (matches!(build_type, DataType::Decimal128(_, _)) || is_string_key_type(&build_type))
        && (!is_direct_pruning_key(join.left(), build_key)
            || !is_direct_pruning_key(join.right(), probe_key))
    {
        return Ok(Some(
            "computed or annotated decimal/string join keys are not supported",
        ));
    }
    // Decimal batches support exact bounds and membership on every probe backend.
    // Date, timestamp and string keys require native Iceberg probes.
    if !is_parquet_reader_key(probe_key, &join.right().schema())
        && !matches!(probe_type, DataType::Decimal128(_, _))
        && !reaches_iceberg_reader(join.right())
    {
        return Ok(Some(
            "date, timestamp and string keys require a native Iceberg probe",
        ));
    }
    Ok(None)
}

/// String dictionary values compare by their bytes, never by dictionary indices.
/// Keep the exact Arrow type match above, including the dictionary key/value types;
/// Utf8/Utf8View coercions must be materialized before the join, not inferred here.
fn is_string_key_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
        DataType::Dictionary(_, value) => {
            matches!(
                value.as_ref(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            )
        }
        _ => false,
    }
}

/// A cast or padding expression in a projection is still a computed pruning key,
/// even when the join itself sees only the projection's output column.
/// Spark exchange/JVM inputs are already materialized columns in the join's domain.
/// Reader attachment stops at those boundaries; it cannot reach a pre-cast scan.
/// In particular, a lossless build upcast below a broadcast exchange cannot change
/// the probe reader's scale, so Spark-side lineage tracking adds no safety here.
fn is_direct_pruning_key(input: &Arc<dyn ExecutionPlan>, key: &Arc<dyn PhysicalExpr>) -> bool {
    use datafusion::physical_plan::filter::FilterExec;

    let Some(column) = key.downcast_ref::<Column>() else {
        return false;
    };
    // Spark rejects collations/CHAR before serde; Arrow has no collation ID.
    // If an external/native schema supplies semantic annotations, fail closed.
    let schema = input.schema();
    let field = schema.field(column.index());
    if is_string_key_type(field.data_type()) {
        let metadata = field.metadata();
        let annotated_comparison = ["__COLLATIONS", "ARROW:extension:name"]
            .iter()
            .any(|name| metadata.contains_key(*name));
        // VARCHAR constrains writes but does not pad comparisons. CHAR and unknown
        // raw type annotations cannot prove byte equality of the stored values.
        let padded_or_unknown = metadata
            .get("__CHAR_VARCHAR_TYPE_STRING")
            .is_some_and(|raw_type| !raw_type.to_ascii_lowercase().starts_with("varchar("));
        if annotated_comparison || padded_or_unknown {
            return false;
        }
    }
    if let Some(projection) = input.downcast_ref::<ProjectionExec>() {
        return projection
            .expr()
            .get(column.index())
            .is_some_and(|projected| {
                projected.expr.is::<Column>()
                    && is_direct_pruning_key(projection.input(), &projected.expr)
            });
    }
    if let Some(filter) = input.downcast_ref::<CometFilterExec>() {
        return !filter.has_projection() && is_direct_pruning_key(filter.input(), key);
    }
    if let Some(filter) = input.downcast_ref::<FilterExec>() {
        return filter.projection().is_none() && is_direct_pruning_key(filter.input(), key);
    }
    true
}

#[cfg(test)]
mod tests;
