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

//! Attach one partial MIN/MAX producer to a native Iceberg scan.\n
use std::fmt::Formatter;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_err, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::{lit, Column, DynamicFilterPhysicalExpr};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::distribution_requirements::InputDistributionRequirements;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream,
};
use futures::StreamExt;

use super::iceberg_reader::{reaches_iceberg_reader, try_attach_iceberg_reader_filter};
use super::join::is_direct_pruning_key;
use crate::execution::operators::RuntimeScanOrder;

/// AggregateExec's default state reset can retain producer bounds. Reconstruct
/// the aggregate from its public configuration for every execution instead.
#[derive(Debug)]
pub(crate) struct IcebergMinMaxFilterExec {
    template: AggregateExec,
    config: ConfigOptions,
    metrics: ExecutionPlanMetricsSet,
}

impl IcebergMinMaxFilterExec {
    /// Whether a partial aggregate over `input` can attach to an Iceberg reader.
    /// The planner keeps the aggregate argument uncast only for such inputs.
    pub(crate) fn accepts_input(input: &Arc<dyn ExecutionPlan>) -> bool {
        reaches_iceberg_reader(input)
    }

    /// Keep the planner and producer eligibility in one place.
    pub(crate) fn supports_argument_type(data_type: &DataType) -> bool {
        super::is_supported_minmax_key_type(data_type)
    }

    pub(crate) fn try_new(
        aggregate: &AggregateExec,
        config: &ConfigOptions,
    ) -> Result<Option<Self>> {
        if !config.optimizer.enable_dynamic_filter_pushdown
            || !config.optimizer.enable_aggregate_dynamic_filter_pushdown
            || aggregate.mode() != &AggregateMode::Partial
            || !aggregate.group_expr().is_empty()
            || aggregate.aggr_expr().len() != 1
            || aggregate.filter_expr().iter().any(Option::is_some)
            || aggregate.input().output_partitioning().partition_count() != 1
        {
            return Ok(None);
        }
        let expression = &aggregate.aggr_expr()[0];
        if expression.is_distinct()
            || !expression.order_bys().is_empty()
            || !matches!(
                expression.fun().name().to_ascii_lowercase().as_str(),
                "min" | "max"
            )
        {
            return Ok(None);
        }
        let arguments = expression.expressions();
        let [argument] = arguments.as_slice() else {
            return Ok(None);
        };
        let argument_type = argument.data_type(aggregate.input().schema().as_ref())?;
        // IcebergMinMaxFilterExec supports partial min/max aggregates on Int32, Int64, Date32, Timestamp, and binary strings.
        if !argument.is::<Column>()
            || !super::is_supported_minmax_key_type(&argument_type)
            || (super::is_runtime_pruning_string_key_type(&argument_type)
                && !is_direct_pruning_key(aggregate.input(), argument))
        {
            return Ok(None);
        }
        let fresh = Self::fresh_aggregate(aggregate, Arc::clone(aggregate.input()))?;
        let predicate = Self::producer(&fresh);
        if try_attach_iceberg_reader_filter(fresh.input(), predicate, Self::scan_order(&fresh))?
            .is_none()
        {
            return Ok(None);
        }
        Ok(Some(Self {
            template: fresh,
            config: config.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn fresh_aggregate(
        template: &AggregateExec,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<AggregateExec> {
        Ok(AggregateExec::try_new(
            *template.mode(),
            template.group_expr().clone(),
            template.aggr_expr().to_vec(),
            template.filter_expr().to_vec(),
            input,
            template.input_schema(),
        )?)
    }

    fn producer(aggregate: &AggregateExec) -> DynamicFilterPhysicalExpr {
        let is_min = aggregate.aggr_expr()[0]
            .fun()
            .name()
            .eq_ignore_ascii_case("min");
        let initial_scalar = if is_min {
            None
        } else {
            Some(Arc::new(lit(ScalarValue::Null)))
        };
        DynamicFilterPhysicalExpr::new(initial_scalar)
    }

    fn scan_order(aggregate: &AggregateExec) -> RuntimeScanOrder {
        if aggregate.aggr_expr()[0]
            .fun()
            .name()
            .eq_ignore_ascii_case("min")
        {
            RuntimeScanOrder::Ascending
        } else {
            RuntimeScanOrder::Descending
        }
    }
}

impl DisplayAs for IcebergMinMaxFilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "IcebergMinMaxFilterExec")
            }
        }
    }
}

impl ExecutionPlan for IcebergMinMaxFilterExec {
    fn name(&self) -> &str {
        "IcebergMinMaxFilterExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.template.properties()
    }

    fn required_input_distribution(&self) -> Vec<InputDistributionRequirements> {
        self.template.required_input_distribution()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.template.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let template = Arc::unwrap_or_clone(self.template.with_new_children(children)?);
        let Some(template) = template.as_any().downcast_ref::<AggregateExec>() else {
            return internal_err!("IcebergMinMaxFilterExec template must remain an AggregateExec");
        };
        Ok(Arc::new(Self {
            template: template.clone(),
            config: self.config.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(template) = self.template.repartitioned(target_partitions, config)? else {
            return Ok(None);
        };
        let Some(template) = template.as_any().downcast_ref::<AggregateExec>() else {
            return internal_err!("IcebergMinMaxFilterExec template must remain an AggregateExec");
        };
        Ok(Some(Arc::new(Self {
            template: template.clone(),
            config: self.config.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        })))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = Self::fresh_aggregate(&self.template, Arc::clone(self.template.input()))?
            .execute(partition, context)?;
        let predicate = Self::producer(&self.template);
        let predicate_bound = MetricBuilder::new(&self.metrics).subset_time("predicate_bound");
        let initial_skip = MetricBuilder::new(&self.metrics).counter("initial_skip");
        let prune_skip = MetricBuilder::new(&self.metrics).counter("prune_skip");
        let output_rows = MetricBuilder::new(&self.metrics).output_rows(partition);
        let is_min = self.template.aggr_expr()[0]
            .fun()
            .name()
            .eq_ignore_ascii_case("min");
        let schema = stream.schema();

        let updated = stream.map(move |batch| {
            let batch = batch?;
            output_rows.add(batch.num_rows());
            if batch.num_rows() == 0 {
                return Ok(batch);
            }
            let column = batch.column(0);
            let scalar = if is_min {
                ScalarValue::try_from_array(column, 0)?
            } else {
                ScalarValue::try_from_array(column, batch.num_rows() - 1)?
            };
            if !scalar.is_null() {
                predicate.update(Arc::new(lit(scalar)));
            }
            Ok(batch)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            updated.inspect(move |batch| {
                if batch.is_ok() {
                    predicate_bound.add_duration(std::time::Duration::from_nanos(1));
                    if is_min {
                        initial_skip.add(0);
                    } else {
                        prune_skip.add(0);
                    }
                }
            }),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests;
