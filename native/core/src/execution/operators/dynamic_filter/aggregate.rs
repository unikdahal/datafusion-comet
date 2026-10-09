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

//! Attach one partial MIN/MAX producer to a native Iceberg scan.

use std::fmt::Formatter;
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
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
use super::join::{is_direct_pruning_key, is_string_key_type};
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
        is_string_key_type(data_type)
            || matches!(
                data_type,
                DataType::Int32
                    | DataType::Int64
                    | DataType::Date32
                    | DataType::Timestamp(TimeUnit::Microsecond, _)
            )
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
        if !argument.is::<Column>()
            || !Self::supports_argument_type(&argument_type)
            || (is_string_key_type(&argument_type)
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
        )?
        .with_limit_options(template.limit_options()))
    }

    /// MIN tightens toward small values and MAX toward large ones; nulls never qualify.
    fn scan_order(aggregate: &AggregateExec) -> Option<RuntimeScanOrder> {
        match aggregate.aggr_expr()[0]
            .fun()
            .name()
            .to_ascii_lowercase()
            .as_str()
        {
            "min" => Some(RuntimeScanOrder::Ascending { nulls_first: false }),
            "max" => Some(RuntimeScanOrder::Descending { nulls_first: false }),
            _ => None,
        }
    }

    fn producer(aggregate: &AggregateExec) -> Arc<DynamicFilterPhysicalExpr> {
        Arc::new(DynamicFilterPhysicalExpr::new(
            aggregate.aggr_expr()[0].expressions(),
            lit(true),
        ))
    }

    fn build_runtime_aggregate(&self) -> Result<AggregateExec> {
        let aggregate = Self::fresh_aggregate(&self.template, Arc::clone(self.template.input()))?;
        let predicate = Self::producer(&aggregate);
        let input = try_attach_iceberg_reader_filter(
            aggregate.input(),
            Arc::clone(&predicate),
            Self::scan_order(&aggregate),
        )?
        .ok_or_else(|| {
            datafusion::common::internal_datafusion_err!(
                "Eligible MIN/MAX aggregate lost its Iceberg reader"
            )
        })?;
        Self::fresh_aggregate(&aggregate, input)?.with_dynamic_filter_expr(predicate)
    }
}

impl DisplayAs for IcebergMinMaxFilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CometIcebergMinMaxFilterExec: ")?;
        self.template.fmt_as(t, f)
    }
}

impl ExecutionPlan for IcebergMinMaxFilterExec {
    fn name(&self) -> &str {
        "CometIcebergMinMaxFilterExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.template.properties()
    }
    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        self.template.input_distribution_requirements()
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
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("CometIcebergMinMaxFilterExec requires one child");
        }
        let aggregate = Self::fresh_aggregate(&self.template, children.remove(0))?;
        Ok(match Self::try_new(&aggregate, &self.config)? {
            Some(wrapper) => Arc::new(wrapper),
            None => Arc::new(aggregate),
        })
    }
    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let fresh = Self::fresh_aggregate(&self.template, Arc::clone(self.template.input()))?;
        Self::try_new(&fresh, &self.config)?
            .map(|wrapper| Arc::new(wrapper) as Arc<dyn ExecutionPlan>)
            .ok_or_else(|| {
                datafusion::common::internal_datafusion_err!(
                    "MIN/MAX eligibility changed during reset"
                )
            })
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let aggregate = self.build_runtime_aggregate()?;
        MetricBuilder::new(&self.metrics)
            .counter("dynamic_filter_minmax_filters_attached", partition)
            .add(1);
        let result = aggregate.execute(partition, context);
        // Each execution builds a fresh aggregate. Its metrics are summed by
        // name when reported, so repeated executions accumulate correctly.
        for metric in aggregate.metrics().unwrap_or_default().iter() {
            self.metrics.register(Arc::clone(metric));
        }
        drop(aggregate);
        let input = result?;
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
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests;
