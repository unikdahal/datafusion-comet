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

//! Producer discovery for readers whose predicate provider is opaque to DataFusion.

use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{internal_err, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::{reset_plan_states, CardinalityEffect};
use datafusion::physical_plan::{
    apply_expression_roots, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};

/// Connects a producer expression to its reader without evaluating decoded rows.
///
/// Kept at the producer's input schema so projection traversal cannot expose an
/// expression with column indices belonging to another schema. Constructed by
/// the reader attachment path with the same predicate as the provider.
#[derive(Debug)]
pub(super) struct ReaderFilterConsumerExec {
    pub(super) input: Arc<dyn ExecutionPlan>,
    predicate: Arc<DynamicFilterPhysicalExpr>,
}

impl ReaderFilterConsumerExec {
    pub(super) fn new(
        input: Arc<dyn ExecutionPlan>,
        predicate: Arc<DynamicFilterPhysicalExpr>,
    ) -> Self {
        Self { input, predicate }
    }
}

impl DisplayAs for ReaderFilterConsumerExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CometReaderFilterConsumerExec")
    }
}

impl ExecutionPlan for ReaderFilterConsumerExec {
    fn name(&self) -> &str {
        "CometReaderFilterConsumerExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
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
        CardinalityEffect::Equal
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("CometReaderFilterConsumerExec requires one child");
        }
        Ok(Arc::new(Self::new(
            children.remove(0),
            Arc::clone(&self.predicate),
        )))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        // Drop the discovery expression together with its reader provider.
        // The execution wrapper reconnects a fresh producer on the next run.
        reset_plan_states(Arc::clone(&self.input))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}
