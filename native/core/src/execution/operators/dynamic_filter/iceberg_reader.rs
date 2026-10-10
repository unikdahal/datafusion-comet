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

mod predicate;

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::physical_expr::expressions::{Column, DynamicFilterPhysicalExpr};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions};
use datafusion_comet_operators::CometFilterExec;
use iceberg::arrow::{RuntimePredicateProvider, RuntimePredicateSnapshot};
use iceberg::{Error, ErrorKind, Result as IcebergResult};

use super::consumer::ReaderFilterConsumerExec;
use super::safety::is_safe_to_prune_before;
use crate::execution::operators::{IcebergScanExec, RuntimeScanOrder};
use predicate::extract_iceberg_predicate;

#[derive(Debug)]
struct IcebergRuntimePredicateProvider {
    predicate: Arc<DynamicFilterPhysicalExpr>,
    probe_column_index: usize,
    iceberg_field_name: String,
    /// The bound tightens toward large values (descending top-k, MAX).
    largest_first: bool,
}

impl IcebergRuntimePredicateProvider {
    fn new(
        predicate: Arc<DynamicFilterPhysicalExpr>,
        probe_column_index: usize,
        iceberg_field_name: String,
    ) -> Self {
        Self {
            predicate,
            probe_column_index,
            iceberg_field_name,
            largest_first: false,
        }
    }

    fn with_largest_first(mut self, largest_first: bool) -> Self {
        self.largest_first = largest_first;
        self
    }
}

impl RuntimePredicateProvider for IcebergRuntimePredicateProvider {
    fn generation(&self) -> u64 {
        self.predicate.snapshot_generation()
    }

    fn largest_first_column(&self) -> Option<String> {
        self.largest_first.then(|| self.iceberg_field_name.clone())
    }

    fn snapshot(&self) -> IcebergResult<RuntimePredicateSnapshot> {
        // DataFusion exposes the expression and generation through separate
        // reads. Only publish a snapshot bracketed by a stable generation.
        // Bound retries let a busy producer fail open without blocking a scan.
        for _ in 0..3 {
            let generation = self.generation();
            let current = self.predicate.current().map_err(|error| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to snapshot runtime predicate: {error}"),
                )
            })?;
            if self.generation() == generation {
                return Ok(RuntimePredicateSnapshot::new(
                    extract_iceberg_predicate(
                        &current,
                        self.probe_column_index,
                        &self.iceberg_field_name,
                    ),
                    generation,
                ));
            }
        }
        Err(Error::new(
            ErrorKind::Unexpected,
            "Runtime predicate changed while taking its snapshot",
        ))
    }
}

/// Whether `input` is a native Iceberg scan below expressions that reader pruning
/// can bypass without changing their values or suppressing evaluation errors.
pub(super) fn reaches_iceberg_reader(input: &Arc<dyn ExecutionPlan>) -> bool {
    if input.fetch().is_some() {
        return false;
    }
    if let Some(filter) = input.downcast_ref::<CometFilterExec>() {
        return !filter.has_projection()
            && is_safe_to_prune_before(filter.predicate(), filter.input().schema().as_ref())
            && reaches_iceberg_reader(filter.input());
    }
    if let Some(filter) = input.downcast_ref::<FilterExec>() {
        return is_safe_to_prune_before(filter.predicate(), filter.input().schema().as_ref())
            && reaches_iceberg_reader(filter.input());
    }
    if let Some(projection) = input.downcast_ref::<ProjectionExec>() {
        return is_passable_projection(projection) && reaches_iceberg_reader(projection.input());
    }
    input.is::<IcebergScanExec>()
}

/// A projection the reader can attach through: every expression is row-local and infallible.
fn is_passable_projection(projection: &ProjectionExec) -> bool {
    projection.expr().iter().all(|projected| {
        is_safe_to_prune_before(&projected.expr, projection.input().schema().as_ref())
    })
}

/// Attaches `predicate` to the Iceberg scan below `input`. With `order`, the scan also reads
/// its file tasks best-first for that direction (see `order_tasks_for_runtime_bound`).
pub(super) fn try_attach_iceberg_reader_filter(
    input: &Arc<dyn ExecutionPlan>,
    predicate: Arc<DynamicFilterPhysicalExpr>,
    order: Option<RuntimeScanOrder>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // A multi-key TopK filter lists every sort key; only the first one bounds the scan.
    let children = predicate.children();
    let Some(column) = children
        .first()
        .and_then(|child| child.downcast_ref::<Column>())
    else {
        return Ok(None);
    };
    let probe = ProbeColumn {
        predicate_index: column.index(),
        index: column.index(),
        name: column.name().to_string(),
    };
    attach_below(input, &predicate, order, probe)
}

/// Connect a join producer and its reader as one attachment. The consumer is
/// visible at the original probe schema, before any projection mapping, while
/// the provider translates the same publication into the reader field identity.
pub(super) fn try_attach_iceberg_join_filter(
    input: &Arc<dyn ExecutionPlan>,
    predicate: Arc<DynamicFilterPhysicalExpr>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    Ok(
        try_attach_iceberg_reader_filter(input, Arc::clone(&predicate), None)?.map(|reader| {
            Arc::new(ReaderFilterConsumerExec::new(reader, predicate)) as Arc<dyn ExecutionPlan>
        }),
    )
}

/// The probe key: its index in the producer's input, which the published predicate refers
/// to, and its index and name in the plan node currently being descended.
struct ProbeColumn {
    predicate_index: usize,
    index: usize,
    name: String,
}

fn attach_below(
    input: &Arc<dyn ExecutionPlan>,
    predicate: &Arc<DynamicFilterPhysicalExpr>,
    order: Option<RuntimeScanOrder>,
    probe: ProbeColumn,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if input.fetch().is_some() {
        return Ok(None);
    }

    if let Some(filter) = input.downcast_ref::<CometFilterExec>() {
        if filter.has_projection()
            || !is_safe_to_prune_before(filter.predicate(), filter.input().schema().as_ref())
        {
            return Ok(None);
        }
        let Some(reader) = attach_below(filter.input(), predicate, order, probe)? else {
            return Ok(None);
        };
        return match filter.with_execution_input(reader) {
            Ok(updated) => Ok(Some(updated)),
            Err(error) => {
                log::debug!("Iceberg runtime predicate attachment skipped: {error}");
                Ok(None)
            }
        };
    }

    // The planner builds DataFusion filters for ordinary Spark filters; only join probes are
    // converted to CometFilterExec.
    if let Some(filter) = input.downcast_ref::<FilterExec>() {
        if !is_safe_to_prune_before(filter.predicate(), filter.input().schema().as_ref()) {
            return Ok(None);
        }
        // A projection over a filter is folded into the filter's own output projection;
        // follow the key through it to its column in the filter's input.
        let below = match filter.projection() {
            None => probe,
            Some(projection) => {
                let Some(&index) = projection.get(probe.index) else {
                    return Ok(None);
                };
                ProbeColumn {
                    predicate_index: probe.predicate_index,
                    index,
                    name: filter.input().schema().field(index).name().to_string(),
                }
            }
        };
        let Some(reader) = attach_below(filter.input(), predicate, order, below)? else {
            return Ok(None);
        };
        return Ok(Some(Arc::clone(input).replace_children(
            vec![reader],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?));
    }

    if let Some(projection) = input.downcast_ref::<ProjectionExec>() {
        // Follow the key through a column reference to its position below the projection.
        let Some(source) = projection
            .expr()
            .get(probe.index)
            .and_then(|projected| projected.expr.downcast_ref::<Column>())
        else {
            return Ok(None);
        };
        if !is_passable_projection(projection) {
            return Ok(None);
        }
        let below = ProbeColumn {
            predicate_index: probe.predicate_index,
            index: source.index(),
            name: source.name().to_string(),
        };
        let Some(reader) = attach_below(projection.input(), predicate, order, below)? else {
            return Ok(None);
        };
        return Ok(Some(Arc::clone(input).replace_children(
            vec![reader],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?));
    }

    let Some(scan) = input.downcast_ref::<IcebergScanExec>() else {
        return Ok(None);
    };
    if scan
        .schema()
        .fields()
        .get(probe.index)
        .is_none_or(|field| field.name() != &probe.name)
    {
        return Ok(None);
    }
    let Some((iceberg_field_id, iceberg_field_name)) = scan.runtime_predicate_field(probe.index)
    else {
        return Ok(None);
    };
    let provider: Arc<dyn RuntimePredicateProvider> = Arc::new(
        IcebergRuntimePredicateProvider::new(
            Arc::clone(predicate),
            probe.predicate_index,
            iceberg_field_name,
        )
        .with_largest_first(matches!(order, Some(RuntimeScanOrder::Descending { .. }))),
    );
    Ok(Some(Arc::new(scan.with_runtime_predicate_provider(
        provider,
        order.map(|order| (iceberg_field_id, order)),
    ))))
}

#[cfg(test)]
mod tests;
