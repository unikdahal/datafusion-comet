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

use std::sync::Arc;

use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, IsNotNullExpr, IsNullExpr, Literal,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_comet_operators::CometFilterExec;
use iceberg::arrow::{RuntimePredicateProvider, RuntimePredicateSnapshot};
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;
use iceberg::{Error, ErrorKind, Result as IcebergResult};

use super::parquet_reader::is_direct_column_null_checks;
use crate::execution::operators::IcebergScanExec;

#[derive(Debug)]
struct IcebergRuntimePredicateProvider {
    predicate: Arc<DynamicFilterPhysicalExpr>,
    probe_column_index: usize,
    iceberg_field_name: String,
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
        }
    }
}

impl RuntimePredicateProvider for IcebergRuntimePredicateProvider {
    fn generation(&self) -> u64 {
        self.predicate.snapshot_generation()
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

/// Whether `input` is a native Iceberg scan, possibly below projection-free
/// direct-column null checks, that [`try_attach_iceberg_reader_filter`] can reach.
pub(super) fn reaches_iceberg_reader(input: &Arc<dyn ExecutionPlan>) -> bool {
    if input.fetch().is_some() {
        return false;
    }
    if let Some(filter) = input.downcast_ref::<CometFilterExec>() {
        return !filter.has_projection()
            && is_direct_column_null_checks(filter.predicate())
            && reaches_iceberg_reader(filter.input());
    }
    input.is::<IcebergScanExec>()
}

pub(super) fn try_attach_iceberg_reader_filter(
    input: &Arc<dyn ExecutionPlan>,
    predicate: Arc<DynamicFilterPhysicalExpr>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if input.fetch().is_some() {
        return Ok(None);
    }

    if let Some(filter) = input.downcast_ref::<CometFilterExec>() {
        if filter.has_projection() || !is_direct_column_null_checks(filter.predicate()) {
            return Ok(None);
        }
        let Some(reader) =
            try_attach_iceberg_reader_filter(filter.input(), Arc::clone(&predicate))?
        else {
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

    let Some(scan) = input.downcast_ref::<IcebergScanExec>() else {
        return Ok(None);
    };
    let children = predicate.children();
    let [child] = children.as_slice() else {
        return Ok(None);
    };
    let Some(column) = child.downcast_ref::<Column>() else {
        return Ok(None);
    };

    if scan
        .schema()
        .fields()
        .get(column.index())
        .is_none_or(|field| field.name() != column.name())
    {
        return Ok(None);
    }
    let Some(iceberg_field_name) = scan.runtime_predicate_field_name(column.index()) else {
        return Ok(None);
    };
    let provider: Arc<dyn RuntimePredicateProvider> =
        Arc::new(IcebergRuntimePredicateProvider::new(
            Arc::clone(&predicate),
            column.index(),
            iceberg_field_name,
        ));
    Ok(Some(Arc::new(
        scan.with_runtime_predicate_provider(provider),
    )))
}

fn extract_iceberg_predicate(
    expr: &Arc<dyn PhysicalExpr>,
    probe_column_index: usize,
    iceberg_field_name: &str,
) -> Option<Predicate> {
    // An AND permits a conservative subset of its constraints. Do not descend
    // through arbitrary OR, NOT, casts, or exact hash membership.
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        if binary.op() == &Operator::And {
            return match (
                extract_iceberg_predicate(binary.left(), probe_column_index, iceberg_field_name),
                extract_iceberg_predicate(binary.right(), probe_column_index, iceberg_field_name),
            ) {
                (Some(left), Some(right)) => Some(left.and(right)),
                (Some(predicate), None) | (None, Some(predicate)) => Some(predicate),
                (None, None) => None,
            };
        }
        if binary.op() == &Operator::Or {
            // DataFusion's single-key NULLS FIRST TopK emits exactly
            // IS NULL(key) OR key < / > threshold. Both arms must be
            // translated completely; an arbitrary OR still fails open.
            let null = binary.left().downcast_ref::<IsNullExpr>()?;
            if null.arg().downcast_ref::<Column>()?.index() != probe_column_index {
                return None;
            }
            let comparison = binary.right().downcast_ref::<BinaryExpr>()?;
            if !matches!(comparison.op(), Operator::Lt | Operator::Gt) {
                return None;
            }
            return extract_bound(comparison, probe_column_index, iceberg_field_name)
                .map(|bound| Reference::new(iceberg_field_name).is_null().or(bound));
        }
        return extract_bound(binary, probe_column_index, iceberg_field_name);
    }

    if let Some(check) = expr.downcast_ref::<IsNullExpr>() {
        return (check.arg().downcast_ref::<Column>()?.index() == probe_column_index)
            .then(|| Reference::new(iceberg_field_name).is_null());
    }
    if let Some(check) = expr.downcast_ref::<IsNotNullExpr>() {
        return (check.arg().downcast_ref::<Column>()?.index() == probe_column_index)
            .then(|| Reference::new(iceberg_field_name).is_not_null());
    }

    expr.downcast_ref::<Literal>()
        .and_then(|literal| match literal.value() {
            ScalarValue::Boolean(Some(false)) => Some(Predicate::AlwaysFalse),
            _ => None,
        })
}

fn extract_bound(
    binary: &BinaryExpr,
    probe_column_index: usize,
    iceberg_field_name: &str,
) -> Option<Predicate> {
    let column = binary.left().downcast_ref::<Column>()?;
    if column.index() != probe_column_index {
        return None;
    }
    let literal = binary.right().downcast_ref::<Literal>()?;
    let datum = scalar_to_datum(literal.value())?;
    let reference = Reference::new(iceberg_field_name);

    match binary.op() {
        Operator::Gt => Some(reference.greater_than(datum)),
        Operator::Lt => Some(reference.less_than(datum)),
        Operator::GtEq => Some(reference.greater_than_or_equal_to(datum)),
        Operator::LtEq => Some(reference.less_than_or_equal_to(datum)),
        _ => None,
    }
}

fn scalar_to_datum(value: &ScalarValue) -> Option<Datum> {
    match value {
        ScalarValue::Int8(Some(value)) => Some(Datum::int(i32::from(*value))),
        ScalarValue::Int16(Some(value)) => Some(Datum::int(i32::from(*value))),
        ScalarValue::Int32(Some(value)) => Some(Datum::int(*value)),
        ScalarValue::Int64(Some(value)) => Some(Datum::long(*value)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_expr::expressions::lit;

    #[test]
    fn extracts_signed_integer_range() {
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let lower: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&key),
            Operator::GtEq,
            lit(100_i32),
        ));
        let upper: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(key, Operator::LtEq, lit(199_i32)));
        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(lower, Operator::And, upper));

        let expected = Reference::new("iceberg_key")
            .greater_than_or_equal_to(Datum::int(100))
            .and(Reference::new("iceberg_key").less_than_or_equal_to(Datum::int(199)));
        assert_eq!(
            extract_iceberg_predicate(&expr, 0, "iceberg_key"),
            Some(expected)
        );
    }

    #[test]
    fn retains_minmax_strictness_at_integer_extremes() {
        for (operator, literal, expected) in [
            (
                Operator::Gt,
                lit(i32::MAX),
                Reference::new("id").greater_than(Datum::int(i32::MAX)),
            ),
            (
                Operator::Lt,
                lit(i32::MIN),
                Reference::new("id").less_than(Datum::int(i32::MIN)),
            ),
            (
                Operator::Gt,
                lit(i64::MAX),
                Reference::new("id").greater_than(Datum::long(i64::MAX)),
            ),
            (
                Operator::Lt,
                lit(i64::MIN),
                Reference::new("id").less_than(Datum::long(i64::MIN)),
            ),
        ] {
            let expression: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
                Arc::new(Column::new("key", 0)),
                operator,
                literal,
            ));
            assert_eq!(
                extract_iceberg_predicate(&expression, 0, "id"),
                Some(expected)
            );
        }
    }

    #[test]
    fn keeps_supported_bounds_with_unconvertible_conjunct() {
        let bound: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("key", 0)),
            Operator::GtEq,
            lit(10_i64),
        ));
        let expr: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(bound, Operator::And, lit(true)));

        assert_eq!(
            extract_iceberg_predicate(&expr, 0, "iceberg_key"),
            Some(Reference::new("iceberg_key").greater_than_or_equal_to(Datum::long(10)))
        );
    }

    #[test]
    fn topk_nulls_first_keeps_the_null_arm_and_rejects_partial_or() {
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let nulls: Arc<dyn PhysicalExpr> = Arc::new(IsNullExpr::new(Arc::clone(&column)));
        let bound: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(column, Operator::Lt, lit(10_i32)));
        let expression: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(Arc::clone(&nulls), Operator::Or, bound));
        assert_eq!(
            extract_iceberg_predicate(&expression, 0, "id"),
            Some(
                Reference::new("id")
                    .is_null()
                    .or(Reference::new("id").less_than(Datum::int(10))),
            )
        );
        for other in [
            lit(true),
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("other", 1)),
                Operator::Lt,
                lit(10_i32),
            )) as Arc<dyn PhysicalExpr>,
        ] {
            let expression: Arc<dyn PhysicalExpr> =
                Arc::new(BinaryExpr::new(Arc::clone(&nulls), Operator::Or, other));
            assert!(extract_iceberg_predicate(&expression, 0, "id").is_none());
        }
    }

    #[test]
    fn handles_constant_filters() {
        let false_expr: Arc<dyn PhysicalExpr> = lit(false);
        let true_expr: Arc<dyn PhysicalExpr> = lit(true);
        assert_eq!(
            extract_iceberg_predicate(&false_expr, 0, "iceberg_key"),
            Some(Predicate::AlwaysFalse)
        );
        assert_eq!(
            extract_iceberg_predicate(&true_expr, 0, "iceberg_key"),
            None
        );
    }

    #[test]
    fn unsupported_shapes_fail_open() {
        use arrow::datatypes::DataType;
        use datafusion::physical_expr::expressions::CastExpr;

        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let bound: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&column),
            Operator::GtEq,
            lit(10_i32),
        ));
        let expressions: Vec<Arc<dyn PhysicalExpr>> = vec![
            Arc::new(BinaryExpr::new(bound, Operator::Or, lit(true))),
            Arc::new(BinaryExpr::new(
                Arc::clone(&column),
                Operator::Eq,
                lit(10_i32),
            )),
            Arc::new(BinaryExpr::new(
                Arc::clone(&column),
                Operator::GtEq,
                lit(10_u32),
            )),
            Arc::new(BinaryExpr::new(
                Arc::clone(&column),
                Operator::GtEq,
                lit(10_f64),
            )),
            Arc::new(BinaryExpr::new(
                Arc::clone(&column),
                Operator::GtEq,
                lit(ScalarValue::Int32(None)),
            )),
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("other", 1)),
                Operator::GtEq,
                lit(10_i32),
            )),
            Arc::new(BinaryExpr::new(
                Arc::new(CastExpr::new(column, DataType::Int64, None)),
                Operator::GtEq,
                lit(10_i64),
            )),
            Arc::new(BinaryExpr::new(
                lit(10_i32),
                Operator::LtEq,
                Arc::new(Column::new("key", 0)),
            )),
        ];
        for expression in expressions {
            assert_eq!(
                extract_iceberg_predicate(&expression, 0, "key"),
                None,
                "{expression}"
            );
        }
    }

    #[test]
    fn provider_samples_live_bounds() {
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&key)],
            lit(true),
        ));
        let provider = IcebergRuntimePredicateProvider::new(Arc::clone(&dynamic), 0, "id".into());
        let initial_generation = dynamic.snapshot_generation();
        assert_eq!(provider.generation(), initial_generation);
        assert!(provider.snapshot().unwrap().into_predicate().is_none());
        dynamic
            .update(Arc::new(BinaryExpr::new(key, Operator::LtEq, lit(42_i64))))
            .unwrap();
        assert_eq!(provider.generation(), initial_generation + 1);
        let snapshot = provider.snapshot().unwrap();
        let generation = snapshot.generation();
        assert_eq!(
            snapshot.into_predicate(),
            Some(Reference::new("id").less_than_or_equal_to(Datum::long(42)))
        );
        assert_eq!(generation, dynamic.snapshot_generation());
    }

    #[test]
    fn provider_pairs_bounds_with_their_publication_generation() {
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&key)],
            lit(true),
        ));
        let provider = IcebergRuntimePredicateProvider::new(Arc::clone(&dynamic), 0, "id".into());
        let initial_generation = dynamic.snapshot_generation();
        let producer = Arc::clone(&dynamic);
        let writer = std::thread::spawn(move || {
            for value in 1..=1000_i64 {
                producer
                    .update(Arc::new(BinaryExpr::new(
                        Arc::clone(&key),
                        Operator::LtEq,
                        lit(value),
                    )))
                    .unwrap();
            }
        });
        for _ in 0..1000 {
            // A continuously changing publication may fail open. Every
            // successful snapshot must carry its expression's generation.
            if let Ok(snapshot) = provider.snapshot() {
                let generation = snapshot.generation();
                if generation == initial_generation {
                    assert!(snapshot.into_predicate().is_none());
                } else {
                    assert_eq!(
                        snapshot.into_predicate(),
                        Some(Reference::new("id").less_than_or_equal_to(Datum::long(
                            (generation - initial_generation) as i64
                        )))
                    );
                }
            }
        }
        writer.join().unwrap();
        let snapshot = provider.snapshot().unwrap();
        assert_eq!(snapshot.generation(), initial_generation + 1000);
        assert_eq!(
            snapshot.into_predicate(),
            Some(Reference::new("id").less_than_or_equal_to(Datum::long(1000)))
        );
    }

    #[allow(deprecated)]
    #[tokio::test]
    async fn extracts_bounds_from_real_hash_membership_filter() {
        use arrow::array::{Int32Array, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::common::{JoinType, NullEquality};
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::physical_plan::collect;
        use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
        use datafusion::prelude::{SessionConfig, SessionContext};

        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let input = |values| {
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int32Array::from(values))],
            )
            .unwrap();
            MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None).unwrap()
        };
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&key)],
            lit(true),
        ));
        let join = HashJoinExec::try_new(
            input(vec![100, 103]),
            Arc::new(super::super::DynamicFilterExec::new(
                input(vec![100, 101, 102, 103]),
                Arc::clone(&dynamic),
                datafusion::physical_plan::metrics::ExecutionPlanMetricsSet::new(),
                "adapter_test",
            )),
            vec![(Arc::clone(&key), key)],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap()
        .with_dynamic_filter_expr(Arc::clone(&dynamic))
        .unwrap();
        let mut config = SessionConfig::new();
        config
            .options_mut()
            .optimizer
            .hash_join_inlist_pushdown_max_size = 0;
        config
            .options_mut()
            .optimizer
            .hash_join_inlist_pushdown_max_distinct_values = 0;
        let session = SessionContext::new_with_config(config);
        let result = collect(Arc::new(join), session.task_ctx()).await.unwrap();
        assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert!(dynamic
            .current()
            .unwrap()
            .to_string()
            .contains("hash_lookup"));
        let provider = IcebergRuntimePredicateProvider::new(dynamic, 0, "id".into());
        assert_eq!(
            provider.snapshot().unwrap().into_predicate(),
            Some(
                Reference::new("id")
                    .greater_than_or_equal_to(Datum::int(100))
                    .and(Reference::new("id").less_than_or_equal_to(Datum::int(103)))
            )
        );
    }
}
