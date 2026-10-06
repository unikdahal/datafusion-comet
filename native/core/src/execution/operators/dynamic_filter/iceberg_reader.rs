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
    BinaryExpr, Column, DynamicFilterPhysicalExpr, InListExpr, IsNotNullExpr, IsNullExpr, Literal,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_comet_operators::CometFilterExec;
use iceberg::arrow::{RuntimePredicateProvider, RuntimePredicateSnapshot};
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;
use iceberg::{Error, ErrorKind, Result as IcebergResult};

use crate::execution::operators::{IcebergScanExec, RuntimeScanOrder};

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

/// Whether `input` is a native Iceberg scan, possibly below projection-free
/// direct-column null checks, that [`try_attach_iceberg_reader_filter`] can reach.
pub(super) fn reaches_iceberg_reader(input: &Arc<dyn ExecutionPlan>) -> bool {
    if input.fetch().is_some() {
        return false;
    }
    if let Some(filter) = input.downcast_ref::<CometFilterExec>() {
        return !filter.has_projection()
            && is_deterministic(filter.predicate())
            && reaches_iceberg_reader(filter.input());
    }
    input.is::<IcebergScanExec>()
}

/// Whether `expr` is built only from expressions known to be deterministic.
///
/// The reader may attach through a filter between the scan and the producer: a row whose key
/// the runtime predicate rejects can never reach the producer's result, whatever the filter
/// does with it. That holds only if skipping rows cannot change what the filter returns for the
/// others, so volatile functions (`rand`, `uuid`, `monotonically_increasing_id`, ...), JVM
/// UDFs and any expression not listed here keep the filter in the way.
fn is_deterministic(expr: &Arc<dyn PhysicalExpr>) -> bool {
    use datafusion::logical_expr::Volatility;
    use datafusion::physical_expr::expressions::{
        CaseExpr, CastExpr, LikeExpr, NegativeExpr, NotExpr, TryCastExpr,
    };
    use datafusion::physical_expr::ScalarFunctionExpr;

    let deterministic_node = expr.is::<Column>()
        || expr.is::<Literal>()
        || expr.is::<BinaryExpr>()
        || expr.is::<IsNullExpr>()
        || expr.is::<IsNotNullExpr>()
        || expr.is::<NotExpr>()
        || expr.is::<NegativeExpr>()
        || expr.is::<InListExpr>()
        || expr.is::<CastExpr>()
        || expr.is::<TryCastExpr>()
        || expr.is::<CaseExpr>()
        || expr.is::<LikeExpr>()
        || expr
            .downcast_ref::<ScalarFunctionExpr>()
            .is_some_and(|function| function.fun().signature().volatility != Volatility::Volatile);
    deterministic_node && expr.children().iter().all(|child| is_deterministic(child))
}

/// Attaches `predicate` to the Iceberg scan below `input`. With `order`, the scan also reads
/// its file tasks best-first for that direction (see `order_tasks_for_runtime_bound`).
pub(super) fn try_attach_iceberg_reader_filter(
    input: &Arc<dyn ExecutionPlan>,
    predicate: Arc<DynamicFilterPhysicalExpr>,
    order: Option<RuntimeScanOrder>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if input.fetch().is_some() {
        return Ok(None);
    }

    if let Some(filter) = input.downcast_ref::<CometFilterExec>() {
        if filter.has_projection() || !is_deterministic(filter.predicate()) {
            return Ok(None);
        }
        let Some(reader) =
            try_attach_iceberg_reader_filter(filter.input(), Arc::clone(&predicate), order)?
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
    // A multi-key TopK filter lists every sort key; only the first one bounds the scan.
    let children = predicate.children();
    let Some(column) = children
        .first()
        .and_then(|child| child.downcast_ref::<Column>())
    else {
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
    let Some((iceberg_field_id, iceberg_field_name)) = scan.runtime_predicate_field(column.index())
    else {
        return Ok(None);
    };
    let provider: Arc<dyn RuntimePredicateProvider> = Arc::new(
        IcebergRuntimePredicateProvider::new(
            Arc::clone(&predicate),
            column.index(),
            iceberg_field_name,
        )
        .with_largest_first(matches!(order, Some(RuntimeScanOrder::Descending { .. }))),
    );
    Ok(Some(Arc::new(scan.with_runtime_predicate_provider(
        provider,
        order.map(|order| (iceberg_field_id, order)),
    ))))
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
            return extract_disjunction(binary, probe_column_index, iceberg_field_name);
        }
        return extract_bound(binary, probe_column_index, iceberg_field_name);
    }

    if let Some(list) = expr.downcast_ref::<InListExpr>() {
        return extract_in_list(list, probe_column_index, iceberg_field_name);
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

/// The largest `IN` list translated into an Iceberg predicate. DataFusion only publishes a list
/// for small build sides; larger ones stay hash lookups on the probe side.
const MAX_IN_LIST_LITERALS: usize = 1024;

/// `key IN (v1, v2, ...)` over the probe column, or `None` if any part is not a plain literal.
fn extract_in_list(
    list: &InListExpr,
    probe_column_index: usize,
    iceberg_field_name: &str,
) -> Option<Predicate> {
    if list.negated() || list.is_empty() || list.len() > MAX_IN_LIST_LITERALS {
        return None;
    }
    if list.expr().downcast_ref::<Column>()?.index() != probe_column_index {
        return None;
    }
    let datums = list
        .list()
        .iter()
        .map(|item| {
            item.downcast_ref::<Literal>()
                .and_then(|literal| scalar_to_datum(literal.value()))
        })
        .collect::<Option<Vec<_>>>()?;
    Some(Reference::new(iceberg_field_name).is_in(datums))
}

/// A leaf of `a OR b OR ...` with nested ORs flattened left to right.
fn flatten_or<'a>(expr: &'a Arc<dyn PhysicalExpr>, arms: &mut Vec<&'a Arc<dyn PhysicalExpr>>) {
    match expr.downcast_ref::<BinaryExpr>() {
        Some(binary) if binary.op() == &Operator::Or => {
            flatten_or(binary.left(), arms);
            flatten_or(binary.right(), arms);
        }
        _ => arms.push(expr),
    }
}

/// Whether the leftmost conjunct of `arm` is `column = literal` on the probe column.
fn starts_with_equality(
    arm: &Arc<dyn PhysicalExpr>,
    probe_column_index: usize,
    literal: &ScalarValue,
) -> bool {
    let Some(binary) = arm.downcast_ref::<BinaryExpr>() else {
        return false;
    };
    match binary.op() {
        Operator::And => starts_with_equality(binary.left(), probe_column_index, literal),
        Operator::Eq => {
            binary
                .left()
                .downcast_ref::<Column>()
                .is_some_and(|column| column.index() == probe_column_index)
                && binary
                    .right()
                    .downcast_ref::<Literal>()
                    .is_some_and(|value| value.value() == literal)
        }
        _ => false,
    }
}

/// The two OR shapes DataFusion's TopK produces, translated on the first sort key only:
///
/// * single key: `key < t` or `key IS NULL OR key < t` (also `>`), kept exact;
/// * several keys: those arms followed by one `key = t AND ...` arm per further key. Every
///   further arm implies `key = t`, so the first key alone is bounded by `key <= t`
///   (`>=` for descending), plus `OR key IS NULL` when nulls sort first.
///
/// Any other OR fails open.
fn extract_disjunction(
    binary: &BinaryExpr,
    probe_column_index: usize,
    iceberg_field_name: &str,
) -> Option<Predicate> {
    let mut arms = Vec::new();
    flatten_or(binary.left(), &mut arms);
    flatten_or(binary.right(), &mut arms);
    let mut arms = arms.into_iter();

    let mut first = arms.next()?;
    let mut with_null = false;
    if let Some(null) = first.downcast_ref::<IsNullExpr>() {
        if null.arg().downcast_ref::<Column>()?.index() != probe_column_index {
            return None;
        }
        with_null = true;
        first = arms.next()?;
    }
    let comparison = first.downcast_ref::<BinaryExpr>()?;
    if !matches!(comparison.op(), Operator::Lt | Operator::Gt) {
        return None;
    }
    let threshold = comparison.right().downcast_ref::<Literal>()?;
    let rest: Vec<_> = arms.collect();
    let bound = if rest.is_empty() {
        extract_bound(comparison, probe_column_index, iceberg_field_name)?
    } else {
        if !rest
            .iter()
            .all(|arm| starts_with_equality(arm, probe_column_index, threshold.value()))
        {
            return None;
        }
        let column = comparison.left().downcast_ref::<Column>()?;
        if column.index() != probe_column_index {
            return None;
        }
        let datum = scalar_to_datum(threshold.value())?;
        let reference = Reference::new(iceberg_field_name);
        match comparison.op() {
            Operator::Lt => reference.less_than_or_equal_to(datum),
            _ => reference.greater_than_or_equal_to(datum),
        }
    };
    Some(if with_null {
        Reference::new(iceberg_field_name).is_null().or(bound)
    } else {
        bound
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
        ScalarValue::Date32(Some(days)) => Some(Datum::date(*days)),
        // Spark TIMESTAMP carries a zone and maps to Iceberg timestamptz; TIMESTAMP_NTZ has
        // none and maps to timestamp. Both store microseconds since the epoch.
        ScalarValue::TimestampMicrosecond(Some(micros), Some(_)) => {
            Some(Datum::timestamptz_micros(*micros))
        }
        ScalarValue::TimestampMicrosecond(Some(micros), None) => {
            Some(Datum::timestamp_micros(*micros))
        }
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

    #[test]
    fn multi_key_topk_filter_bounds_only_the_first_key_inclusively() {
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let second: Arc<dyn PhysicalExpr> = Arc::new(Column::new("second", 1));
        let third: Arc<dyn PhysicalExpr> = Arc::new(Column::new("third", 2));
        let binary = |left: &Arc<dyn PhysicalExpr>, op, right: i32| -> Arc<dyn PhysicalExpr> {
            Arc::new(BinaryExpr::new(Arc::clone(left), op, lit(right)))
        };
        let and = |left, right| -> Arc<dyn PhysicalExpr> {
            Arc::new(BinaryExpr::new(left, Operator::And, right))
        };
        let or = |left, right| -> Arc<dyn PhysicalExpr> {
            Arc::new(BinaryExpr::new(left, Operator::Or, right))
        };
        // ORDER BY key, second, third LIMIT n, as DataFusion builds it after its heap fills.
        let tie = binary(&key, Operator::Eq, 10);
        let tie_second = and(Arc::clone(&tie), binary(&second, Operator::Eq, 5));
        let filter = or(
            or(
                binary(&key, Operator::Lt, 10),
                and(Arc::clone(&tie), binary(&second, Operator::Lt, 5)),
            ),
            and(tie_second, binary(&third, Operator::Lt, 3)),
        );
        assert_eq!(
            extract_iceberg_predicate(&filter, 0, "id"),
            Some(Reference::new("id").less_than_or_equal_to(Datum::int(10)))
        );

        // Descending, nulls first: the null arm stays and the bound flips.
        let nulls: Arc<dyn PhysicalExpr> = Arc::new(IsNullExpr::new(Arc::clone(&key)));
        let filter = or(
            or(nulls, binary(&key, Operator::Gt, 10)),
            and(Arc::clone(&tie), binary(&second, Operator::Gt, 5)),
        );
        assert_eq!(
            extract_iceberg_predicate(&filter, 0, "id"),
            Some(
                Reference::new("id")
                    .is_null()
                    .or(Reference::new("id").greater_than_or_equal_to(Datum::int(10)))
            )
        );

        // A tie arm on another threshold, another column, or no tie at all fails open.
        let other_threshold = and(
            binary(&key, Operator::Eq, 11),
            binary(&second, Operator::Lt, 5),
        );
        let other_column = and(
            binary(&second, Operator::Eq, 10),
            binary(&second, Operator::Lt, 5),
        );
        let no_tie = binary(&second, Operator::Lt, 5);
        for arm in [other_threshold, other_column, no_tie] {
            let filter = or(binary(&key, Operator::Lt, 10), arm);
            assert!(extract_iceberg_predicate(&filter, 0, "id").is_none());
        }
    }

    #[test]
    fn small_exact_membership_becomes_an_in_predicate() {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_expr::expressions::in_list;

        let schema = Schema::new(vec![Field::new("key", DataType::Int32, false)]);
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let list = in_list(
            Arc::clone(&key),
            vec![lit(7_i32), lit(3_i32), lit(7_i32)],
            &false,
            &schema,
        )
        .unwrap();
        assert_eq!(
            extract_iceberg_predicate(&list, 0, "id"),
            Some(Reference::new("id").is_in([Datum::int(3), Datum::int(7)]))
        );

        // Bounds and membership combine.
        let range: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&key),
            Operator::GtEq,
            lit(3_i32),
        ));
        let both: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(range, Operator::And, list));
        assert_eq!(
            extract_iceberg_predicate(&both, 0, "id"),
            Some(
                Reference::new("id")
                    .greater_than_or_equal_to(Datum::int(3))
                    .and(Reference::new("id").is_in([Datum::int(3), Datum::int(7)]))
            )
        );

        // Negated lists, other columns, nulls and unsupported literals fail open.
        let negated = in_list(Arc::clone(&key), vec![lit(1_i32)], &true, &schema).unwrap();
        assert!(extract_iceberg_predicate(&negated, 0, "id").is_none());
        let other_column = in_list(
            Arc::new(Column::new("other", 1)),
            vec![lit(1_i32)],
            &false,
            &Schema::new(vec![
                Field::new("key", DataType::Int32, false),
                Field::new("other", DataType::Int32, false),
            ]),
        )
        .unwrap();
        assert!(extract_iceberg_predicate(&other_column, 0, "id").is_none());
        let with_null = in_list(
            Arc::clone(&key),
            vec![lit(1_i32), lit(ScalarValue::Int32(None))],
            &false,
            &schema,
        )
        .unwrap();
        assert!(extract_iceberg_predicate(&with_null, 0, "id").is_none());
        // More literals than the cap is not translated.
        let many = in_list(
            key,
            (0..=MAX_IN_LIST_LITERALS as i32).map(lit).collect(),
            &false,
            &schema,
        )
        .unwrap();
        assert!(extract_iceberg_predicate(&many, 0, "id").is_none());
    }

    #[test]
    fn reader_attachment_passes_only_deterministic_filters() {
        use datafusion::physical_expr::expressions::NotExpr;
        use datafusion_comet_spark_expr::RandExpr;

        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let value: Arc<dyn PhysicalExpr> = Arc::new(Column::new("value", 1));
        let modulo: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::clone(&value),
                Operator::Modulo,
                lit(3_i32),
            )),
            Operator::Eq,
            lit(0_i32),
        ));
        let deterministic: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(NotExpr::new(Arc::new(IsNullExpr::new(Arc::clone(&key))))),
            Operator::And,
            Arc::clone(&modulo),
        ));
        assert!(is_deterministic(&deterministic));

        // A volatile expression anywhere below the filter keeps it in the way.
        let random: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(RandExpr::new(42)),
            Operator::Lt,
            lit(0.5_f64),
        ));
        let mixed: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(modulo, Operator::And, random));
        assert!(!is_deterministic(&mixed));
    }

    #[test]
    fn timestamp_bounds_keep_their_zone_semantics() {
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("ts", 0));
        let bound = |value: ScalarValue| -> Arc<dyn PhysicalExpr> {
            Arc::new(BinaryExpr::new(
                Arc::clone(&column),
                Operator::Lt,
                lit(value),
            ))
        };
        // Spark TIMESTAMP (with a zone) is Iceberg timestamptz; TIMESTAMP_NTZ is timestamp.
        assert_eq!(
            extract_iceberg_predicate(
                &bound(ScalarValue::TimestampMicrosecond(
                    Some(7),
                    Some("UTC".into())
                )),
                0,
                "ts"
            ),
            Some(Reference::new("ts").less_than(Datum::timestamptz_micros(7)))
        );
        assert_eq!(
            extract_iceberg_predicate(
                &bound(ScalarValue::TimestampMicrosecond(Some(7), None)),
                0,
                "ts"
            ),
            Some(Reference::new("ts").less_than(Datum::timestamp_micros(7)))
        );
        // Other units are not translated.
        assert!(extract_iceberg_predicate(
            &bound(ScalarValue::TimestampNanosecond(Some(7), None)),
            0,
            "ts"
        )
        .is_none());
    }

    #[test]
    fn date_bounds_become_date_predicates() {
        let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("day", 0));
        let expression: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            column,
            Operator::GtEq,
            lit(ScalarValue::Date32(Some(19_000))),
        ));
        assert_eq!(
            extract_iceberg_predicate(&expression, 0, "day"),
            Some(Reference::new("day").greater_than_or_equal_to(Datum::date(19_000)))
        );
    }
}
