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

use super::predicate::MAX_IN_LIST_LITERALS;
use super::*;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::lit;
use datafusion::physical_expr::expressions::{BinaryExpr, IsNullExpr};
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;

#[test]
fn extracts_signed_integer_range() {
    let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
    let lower: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::clone(&key),
        Operator::GtEq,
        lit(100_i32),
    ));
    let upper: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(key, Operator::LtEq, lit(199_i32)));
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
    let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(bound, Operator::And, lit(true)));

    assert_eq!(
        extract_iceberg_predicate(&expr, 0, "iceberg_key"),
        Some(Reference::new("iceberg_key").greater_than_or_equal_to(Datum::long(10)))
    );
}

#[test]
fn topk_nulls_first_keeps_the_null_arm_and_rejects_partial_or() {
    let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
    let nulls: Arc<dyn PhysicalExpr> = Arc::new(IsNullExpr::new(Arc::clone(&column)));
    let bound: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(column, Operator::Lt, lit(10_i32)));
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

/// A small build side publishes bounds AND an exact IN list; both reach the reader.
#[tokio::test]
async fn extracts_bounds_and_membership_from_real_in_list_filter() {
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::prelude::SessionContext;

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
    // DataFusion's default limits admit this two-key build side as an IN list.
    let session = SessionContext::new();
    let result = collect(Arc::new(join), session.task_ctx()).await.unwrap();
    assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    let published = dynamic.current().unwrap().to_string();
    assert!(!published.contains("hash_lookup"), "{published}");
    let provider = IcebergRuntimePredicateProvider::new(dynamic, 0, "id".into());
    let predicate = provider
        .snapshot()
        .unwrap()
        .into_predicate()
        .expect("bounds and membership translate")
        .to_string();
    for part in ["id >= 100", "id <= 103", "IN"] {
        assert!(predicate.contains(part), "{part} missing from {predicate}");
    }
}

/// DataFusion's TopK over two keys publishes `key < t OR (key = t AND ...)`; the reader
/// gets the first key bounded inclusively.
#[tokio::test]
async fn extracts_first_key_bound_from_real_multi_key_topk_filter() {
    use arrow::array::{Int32Array, RecordBatch};
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::prelude::SessionContext;

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("second", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![5, 1, 3, 1, 4, 2])),
            Arc::new(Int32Array::from(vec![0, 9, 0, 7, 0, 0])),
        ],
    )
    .unwrap();
    let input =
        MemorySourceConfig::try_new_exec(&[vec![batch]], Arc::clone(&schema), None).unwrap();
    let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
    let second: Arc<dyn PhysicalExpr> = Arc::new(Column::new("second", 1));
    let options = SortOptions {
        descending: false,
        nulls_first: false,
    };
    let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(
        vec![Arc::clone(&key), Arc::clone(&second)],
        lit(true),
    ));
    let sort = SortExec::new(
        LexOrdering::new(vec![
            PhysicalSortExpr::new(key, options),
            PhysicalSortExpr::new(second, options),
        ])
        .unwrap(),
        input,
    )
    .with_fetch(Some(2))
    .with_dynamic_filter_expr(Arc::clone(&dynamic))
    .unwrap();
    let session = SessionContext::new();
    let result = collect(Arc::new(sort), session.task_ctx()).await.unwrap();
    assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    // The heap holds (1, 7) and (1, 9): every later candidate needs key <= 1.
    let provider = IcebergRuntimePredicateProvider::new(dynamic, 0, "id".into());
    assert_eq!(
        provider.snapshot().unwrap().into_predicate(),
        Some(Reference::new("id").less_than_or_equal_to(Datum::int(1)))
    );
}

#[test]
fn decimal_bounds_and_membership_translate_exactly() {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::in_list;

    let schema = Schema::new(vec![Field::new("key", DataType::Decimal128(38, 2), true)]);
    let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
    let minimum = Datum::decimal_from_str("-1.25").unwrap();
    let maximum = Datum::decimal_from_str("999999999999999999999999999999999999.99").unwrap();
    let list = in_list(
        Arc::clone(&key),
        vec![
            lit(ScalarValue::Decimal128(Some(-125), 38, 2)),
            lit(ScalarValue::Decimal128(Some(0), 38, 2)),
            lit(ScalarValue::Decimal128(Some(10_i128.pow(38) - 1), 38, 2)),
        ],
        &false,
        &schema,
    )
    .unwrap();
    let lower: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::clone(&key),
        Operator::GtEq,
        lit(ScalarValue::Decimal128(Some(-125), 38, 2)),
    ));
    let upper: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        key,
        Operator::LtEq,
        lit(ScalarValue::Decimal128(Some(10_i128.pow(38) - 1), 38, 2)),
    ));
    let range: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(lower, Operator::And, upper));
    let both: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(range, Operator::And, list));
    assert_eq!(
        extract_iceberg_predicate(&both, 0, "id"),
        Some(
            Reference::new("id")
                .greater_than_or_equal_to(minimum.clone())
                .and(Reference::new("id").less_than_or_equal_to(maximum.clone()))
                .and(Reference::new("id").is_in([
                    minimum,
                    Datum::decimal_from_str("0.00").unwrap(),
                    maximum,
                ]))
        )
    );
}

#[test]
fn string_bounds_and_membership_translate_without_truncation() {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::in_list;

    for data_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let schema = Schema::new(vec![Field::new("key", data_type.clone(), true)]);
        let scalar = |value: &str| match data_type {
            DataType::Utf8 => ScalarValue::Utf8(Some(value.into())),
            DataType::LargeUtf8 => ScalarValue::LargeUtf8(Some(value.into())),
            _ => ScalarValue::Utf8View(Some(value.into())),
        };
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let values = ["", "abcdefghijklmnop-long-prefix", "é東京🙂"];
        let list = in_list(
            Arc::clone(&key),
            values.iter().map(|value| lit(scalar(value))).collect(),
            &false,
            &schema,
        )
        .unwrap();
        let lower: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::clone(&key),
            Operator::GtEq,
            lit(scalar("")),
        ));
        let upper: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(key, Operator::LtEq, lit(scalar("é東京🙂"))));
        let range: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(lower, Operator::And, upper));
        let both: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(range, Operator::And, list));
        assert_eq!(
            extract_iceberg_predicate(&both, 0, "id"),
            Some(
                Reference::new("id")
                    .greater_than_or_equal_to(Datum::string(""))
                    .and(Reference::new("id").less_than_or_equal_to(Datum::string("é東京🙂")))
                    .and(Reference::new("id").is_in(values.map(Datum::string)))
            )
        );
    }
}
