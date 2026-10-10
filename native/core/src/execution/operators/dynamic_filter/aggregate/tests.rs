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

use super::*;

use crate::execution::operators::IcebergScanExec;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::expressions::{BinaryExpr, CastExpr};
use datafusion::physical_plan::aggregates::PhysicalGroupBy;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
use iceberg::scan::FileScanTask;
use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema as IcebergSchema, Type};

fn scan(data_type: DataType) -> Arc<dyn ExecutionPlan> {
    scan_field(Field::new("key", data_type, true))
}

fn scan_field(field: Field) -> Arc<dyn ExecutionPlan> {
    let iceberg_type = match field.data_type().clone() {
        DataType::Int32 => PrimitiveType::Int,
        DataType::Int64 => PrimitiveType::Long,
        data_type if super::super::join::is_string_key_type(&data_type) => PrimitiveType::String,
        _ => unreachable!(),
    };
    let schema = Arc::new(
        IcebergSchema::builder()
            .with_fields(vec![NestedField::optional(
                1,
                "key",
                Type::Primitive(iceberg_type),
            )
            .into()])
            .build()
            .unwrap(),
    );
    let task = FileScanTask::builder()
        .with_data_file_path("/tmp/minmax.parquet".into())
        .with_data_file_format(DataFileFormat::Parquet)
        .with_file_size_in_bytes(1024)
        .with_start(0)
        .with_length(1024)
        .with_record_count(Some(10))
        .with_schema(schema)
        .with_project_field_ids(vec![1])
        .with_case_sensitive(false)
        .build()
        .unwrap();
    Arc::new(
        IcebergScanExec::new(
            "/tmp/metadata.json".into(),
            Arc::new(Schema::new(vec![field])),
            Default::default(),
            String::new(),
            vec![task],
            1,
        )
        .unwrap(),
    )
}

fn aggregate(input: Arc<dyn ExecutionPlan>, max: bool, cast: bool, grouped: bool) -> AggregateExec {
    let key: Arc<dyn datafusion::physical_expr::PhysicalExpr> = Arc::new(Column::new("key", 0));
    let argument = if cast {
        Arc::new(CastExpr::new(Arc::clone(&key), DataType::Int64, None)) as _
    } else {
        Arc::clone(&key)
    };
    let expr = Arc::new(
        AggregateExprBuilder::new(if max { max_udaf() } else { min_udaf() }, vec![argument])
            .schema(input.schema())
            .alias("result")
            .build()
            .unwrap(),
    );
    let group = PhysicalGroupBy::new_single(if grouped {
        vec![(key, "key".into())]
    } else {
        vec![]
    });
    let schema = input.schema();
    AggregateExec::try_new(
        AggregateMode::Partial,
        group,
        vec![expr],
        vec![None],
        input,
        schema,
    )
    .unwrap()
}

#[test]
fn integer_minmax_attaches_and_recreates_producer_state() {
    for data_type in [DataType::Int32, DataType::Int64] {
        for max in [false, true] {
            let aggregate = aggregate(scan(data_type.clone()), max, false, false);
            let wrapper = IcebergMinMaxFilterExec::try_new(&aggregate, &ConfigOptions::default())
                .unwrap()
                .unwrap();
            let first = wrapper.build_runtime_aggregate().unwrap();
            let expressions = first.dynamic_expressions_produced();
            let producer = expressions[0]
                .downcast_ref::<DynamicFilterPhysicalExpr>()
                .unwrap();
            producer
                .update(Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("key", 0)),
                    Operator::Gt,
                    if data_type == DataType::Int32 {
                        lit(i32::MAX)
                    } else {
                        lit(i64::MAX)
                    },
                )))
                .unwrap();
            let second = wrapper.build_runtime_aggregate().unwrap();
            let fresh = second.dynamic_expressions_produced();
            assert!(!Arc::ptr_eq(&expressions[0], &fresh[0]));
            let fresh = fresh[0]
                .downcast_ref::<DynamicFilterPhysicalExpr>()
                .unwrap();
            assert_eq!(
                fresh
                    .current()
                    .unwrap()
                    .downcast_ref::<Literal>()
                    .unwrap()
                    .value(),
                &ScalarValue::Boolean(Some(true))
            );
            assert!(producer.snapshot_generation() > fresh.snapshot_generation());
            assert_eq!(first.input().name(), "IcebergScanExec");
            let reset = Arc::new(wrapper).reset_state().unwrap();
            let reset = reset.downcast_ref::<IcebergMinMaxFilterExec>().unwrap();
            let runtime = reset.build_runtime_aggregate().unwrap();
            assert_eq!(
                runtime.dynamic_expressions_produced()[0]
                    .downcast_ref::<DynamicFilterPhysicalExpr>()
                    .unwrap()
                    .current()
                    .unwrap()
                    .downcast_ref::<Literal>()
                    .unwrap()
                    .value(),
                &ScalarValue::Boolean(Some(true))
            );
        }
    }
}

#[test]
fn unsupported_shapes_and_disabled_config_retain_aggregate() {
    for aggregate in [
        aggregate(scan(DataType::Int32), true, true, false),
        aggregate(scan(DataType::Int32), true, false, true),
        aggregate(
            Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![Field::new(
                "key",
                DataType::Int32,
                true,
            )])))),
            true,
            false,
            false,
        ),
    ] {
        assert!(
            IcebergMinMaxFilterExec::try_new(&aggregate, &ConfigOptions::default())
                .unwrap()
                .is_none()
        );
    }
    let aggregate = aggregate(scan(DataType::Int64), false, false, false);
    let mut config = ConfigOptions::default();
    config.optimizer.enable_aggregate_dynamic_filter_pushdown = false;
    assert!(IcebergMinMaxFilterExec::try_new(&aggregate, &config)
        .unwrap()
        .is_none());
    config.optimizer.enable_aggregate_dynamic_filter_pushdown = true;
    config.optimizer.enable_dynamic_filter_pushdown = false;
    assert!(IcebergMinMaxFilterExec::try_new(&aggregate, &config)
        .unwrap()
        .is_none());
}

/// `SELECT min(key) FROM t WHERE other > 5`: a filter whose folded projection drops `other`,
/// under a column projection. The key is followed to its scan column through both.
#[test]
fn minmax_attaches_through_filter_and_projection() {
    use datafusion::physical_plan::filter::FilterExecBuilder;
    use datafusion::physical_plan::projection::ProjectionExec;

    let schema = Arc::new(
        IcebergSchema::builder()
            .with_fields(vec![
                NestedField::optional(1, "other", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "key", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap(),
    );
    let task = FileScanTask::builder()
        .with_data_file_path("/tmp/minmax.parquet".into())
        .with_data_file_format(DataFileFormat::Parquet)
        .with_file_size_in_bytes(1024)
        .with_start(0)
        .with_length(1024)
        .with_record_count(Some(10))
        .with_schema(schema)
        .with_project_field_ids(vec![1, 2])
        .with_case_sensitive(false)
        .build()
        .unwrap();
    let scan: Arc<dyn ExecutionPlan> = Arc::new(
        IcebergScanExec::new(
            "/tmp/metadata.json".into(),
            Arc::new(Schema::new(vec![
                Field::new("other", DataType::Int32, true),
                Field::new("key", DataType::Int32, true),
            ])),
            Default::default(),
            String::new(),
            vec![task],
            1,
        )
        .unwrap(),
    );
    let filter: Arc<dyn ExecutionPlan> = Arc::new(
        FilterExecBuilder::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("other", 0)),
                Operator::Gt,
                lit(5_i32),
            )),
            scan,
        )
        .apply_projection(Some(vec![1]))
        .unwrap()
        .build()
        .unwrap(),
    );
    let projection: Arc<dyn ExecutionPlan> = Arc::new(
        ProjectionExec::try_new(
            vec![(
                Arc::new(Column::new("key", 0)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                "key".to_string(),
            )],
            filter,
        )
        .unwrap(),
    );
    let aggregate = aggregate(projection, false, false, false);
    assert!(
        IcebergMinMaxFilterExec::try_new(&aggregate, &ConfigOptions::default())
            .unwrap()
            .is_some(),
        "MIN must reach the Iceberg reader through the filter and the projection"
    );
}

#[test]
fn binary_string_minmax_attaches_for_all_arrow_encodings() {
    for value_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        for data_type in [
            value_type.clone(),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(value_type)),
        ] {
            for max in [false, true] {
                let aggregate = aggregate(scan(data_type.clone()), max, false, false);
                let wrapper =
                    IcebergMinMaxFilterExec::try_new(&aggregate, &ConfigOptions::default())
                        .unwrap()
                        .expect("binary string MIN/MAX");
                let runtime = wrapper.build_runtime_aggregate().unwrap();
                assert!(runtime.input().is::<IcebergScanExec>());
                assert_eq!(runtime.dynamic_expressions_produced().len(), 1);
            }
        }
    }
}

#[test]
fn annotated_string_columns_fail_closed() {
    for (name, value) in [
        ("__COLLATIONS", "UTF8_LCASE"),
        ("__CHAR_VARCHAR_TYPE_STRING", "char(24)"),
        ("ARROW:extension:name", "unknown"),
    ] {
        let field = Field::new("key", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([(name.into(), value.into())]),
        );
        for max in [false, true] {
            let aggregate = aggregate(scan_field(field.clone()), max, false, false);
            assert!(
                IcebergMinMaxFilterExec::try_new(&aggregate, &ConfigOptions::default())
                    .unwrap()
                    .is_none()
            );
        }
    }
}

fn produced_filter(aggregate: &AggregateExec) -> Arc<DynamicFilterPhysicalExpr> {
    let mut expressions = aggregate.dynamic_expressions_produced();
    assert_eq!(expressions.len(), 1);
    let expression: Arc<dyn std::any::Any + Send + Sync> = expressions.pop().unwrap();
    expression.downcast::<DynamicFilterPhysicalExpr>().unwrap()
}

fn result_value(batches: &[arrow::record_batch::RecordBatch]) -> ScalarValue {
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    ScalarValue::try_from_array(batches[0].column(0), 0).unwrap()
}

#[tokio::test]
async fn native_minmax_execution_matches_disabled_pruning_and_reexecutes() {
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{SessionConfig, SessionContext};

    for max in [false, true] {
        for values in [
            vec![Some(5), None, Some(9), Some(-3)],
            vec![None; 4],
            vec![],
        ] {
            let session = SessionContext::new_with_config(SessionConfig::new().with_batch_size(2));
            let (_file, scan) = super::super::test_utils::iceberg_input(values.clone());
            let plain = aggregate(Arc::clone(&scan), max, false, false);
            let plan = Arc::new(
                IcebergMinMaxFilterExec::try_new(&plain, &ConfigOptions::default())
                    .unwrap()
                    .unwrap(),
            );
            let mut disabled = SessionConfig::new().with_batch_size(2);
            disabled
                .options_mut()
                .optimizer
                .enable_aggregate_dynamic_filter_pushdown = false;
            let baseline_session = SessionContext::new_with_config(disabled);
            let baseline = collect(Arc::new(plain), baseline_session.task_ctx())
                .await
                .unwrap();
            let expected = if max {
                values.into_iter().flatten().max()
            } else {
                values.into_iter().flatten().min()
            };
            assert_eq!(result_value(&baseline), ScalarValue::Int32(expected));
            for execution in [
                Arc::clone(&plan) as Arc<dyn ExecutionPlan>,
                Arc::clone(&plan).reset_state().unwrap(),
            ] {
                let actual = collect(execution, session.task_ctx()).await.unwrap();
                assert_eq!(result_value(&actual), result_value(&baseline));
            }
            let (_replacement_file, replacement) =
                super::super::test_utils::iceberg_input(vec![Some(101), Some(109)]);
            let replaced = plan
                .replace_children(
                    vec![replacement],
                    ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                )
                .unwrap();
            assert!(replaced.is::<IcebergMinMaxFilterExec>());
            let actual = collect(replaced, session.task_ctx()).await.unwrap();
            assert_eq!(
                result_value(&actual),
                ScalarValue::Int32(Some(if max { 109 } else { 101 })),
            );
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Ending {
    Eof,
    Error,
    Pending,
}

/// Execute the attached real reader, then control only the final stream event.
#[derive(Debug)]
struct ControlledScan {
    input: Arc<dyn ExecutionPlan>,
    schema: arrow::datatypes::SchemaRef,
    ending: Ending,
    tail_reached: Arc<std::sync::atomic::AtomicBool>,
}

impl datafusion::physical_plan::streaming::PartitionStream for ControlledScan {
    fn schema(&self) -> &arrow::datatypes::SchemaRef {
        &self.schema
    }

    fn execute(
        &self,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> SendableRecordBatchStream {
        let input = self.input.execute(0, context).unwrap();
        let tail: futures::stream::BoxStream<'static, Result<arrow::record_batch::RecordBatch>> =
            match self.ending {
                Ending::Eof => futures::stream::empty().boxed(),
                Ending::Error => futures::stream::once(async {
                    datafusion::common::exec_err!("controlled failure after real Iceberg input")
                })
                .boxed(),
                Ending::Pending => futures::stream::pending().boxed(),
            };
        let tail_reached = Arc::clone(&self.tail_reached);
        let tail = futures::stream::once(async move {
            tail_reached.store(true, std::sync::atomic::Ordering::SeqCst);
            tail
        })
        .flatten();
        Box::pin(RecordBatchStreamAdapter::new(
            self.input.schema(),
            input.chain(tail),
        ))
    }
}

#[tokio::test]
async fn native_minmax_execution_and_stream_lifetime() {
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::streaming::StreamingTableExec;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures::FutureExt;

    for max in [false, true] {
        for ending in [Ending::Eof, Ending::Error, Ending::Pending] {
            let pool = Arc::new(GreedyMemoryPool::new(8 * 1024 * 1024));
            let runtime_env = RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool) as _)
                .build_arc()
                .unwrap();
            let session = SessionContext::new_with_config_rt(
                SessionConfig::new().with_batch_size(2),
                runtime_env,
            );
            let (_file, scan) =
                super::super::test_utils::iceberg_input(vec![Some(5), None, Some(9), Some(-3)]);
            let plain = aggregate(scan, max, false, false);
            let plan = Arc::new(
                IcebergMinMaxFilterExec::try_new(&plain, &ConfigOptions::default())
                    .unwrap()
                    .unwrap(),
            );
            let runtime = plan.build_runtime_aggregate().unwrap();
            let producer = produced_filter(&runtime);
            let initial_generation = producer.snapshot_generation();
            let predicate = Arc::downgrade(&producer);
            let tail_reached = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let controlled: Arc<dyn ExecutionPlan> = Arc::new(
                StreamingTableExec::try_new(
                    runtime.input().schema(),
                    vec![Arc::new(ControlledScan {
                        input: Arc::clone(runtime.input()),
                        schema: runtime.input().schema(),
                        ending,
                        tail_reached: Arc::clone(&tail_reached),
                    })],
                    None,
                    [],
                    false,
                    None,
                )
                .unwrap(),
            );
            let controlled_runtime = IcebergMinMaxFilterExec::fresh_aggregate(&runtime, controlled)
                .unwrap()
                .with_dynamic_filter_expr(producer)
                .unwrap();
            drop(runtime);
            let mut stream = plan
                .execute_runtime_aggregate(controlled_runtime, 0, session.task_ctx())
                .unwrap();
            match ending {
                Ending::Eof => {
                    let batch = stream.next().await.unwrap().unwrap();
                    assert_eq!(
                        result_value(&[batch]),
                        ScalarValue::Int32(Some(if max { 9 } else { -3 })),
                    );
                    assert!(
                        predicate.upgrade().unwrap().snapshot_generation() > initial_generation
                    );
                    assert!(stream.next().await.is_none());
                }
                Ending::Error => {
                    let error = stream.next().await.unwrap().unwrap_err();
                    assert!(error
                        .to_string()
                        .contains("controlled failure after real Iceberg input"));
                    assert!(stream.next().await.is_none());
                }
                Ending::Pending => {
                    // Poll until the real reader reaches the controlled tail, without timing races.
                    tokio::time::timeout(std::time::Duration::from_secs(10), async {
                        loop {
                            if let Some(batch) = stream.next().now_or_never() {
                                panic!("pending aggregate unexpectedly returned {batch:?}");
                            }
                            if tail_reached.load(std::sync::atomic::Ordering::SeqCst) {
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                    assert!(
                        predicate.upgrade().unwrap().snapshot_generation() > initial_generation
                    );
                    drop(stream);
                }
            }
            // The permanent template stays alive, as does the terminated EOF/error stream.
            assert_eq!(pool.reserved(), 0, "{max}: {ending:?}");
            assert!(predicate.upgrade().is_none(), "{max}: {ending:?}");
            let fresh = plan.build_runtime_aggregate().unwrap();
            let fresh_filter = produced_filter(&fresh);
            assert_eq!(fresh_filter.snapshot_generation(), initial_generation);
            assert_eq!(
                fresh_filter
                    .current()
                    .unwrap()
                    .downcast_ref::<Literal>()
                    .unwrap()
                    .value(),
                &ScalarValue::Boolean(Some(true)),
            );
            drop(fresh_filter);
            drop(fresh);
            let (_replacement_file, replacement) =
                super::super::test_utils::iceberg_input(vec![Some(101), Some(109)]);
            let replaced = plan
                .replace_children(
                    vec![replacement],
                    ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
                )
                .unwrap();
            let actual = collect(replaced, session.task_ctx()).await.unwrap();
            assert_eq!(
                result_value(&actual),
                ScalarValue::Int32(Some(if max { 109 } else { 101 })),
            );
            assert_eq!(pool.reserved(), 0);
        }
    }
}

/// Executing one wrapper instance repeatedly sums its counters across executions. Each
/// execution registers a fixed number of metrics, at most the plain aggregate's own
/// per-execution registrations plus the attachment counter: DataFusion's `MetricBuilder`
/// registers a new metric per call, so a plain operator grows the same way.
#[tokio::test]
async fn repeated_execution_of_one_wrapper_aggregates_metrics() {
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    let session = SessionContext::new();
    let (_file, scan) =
        super::super::test_utils::iceberg_input(vec![Some(5), None, Some(9), Some(-3)]);
    let plain = aggregate(scan, true, false, false);
    let plan: Arc<dyn ExecutionPlan> = Arc::new(
        IcebergMinMaxFilterExec::try_new(&plain, &ConfigOptions::default())
            .unwrap()
            .unwrap(),
    );
    let baseline: Arc<dyn ExecutionPlan> = Arc::new(plain);
    let registered =
        |plan: &Arc<dyn ExecutionPlan>| plan.metrics().map_or(0, |set| set.iter().count());
    let sum = |plan: &Arc<dyn ExecutionPlan>, name: &str| {
        plan.metrics()
            .and_then(|set| set.sum_by_name(name))
            .map_or(0, |value| value.as_usize())
    };
    let mut per_execution = None;
    for execution in 1..=3 {
        let before = registered(&plan);
        let baseline_before = registered(&baseline);
        let output = collect(Arc::clone(&plan), session.task_ctx()).await.unwrap();
        assert_eq!(result_value(&output), ScalarValue::Int32(Some(9)));
        let expected = collect(Arc::clone(&baseline), session.task_ctx())
            .await
            .unwrap();
        assert_eq!(result_value(&expected), ScalarValue::Int32(Some(9)));
        let added = registered(&plan) - before;
        let baseline_added = registered(&baseline) - baseline_before;
        assert!(added > 0 && added <= baseline_added + 1, "{added} vs {baseline_added}");
        assert_eq!(*per_execution.get_or_insert(added), added, "execution {execution}");
        assert_eq!(
            sum(&plan, "dynamic_filter_minmax_filters_attached"),
            execution
        );
        assert_eq!(sum(&plan, "output_rows"), execution);
    }
}
