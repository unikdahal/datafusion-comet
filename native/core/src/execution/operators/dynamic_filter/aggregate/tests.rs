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
use arrow::datatypes::{Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::expressions::{BinaryExpr, CastExpr};
use datafusion::physical_plan::aggregates::PhysicalGroupBy;
use datafusion::physical_plan::empty::EmptyExec;
use iceberg::scan::FileScanTask;
use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema as IcebergSchema, Type};

fn scan(data_type: DataType) -> Arc<dyn ExecutionPlan> {
    let iceberg_type = match data_type {
        DataType::Int32 => PrimitiveType::Int,
        DataType::Int64 => PrimitiveType::Long,
        DataType::Utf8 => PrimitiveType::String,
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
            Arc::new(Schema::new(vec![Field::new("key", data_type, true)])),
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
        aggregate(scan(DataType::Utf8), true, false, false),
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
