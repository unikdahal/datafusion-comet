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
use ::iceberg::scan::FileScanTask;
use ::iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_expr::expressions::Literal;

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

#[test]
fn iceberg_topk_attaches_with_fresh_execution_local_thresholds() {
    for data_type in [DataType::Int32, DataType::Int64] {
        for descending in [false, true] {
            for nulls_first in [false, true] {
                let sort = sort(
                    scan(data_type.clone()),
                    10,
                    SortOptions {
                        descending,
                        nulls_first,
                    },
                );
                let plan = wrapper(&sort, &session(4));
                let first = plan.build_runtime_sort().unwrap();
                assert!(first.reader_filter_attached);
                assert!(first.sort.input().is::<IcebergScanExec>());
                let first = produced_filter(&first.sort);
                first
                    .update(Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("key", 0)),
                        Operator::Lt,
                        if data_type == DataType::Int32 {
                            lit(i32::MIN)
                        } else {
                            lit(i64::MIN)
                        },
                    )))
                    .unwrap();
                let second = plan.build_runtime_sort().unwrap();
                assert!(second.reader_filter_attached);
                let second = produced_filter(&second.sort);
                assert!(!Arc::ptr_eq(&first, &second));
                assert_eq!(
                    second
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
}
