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

#[tokio::test]
async fn iceberg_topk_preserves_errors_in_late_secondary_sort_keys() {
    use std::collections::HashMap;

    use ::iceberg::scan::FileScanTaskMetrics;
    use ::iceberg::spec::Datum;

    let physical_schema = Arc::new(Schema::new(
        ["key", "payload"]
            .into_iter()
            .enumerate()
            .map(|(index, name)| {
                Field::new(name, DataType::Int32, false).with_metadata(HashMap::from([(
                    parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
                    (index + 1).to_string(),
                )]))
            })
            .collect::<Vec<_>>(),
    ));
    let table_schema = Arc::new(
        IcebergSchema::builder()
            .with_fields(vec![
                NestedField::required(1, "key", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "payload", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap(),
    );
    let mut files = Vec::new();
    let mut tasks = Vec::new();
    for (key, payload) in [(0, 1), (100, 0)] {
        let file = tempfile::NamedTempFile::new().unwrap();
        let batch = RecordBatch::try_new(
            Arc::clone(&physical_schema),
            vec![
                Arc::new(Int32Array::from(vec![key])),
                Arc::new(Int32Array::from(vec![payload])),
            ],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(file.reopen().unwrap(), Arc::clone(&physical_schema), None)
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        tasks.push(
            FileScanTask::builder()
                .with_data_file_path(file.path().to_string_lossy().into_owned())
                .with_data_file_format(DataFileFormat::Parquet)
                .with_file_size_in_bytes(file.as_file().metadata().unwrap().len())
                .with_start(0)
                .with_length(0)
                .with_schema(Arc::clone(&table_schema))
                .with_project_field_ids(vec![1, 2])
                .with_case_sensitive(false)
                .with_file_metrics(Some(Arc::new(FileScanTaskMetrics::new(
                    Some(1),
                    HashMap::from([(1, 1)]),
                    HashMap::from([(1, 0)]),
                    HashMap::new(),
                    HashMap::from([(1, Datum::int(key))]),
                    HashMap::from([(1, Datum::int(key))]),
                ))))
                .build()
                .unwrap(),
        );
        files.push(file);
    }
    let session = session(1);
    for enabled in [false, true] {
        let scan: Arc<dyn ExecutionPlan> = Arc::new(
            IcebergScanExec::new(
                files[0].path().to_string_lossy().into_owned(),
                Arc::clone(&physical_schema),
                Default::default(),
                String::new(),
                tasks.clone(),
                1,
            )
            .unwrap(),
        );
        let sort = SortExec::new(
            LexOrdering::new(vec![
                PhysicalSortExpr::new_default(Arc::new(Column::new("key", 0))),
                PhysicalSortExpr::new_default(Arc::new(BinaryExpr::new(
                    lit(1_i32),
                    Operator::Divide,
                    Arc::new(Column::new("payload", 1)),
                ))),
            ])
            .unwrap(),
            scan,
        )
        .with_fetch(Some(1));
        let plan: Arc<dyn ExecutionPlan> = if enabled {
            let wrapper =
                TopKReaderFilterExec::try_new(&sort, session.copied_config().options()).unwrap();
            assert!(
                wrapper.is_none(),
                "fallible secondary key must remain a boundary"
            );
            Arc::new(sort)
        } else {
            Arc::new(sort)
        };
        let error = collect(plan, session.task_ctx()).await.unwrap_err();
        assert!(
            error.to_string().to_lowercase().contains("divide by zero"),
            "enabled={enabled}: {error}"
        );
    }
}
