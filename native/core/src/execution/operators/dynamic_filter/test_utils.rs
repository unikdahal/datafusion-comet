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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::ExecutionPlan;
use iceberg::scan::FileScanTask;
use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::file::properties::WriterProperties;

use crate::execution::operators::IcebergScanExec;

pub(super) fn iceberg_input(
    values: Vec<Option<i32>>,
) -> (tempfile::NamedTempFile, Arc<dyn ExecutionPlan>) {
    let file = tempfile::NamedTempFile::new().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, true).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.into(),
            "1".into(),
        )])),
    ]));
    let rows = values.len();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(values))],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let mut writer = ArrowWriter::try_new(
        file.reopen().unwrap(),
        Arc::clone(&schema),
        Some(properties),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let table_schema = Arc::new(
        IcebergSchema::builder()
            .with_fields(vec![
                NestedField::optional(1, "key", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap(),
    );
    let size = file.as_file().metadata().unwrap().len();
    let task = FileScanTask::builder()
        .with_data_file_path(file.path().to_string_lossy().into_owned())
        .with_data_file_format(DataFileFormat::Parquet)
        .with_file_size_in_bytes(size)
        .with_start(0)
        .with_length(0)
        .with_record_count(Some(rows as u64))
        .with_schema(table_schema)
        .with_project_field_ids(vec![1])
        .with_case_sensitive(false)
        .build()
        .unwrap();
    let scan = IcebergScanExec::new(
        file.path().to_string_lossy().into_owned(),
        schema,
        Default::default(),
        String::new(),
        vec![task],
        1,
    )
    .unwrap();
    (file, Arc::new(scan))
}
