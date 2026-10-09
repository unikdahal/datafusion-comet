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

use super::*;
use arrow::datatypes::{DataType, Field, TimeUnit};

/// Cross-reference test with Scala:
/// `spark/src/test/scala/org/apache/spark/sql/comet/RuntimePruningKeyTypesSuite.scala`.
#[test]
fn test_join_key_types() {
    let supported = vec![
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DataType::Decimal128(10, 2),
        DataType::Decimal128(38, 18),
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::LargeUtf8)),
        DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8View)),
    ];

    for data_type in &supported {
        assert!(
            is_supported_join_key_type(data_type),
            "expected {data_type:?} to be supported as join key"
        );
        assert!(
            RuntimePruningConsumer::Join.is_supported(data_type),
            "expected {data_type:?} to be supported via RuntimePruningConsumer::Join"
        );
    }

    let unsupported = vec![
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Float32,
        DataType::Float64,
        DataType::Boolean,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
        DataType::FixedSizeBinary(16),
        DataType::Date64,
        DataType::Time32(TimeUnit::Second),
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Timestamp(TimeUnit::Second, None),
        DataType::Timestamp(TimeUnit::Millisecond, None),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        DataType::Decimal256(50, 10),
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32)),
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int64)),
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        DataType::Struct(vec![Field::new("a", DataType::Int32, true)].into()),
    ];

    for data_type in &unsupported {
        assert!(
            !is_supported_join_key_type(data_type),
            "expected {data_type:?} to be unsupported as join key"
        );
        assert!(
            !RuntimePruningConsumer::Join.is_supported(data_type),
            "expected {data_type:?} to be unsupported via RuntimePruningConsumer::Join"
        );
    }
}

#[test]
fn test_topk_key_types() {
    let supported = vec![
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    ];

    for data_type in &supported {
        assert!(
            is_supported_topk_key_type(data_type),
            "expected {data_type:?} to be supported as topk key"
        );
        assert!(
            RuntimePruningConsumer::TopK.is_supported(data_type),
            "expected {data_type:?} to be supported via RuntimePruningConsumer::TopK"
        );
    }

    let unsupported = vec![
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Decimal128(10, 2),
        DataType::Decimal128(38, 18),
        DataType::UInt32,
        DataType::Float32,
        DataType::Float64,
        DataType::Boolean,
        DataType::Binary,
        DataType::Timestamp(TimeUnit::Millisecond, None),
    ];

    for data_type in &unsupported {
        assert!(
            !is_supported_topk_key_type(data_type),
            "expected {data_type:?} to be unsupported as topk key"
        );
        assert!(
            !RuntimePruningConsumer::TopK.is_supported(data_type),
            "expected {data_type:?} to be unsupported via RuntimePruningConsumer::TopK"
        );
    }
}

#[test]
fn test_minmax_key_types() {
    let supported = vec![
        DataType::Int32,
        DataType::Int64,
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    ];

    for data_type in &supported {
        assert!(
            is_supported_minmax_key_type(data_type),
            "expected {data_type:?} to be supported as minmax key"
        );
        assert!(
            RuntimePruningConsumer::MinMax.is_supported(data_type),
            "expected {data_type:?} to be supported via RuntimePruningConsumer::MinMax"
        );
    }

    let unsupported = vec![
        DataType::Int8,
        DataType::Int16,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Decimal128(10, 2),
        DataType::Decimal128(38, 18),
        DataType::Float32,
        DataType::Float64,
        DataType::Boolean,
        DataType::Binary,
    ];

    for data_type in &unsupported {
        assert!(
            !is_supported_minmax_key_type(data_type),
            "expected {data_type:?} to be unsupported as minmax key"
        );
        assert!(
            !RuntimePruningConsumer::MinMax.is_supported(data_type),
            "expected {data_type:?} to be unsupported via RuntimePruningConsumer::MinMax"
        );
    }
}

#[test]
fn test_parquet_reader_key_types() {
    let supported = vec![
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ];

    for data_type in &supported {
        assert!(
            is_supported_parquet_reader_key_type(data_type),
            "expected {data_type:?} to be supported as parquet reader key"
        );
        assert!(
            RuntimePruningConsumer::ParquetReader.is_supported(data_type),
            "expected {data_type:?} to be supported via RuntimePruningConsumer::ParquetReader"
        );
    }

    let unsupported = vec![
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DataType::Decimal128(10, 2),
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Float32,
        DataType::Boolean,
    ];

    for data_type in &unsupported {
        assert!(
            !is_supported_parquet_reader_key_type(data_type),
            "expected {data_type:?} to be unsupported as parquet reader key"
        );
        assert!(
            !RuntimePruningConsumer::ParquetReader.is_supported(data_type),
            "expected {data_type:?} to be unsupported via RuntimePruningConsumer::ParquetReader"
        );
    }
}

#[test]
fn test_column_stats_key_types() {
    let supported = vec![
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Date32,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        DataType::Decimal128(10, 2),
        DataType::Decimal128(38, 18),
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Utf8View,
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
    ];

    for data_type in &supported {
        assert!(
            is_supported_column_stats_key_type(data_type),
            "expected {data_type:?} to be supported for column stats"
        );
        assert!(
            is_runtime_pruning_key_type(data_type),
            "expected {data_type:?} to be supported via is_runtime_pruning_key_type alias"
        );
        assert!(
            RuntimePruningConsumer::ColumnStats.is_supported(data_type),
            "expected {data_type:?} to be supported via RuntimePruningConsumer::ColumnStats"
        );
    }
}

#[test]
fn test_is_runtime_pruning_string_key_type() {
    assert!(is_runtime_pruning_string_key_type(&DataType::Utf8));
    assert!(is_runtime_pruning_string_key_type(&DataType::LargeUtf8));
    assert!(is_runtime_pruning_string_key_type(&DataType::Utf8View));
    assert!(is_runtime_pruning_string_key_type(&DataType::Dictionary(
        Box::new(DataType::Int32),
        Box::new(DataType::Utf8)
    )));
    assert!(!is_runtime_pruning_string_key_type(&DataType::Int32));
    assert!(!is_runtime_pruning_string_key_type(&DataType::Dictionary(
        Box::new(DataType::Int32),
        Box::new(DataType::Int32)
    )));
}
