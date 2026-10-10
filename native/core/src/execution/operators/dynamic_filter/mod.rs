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

//! Execution-local runtime producers, reader attachment, and decoded fallback.

use arrow::datatypes::{DataType, TimeUnit};

mod aggregate;
mod batch_filter;
mod consumer;
mod iceberg_reader;
mod join;
mod parquet_reader;
mod safety;
mod topk;

#[cfg(test)]
mod tests;

pub(crate) use aggregate::IcebergMinMaxFilterExec;
pub(crate) use join::DynamicFilterJoinExec;
pub(crate) use topk::TopKReaderFilterExec;

pub(super) use batch_filter::DynamicFilterExec;

/// Shared producer/reader ceiling for exact IN-list membership.
/// The separate packed-key publication budget is 16 KiB.
pub(super) const MAX_IN_LIST_LITERALS: usize = 1024;

/// Returns whether `data_type` is an eligible join runtime pruning key type.
pub(crate) fn is_supported_join_key_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Date32
            | DataType::Timestamp(TimeUnit::Microsecond, _)
            | DataType::Decimal128(_, _)
    ) || is_runtime_pruning_string_key_type(data_type)
}

/// Returns whether `data_type` is an eligible Top-K runtime pruning key type.
pub(crate) fn is_supported_topk_key_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Date32
            | DataType::Timestamp(TimeUnit::Microsecond, _)
    ) || is_runtime_pruning_string_key_type(data_type)
}

/// Returns whether `data_type` is an eligible Min/Max aggregate runtime pruning key type.
pub(crate) fn is_supported_minmax_key_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int32
            | DataType::Int64
            | DataType::Date32
            | DataType::Timestamp(TimeUnit::Microsecond, _)
    ) || is_runtime_pruning_string_key_type(data_type)
}

/// Returns whether `data_type` is supported by native Parquet reader pushdown.
pub(crate) fn is_supported_parquet_reader_key_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

/// Returns whether `data_type` is an eligible runtime pruning string key type.
pub(crate) fn is_runtime_pruning_string_key_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => true,
        DataType::Dictionary(_, value) => matches!(
            value.as_ref(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ),
        _ => false,
    }
}
