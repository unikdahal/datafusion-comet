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

mod aggregate;
mod batch_filter;
mod consumer;
mod iceberg_reader;
mod join;
mod parquet_reader;
mod safety;
mod topk;

pub(crate) use aggregate::IcebergMinMaxFilterExec;
pub(crate) use join::DynamicFilterJoinExec;
pub(crate) use topk::TopKReaderFilterExec;

pub(super) use batch_filter::DynamicFilterExec;
