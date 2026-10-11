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

// Registered only by the fork benchmark workflow's test-only CI checkout edit.
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use iceberg::scan::{FileScanTask, FileScanTaskMetrics};
use iceberg::spec::{DataFileFormat, Datum, NestedField, PrimitiveType, Schema, Type};

use super::{order_tasks_for_runtime_bound, RuntimeScanOrder};

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

#[test]
#[ignore = "release timing experiment; run only in GitHub Actions"]
fn runtime_task_order_timing() {
    let schema = Arc::new(
        Schema::builder()
            .with_fields(vec![NestedField::optional(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            )
            .into()])
            .build()
            .unwrap(),
    );
    for count in [1_000usize, 10_000, 100_000] {
        for layout in ["permuted", "overlapping", "unknown"] {
            let tasks: Arc<[FileScanTask]> = (0..count)
                .map(|index| {
                    // Coprime with every tested count; covers each rank exactly once.
                    let rank = (index * 7919) % count;
                    let bound = if layout == "overlapping" { 0 } else { rank as i32 };
                    let metrics = (layout != "unknown").then(|| {
                        Arc::new(
                            FileScanTaskMetrics::builder()
                                .with_record_count(Some(10))
                                .with_null_value_counts(HashMap::from([(1, 0)]))
                                .with_lower_bounds(HashMap::from([(1, Datum::int(bound))]))
                                .with_upper_bounds(HashMap::from([(1, Datum::int(bound + 10))]))
                                .build(),
                        )
                    });
                    FileScanTask::builder()
                        .with_file_size_in_bytes(1024)
                        .with_start(0)
                        .with_length(1024)
                        .with_data_file_path(format!("file-{rank}.parquet"))
                        .with_data_file_format(DataFileFormat::Parquet)
                        .with_schema(Arc::clone(&schema))
                        .with_project_field_ids(vec![1])
                        .with_case_sensitive(false)
                        .with_file_metrics(metrics)
                        .build()
                        .unwrap()
                })
                .collect::<Vec<_>>()
                .into();
            let mut clone_ms = Vec::new();
            let mut order_ms = Vec::new();
            let mut total_ms = Vec::new();
            for repetition in 0..22 {
                let started = Instant::now();
                let mut ordered = black_box(tasks.as_ref()).to_vec();
                let cloned = Instant::now();
                order_tasks_for_runtime_bound(
                    black_box(&mut ordered),
                    1,
                    RuntimeScanOrder::Descending { nulls_first: false },
                );
                let ordered: Arc<[FileScanTask]> = ordered.into();
                let finished = Instant::now();
                black_box(&ordered);
                if layout == "permuted" {
                    assert_eq!(ordered[0].data_file_path(), format!("file-{}.parquet", count - 1));
                } else {
                    assert_eq!(ordered[0].data_file_path(), tasks[0].data_file_path());
                }
                if repetition > 0 {
                    clone_ms.push((cloned - started).as_secs_f64() * 1000.0);
                    order_ms.push((finished - cloned).as_secs_f64() * 1000.0);
                    total_ms.push((finished - started).as_secs_f64() * 1000.0);
                }
            }
            println!(
                "ORDERING {{\"tasks\":{count},\"layout\":\"{layout}\",\"reps\":21,\"clone_ms\":{:.6},\"order_ms\":{:.6},\"total_ms\":{:.6}}}",
                median(&mut clone_ms), median(&mut order_ms), median(&mut total_ms)
            );
        }
    }
}
