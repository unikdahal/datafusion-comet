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

// Synthetic CPU isolation only; the query benchmark supplies end-to-end evidence.
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use iceberg::expr::{Bind, Reference};
use iceberg::spec::{Datum, NestedField, PrimitiveType, Schema, Type};
use prost::Message;

use super::{parse_iceberg_file_metrics, spark_operator};

fn median_ms(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

#[test]
#[ignore = "release timing experiment; run only in GitHub Actions"]
fn runtime_task_metrics_timing() {
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
    let metrics = spark_operator::IcebergFileMetrics {
        record_count: Some(10),
        value_counts: HashMap::from([(1, 10)]),
        null_value_counts: HashMap::from([(1, 0)]),
        lower_bounds: HashMap::from([(1, 0_i32.to_le_bytes().to_vec())]),
        upper_bounds: HashMap::from([(1, 9_i32.to_le_bytes().to_vec())]),
        ..Default::default()
    };
    let predicate = Reference::new("id").greater_than(Datum::int(5));
    for pool_size in [4usize, 1_000, 10_000] {
        let common = spark_operator::IcebergScanCommon {
            file_metrics_pool: vec![metrics.clone(); pool_size],
            ..Default::default()
        };
        let bytes = common.encode_to_vec();
        let mut decode = Vec::new();
        let mut bounds = Vec::new();
        let mut bind = Vec::new();
        for _ in 0..21 {
            let started = Instant::now();
            for _ in 0..100 {
                let decoded =
                    spark_operator::IcebergScanCommon::decode(black_box(bytes.as_slice())).unwrap();
                assert_eq!(decoded.file_metrics_pool.len(), pool_size);
                black_box(decoded);
            }
            decode.push(started.elapsed().as_secs_f64() * 10.0);
            let started = Instant::now();
            for _ in 0..100 {
                for _ in 0..4 {
                    black_box(parse_iceberg_file_metrics(black_box(&metrics), &schema));
                }
            }
            bounds.push(started.elapsed().as_secs_f64() * 10.0);
            let started = Instant::now();
            for _ in 0..100 {
                for _ in 0..4 {
                    black_box(predicate.clone().bind(Arc::clone(&schema), false).unwrap());
                }
            }
            bind.push(started.elapsed().as_secs_f64() * 10.0);
        }
        println!(
            "METRICS pool={pool_size} bytes={} decode_ms={:.6} bounds_4_ms={:.6} clone_bind_4_ms={:.6}",
            bytes.len(),
            median_ms(decode),
            median_ms(bounds),
            median_ms(bind)
        );
    }
}
