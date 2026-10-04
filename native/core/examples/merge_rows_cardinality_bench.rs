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

use datafusion::common::utils::memory::estimate_memory_size;
use datafusion::common::HashSet;
use roaring::RoaringTreemap;
use std::hint::black_box;
use std::time::{Duration, Instant};

const IDS: usize = 5_000_000;
const BATCH_ROWS: usize = 4_096;
const ROUNDS: usize = 5;

const HASH_FIXED_BYTES: usize = std::mem::size_of::<HashSet<i64>>();
const HASH_SLACK_BYTES: usize = 64;
const ROARING_FIXED_BYTES: usize = std::mem::size_of::<RoaringTreemap>();
const ROARING_PARTITION_OVERHEAD_BYTES: usize = 256;
const ROARING_CONTAINER_OVERHEAD_BYTES: usize = 64;

#[derive(Clone, Copy)]
struct Scenario {
    name: &'static str,
    stride: u64,
}

fn baseline_reserved_bytes(len: usize) -> usize {
    estimate_memory_size::<i64>(len, HASH_FIXED_BYTES)
        .unwrap()
        .checked_add(HASH_SLACK_BYTES)
        .unwrap()
}

fn roaring_reserved_bytes(seen: &RoaringTreemap) -> usize {
    let mut total = ROARING_FIXED_BYTES;
    for (_, bitmap) in seen.bitmaps() {
        let stats = bitmap.statistics();
        let payload = stats
            .n_bytes_array_containers
            .checked_add(stats.n_bytes_run_containers)
            .and_then(|bytes| bytes.checked_add(stats.n_bytes_bitset_containers))
            .unwrap();
        total = total
            .checked_add(ROARING_PARTITION_OVERHEAD_BYTES)
            .and_then(|bytes| {
                bytes.checked_add(stats.n_containers as usize * ROARING_CONTAINER_OVERHEAD_BYTES)
            })
            .and_then(|bytes| bytes.checked_add(usize::try_from(payload).unwrap()))
            .unwrap();
    }
    total
}

fn baseline_run(stride: u64) -> (Duration, usize) {
    let start = Instant::now();
    let mut seen = HashSet::new();

    for index in 0..IDS {
        let id = ((index as u64) * stride) as i64;
        if seen.len() < seen.capacity() {
            assert!(seen.insert(id));
        } else {
            assert!(!seen.contains(&id));
            let _projected = baseline_reserved_bytes(seen.len() + 1);
            seen.try_reserve(1).unwrap();
            assert!(seen.insert(id));
        }
    }

    black_box(&seen);
    (start.elapsed(), baseline_reserved_bytes(seen.len()))
}

fn roaring_run(stride: u64) -> (Duration, usize) {
    let start = Instant::now();
    let mut seen = RoaringTreemap::new();
    let mut reserved = 0usize;

    for batch_start in (0..IDS).step_by(BATCH_ROWS) {
        let batch_end = (batch_start + BATCH_ROWS).min(IDS);
        for index in batch_start..batch_end {
            let id = (index as u64) * stride;
            assert!(seen.insert(id));
        }
        reserved = roaring_reserved_bytes(&seen);
        black_box(reserved);
    }

    black_box(&seen);
    (start.elapsed(), reserved)
}

fn median(mut values: Vec<Duration>) -> Duration {
    values.sort_unstable();
    values[values.len() / 2]
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn main() {
    let scenarios = [
        Scenario {
            name: "dense",
            stride: 1,
        },
        Scenario {
            name: "one-in-8",
            stride: 8,
        },
        Scenario {
            name: "one-in-200",
            stride: 200,
        },
    ];

    println!("# MergeRows cardinality benchmark");
    println!();
    println!(
        "5,000,000 distinct ids, 4,096 ids per accounting batch, 5 timed rounds per implementation."
    );
    println!("Each round alternates implementation order to reduce runner-order bias.");
    println!();
    println!(
        "| layout | baseline HashSet median | RoaringTreemap median | runtime change | baseline reservation | roaring reservation | memory reduction |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|");

    for scenario in scenarios {
        let mut baseline_times = Vec::with_capacity(ROUNDS);
        let mut roaring_times = Vec::with_capacity(ROUNDS);
        let mut baseline_bytes = 0usize;
        let mut roaring_bytes = 0usize;

        for round in 0..ROUNDS {
            if round % 2 == 0 {
                let (baseline_time, bytes) = baseline_run(scenario.stride);
                baseline_times.push(baseline_time);
                baseline_bytes = bytes;

                let (roaring_time, bytes) = roaring_run(scenario.stride);
                roaring_times.push(roaring_time);
                roaring_bytes = bytes;
            } else {
                let (roaring_time, bytes) = roaring_run(scenario.stride);
                roaring_times.push(roaring_time);
                roaring_bytes = bytes;

                let (baseline_time, bytes) = baseline_run(scenario.stride);
                baseline_times.push(baseline_time);
                baseline_bytes = bytes;
            }
        }

        let baseline_median = median(baseline_times);
        let roaring_median = median(roaring_times);
        let runtime_change =
            (roaring_median.as_secs_f64() / baseline_median.as_secs_f64() - 1.0) * 100.0;
        let memory_reduction = baseline_bytes as f64 / roaring_bytes as f64;

        println!(
            "| {} | {:.2} ms | {:.2} ms | {:+.1}% | {:.2} MiB | {:.2} MiB | {:.1}x |",
            scenario.name,
            baseline_median.as_secs_f64() * 1000.0,
            roaring_median.as_secs_f64() * 1000.0,
            runtime_change,
            mib(baseline_bytes),
            mib(roaring_bytes),
            memory_reduction
        );
    }
}
