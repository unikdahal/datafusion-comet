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
const ROARING_ARRAY_MIN_CAPACITY: u64 = 4;
const ROARING_ARRAY_SLOT_UPPER_BYTES: u64 = 8;
const ROARING_ARRAY_LIMIT: u64 = 4096;
const ROARING_BITMAP_TRANSITION_HEADROOM_BYTES: u64 = 32 * 1024;

#[derive(Clone, Copy)]
enum Layout {
    Stride(u64),
    SparkPartitions(u64),
}

#[derive(Clone, Copy)]
struct Scenario {
    name: &'static str,
    layout: Layout,
}

fn row_id(layout: Layout, index: usize) -> u64 {
    let index = index as u64;
    match layout {
        Layout::Stride(stride) => index * stride,
        // Spark's MonotonicallyIncreasingID encodes partition_id << 33 plus a per-partition row
        // counter. Interleave partitions here so the benchmark also exercises treemap lookups.
        Layout::SparkPartitions(partitions) => {
            let partition = index % partitions;
            let row = index / partitions;
            (partition << 33) + row
        }
    }
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

fn projected_container_payload_headroom(
    existing_cardinality: u64,
    projected_cardinality: u64,
) -> usize {
    let bytes = if existing_cardinality > ROARING_ARRAY_LIMIT {
        0
    } else if projected_cardinality <= ROARING_ARRAY_LIMIT {
        let capacity = projected_cardinality
            .max(ROARING_ARRAY_MIN_CAPACITY)
            .checked_next_power_of_two()
            .unwrap();
        capacity * ROARING_ARRAY_SLOT_UPPER_BYTES
    } else {
        ROARING_BITMAP_TRANSITION_HEADROOM_BYTES
    };
    usize::try_from(bytes).unwrap()
}

fn batch_roaring_peak(ids: &[u64]) -> usize {
    let mut total = ROARING_FIXED_BYTES;
    let mut last_partition = None;
    let mut index = 0usize;

    while index < ids.len() {
        let container_key = ids[index] >> 16;
        let partition = (ids[index] >> 32) as u32;
        if last_partition != Some(partition) {
            total += ROARING_PARTITION_OVERHEAD_BYTES;
            last_partition = Some(partition);
        }

        let mut end_index = index + 1;
        while end_index < ids.len() && (ids[end_index] >> 16) == container_key {
            end_index += 1;
        }

        let cardinality = (end_index - index) as u64;
        total +=
            ROARING_CONTAINER_OVERHEAD_BYTES + projected_container_payload_headroom(0, cardinality);
        index = end_index;
    }

    total
}

fn seen_contains_any_sorted(seen: &RoaringTreemap, ids: &[u64]) -> bool {
    let mut bitmaps = seen.bitmaps();
    let mut current = bitmaps.next();
    let mut index = 0usize;

    while index < ids.len() {
        let partition = (ids[index] >> 32) as u32;
        let mut end_index = index + 1;
        while end_index < ids.len() && (ids[end_index] >> 32) as u32 == partition {
            end_index += 1;
        }

        while current
            .as_ref()
            .is_some_and(|(existing_partition, _)| *existing_partition < partition)
        {
            current = bitmaps.next();
        }

        if let Some((existing_partition, bitmap)) = current.as_ref() {
            if *existing_partition == partition
                && ids[index..end_index]
                    .iter()
                    .any(|id| bitmap.contains(*id as u32))
            {
                return true;
            }
        }

        index = end_index;
    }

    false
}

fn roaring_batch_headroom(seen: &RoaringTreemap, ids: &[u64]) -> usize {
    let mut headroom = 0usize;
    let mut last_partition = None;
    let mut index = 0usize;

    while index < ids.len() {
        let container_key = ids[index] >> 16;
        let partition = (ids[index] >> 32) as u32;
        if last_partition != Some(partition) {
            headroom += ROARING_PARTITION_OVERHEAD_BYTES;
            last_partition = Some(partition);
        }

        let mut end_index = index + 1;
        while end_index < ids.len() && (ids[end_index] >> 16) == container_key {
            end_index += 1;
        }

        let start = container_key << 16;
        let end = start | u16::MAX as u64;
        let existing = seen.range_cardinality(start..=end);
        let projected = existing + (end_index - index) as u64;
        headroom += ROARING_CONTAINER_OVERHEAD_BYTES
            + projected_container_payload_headroom(existing, projected);
        index = end_index;
    }
    headroom
}

fn baseline_run(layout: Layout) -> (Duration, usize) {
    let start = Instant::now();
    let mut seen = HashSet::new();

    for index in 0..IDS {
        let id = row_id(layout, index) as i64;
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

fn roaring_run(layout: Layout) -> (Duration, usize, usize) {
    let start = Instant::now();
    let mut seen = RoaringTreemap::new();
    let mut retained = 0usize;
    let mut peak_admitted = 0usize;

    for batch_start in (0..IDS).step_by(BATCH_ROWS) {
        let batch_end = (batch_start + BATCH_ROWS).min(IDS);
        let mut ids = Vec::with_capacity(batch_end - batch_start);

        for index in batch_start..batch_end {
            ids.push(row_id(layout, index));
        }
        ids.sort_unstable();
        assert!(ids.windows(2).all(|pair| pair[0] != pair[1]));
        assert!(!seen_contains_any_sorted(&seen, &ids));

        let current = roaring_reserved_bytes(&seen);
        let admitted = current
            + roaring_batch_headroom(&seen, &ids)
            + batch_roaring_peak(&ids)
            + ids.capacity() * std::mem::size_of::<u64>();
        peak_admitted = peak_admitted.max(admitted);

        let batch_seen = RoaringTreemap::from_sorted_iter(ids.into_iter()).unwrap();
        seen |= batch_seen;

        retained = roaring_reserved_bytes(&seen);
        assert!(
            retained <= admitted,
            "pre-admission bound must cover post-insert reservation"
        );
        black_box(retained);
    }

    black_box(&seen);
    (start.elapsed(), retained, peak_admitted)
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
            layout: Layout::Stride(1),
        },
        Scenario {
            name: "one-in-8",
            layout: Layout::Stride(8),
        },
        Scenario {
            name: "one-in-200",
            layout: Layout::Stride(200),
        },
        Scenario {
            name: "spark-8-partitions",
            layout: Layout::SparkPartitions(8),
        },
        Scenario {
            name: "spark-200-partitions",
            layout: Layout::SparkPartitions(200),
        },
        Scenario {
            name: "spark-2k-partitions",
            layout: Layout::SparkPartitions(2_048),
        },
        Scenario {
            name: "spark-16k-partitions",
            layout: Layout::SparkPartitions(16_384),
        },
    ];

    println!("# MergeRows cardinality benchmark");
    println!();
    println!(
        "5,000,000 distinct ids, 4,096 ids per accounting batch, 5 timed rounds per implementation."
    );
    println!("Each round alternates implementation order to reduce runner-order bias.");
    println!(
        "Roaring peak reservation includes conservative pre-admission headroom before each batch."
    );
    println!(
        "spark-* layouts model Spark MonotonicallyIncreasingID as partition_id << 33 plus row index."
    );
    println!();
    println!(
        "| layout | baseline HashSet median | RoaringTreemap median | runtime change | baseline peak reservation | roaring retained | roaring peak reservation | peak reduction |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|");

    for scenario in scenarios {
        let mut baseline_times = Vec::with_capacity(ROUNDS);
        let mut roaring_times = Vec::with_capacity(ROUNDS);
        let mut baseline_bytes = 0usize;
        let mut roaring_retained_bytes = 0usize;
        let mut roaring_peak_bytes = 0usize;

        for round in 0..ROUNDS {
            if round % 2 == 0 {
                let (baseline_time, bytes) = baseline_run(scenario.layout);
                baseline_times.push(baseline_time);
                baseline_bytes = bytes;

                let (roaring_time, retained, peak) = roaring_run(scenario.layout);
                roaring_times.push(roaring_time);
                roaring_retained_bytes = retained;
                roaring_peak_bytes = peak;
            } else {
                let (roaring_time, retained, peak) = roaring_run(scenario.layout);
                roaring_times.push(roaring_time);
                roaring_retained_bytes = retained;
                roaring_peak_bytes = peak;

                let (baseline_time, bytes) = baseline_run(scenario.layout);
                baseline_times.push(baseline_time);
                baseline_bytes = bytes;
            }
        }

        let baseline_median = median(baseline_times);
        let roaring_median = median(roaring_times);
        let runtime_change =
            (roaring_median.as_secs_f64() / baseline_median.as_secs_f64() - 1.0) * 100.0;
        let memory_reduction = baseline_bytes as f64 / roaring_peak_bytes as f64;

        println!(
            "| {} | {:.2} ms | {:.2} ms | {:+.1}% | {:.2} MiB | {:.2} MiB | {:.2} MiB | {:.1}x |",
            scenario.name,
            baseline_median.as_secs_f64() * 1000.0,
            roaring_median.as_secs_f64() * 1000.0,
            runtime_change,
            mib(baseline_bytes),
            mib(roaring_retained_bytes),
            mib(roaring_peak_bytes),
            memory_reduction
        );
    }
}
