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

//! Opt-in MergeRows cardinality benchmark, kept on the benchmark branch only. Compares the
//! hash set on main, the pre-admission RoaringTreemap of the earlier revision, a RoaringTreemap
//! measured in full every batch, and the per-partition bitmaps measured as they change. Real heap
//! comes from Comet's accounting allocator, so it is accurate to about 64 KiB.

use super::MatchedRowIds;
use crate::alloc_accounting::current_balance;
use datafusion::common::utils::memory::estimate_memory_size;
use datafusion::common::HashSet;
use roaring::RoaringTreemap;
use std::hint::black_box;
use std::time::{Duration, Instant};

const IDS: usize = 5_000_000;
const BATCH_ROWS: usize = 8_192;
const ROUNDS: usize = 5;

#[derive(Clone, Copy)]
enum Layout {
    Stride(u64),
    SparkPartitions(u64),
    Shuffled(&'static Layout),
}

fn ids(layout: Layout) -> Vec<i64> {
    match layout {
        Layout::Stride(stride) => (0..IDS as u64).map(|i| (i * stride) as i64).collect(),
        // Spark's monotonically increasing ids: partition id << 33 plus a row counter, with the
        // partitions interleaved as a shuffled join would deliver them.
        Layout::SparkPartitions(partitions) => (0..IDS as u64)
            .map(|i| (((i % partitions) << 33) + i / partitions) as i64)
            .collect(),
        Layout::Shuffled(inner) => {
            let mut ids = ids(*inner);
            // Fisher-Yates with a fixed linear congruential generator.
            let mut state = 0x9e37_79b9_7f4a_7c15_u64;
            for i in (1..ids.len()).rev() {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                ids.swap(i, (state >> 33) as usize % (i + 1));
            }
            ids
        }
    }
}

struct Run {
    time: Duration,
    /// Reservation after the last batch.
    reserved: usize,
    /// Largest reservation requested, including pre-admission headroom.
    peak_reserved: usize,
    /// Real heap held by the state after the last batch.
    heap: usize,
}

fn heap_since(base: usize) -> usize {
    current_balance().saturating_sub(base)
}

// The hash set and its per-growth admission on main.
fn hash_set(ids: &[i64]) -> Run {
    let fixed = std::mem::size_of::<HashSet<i64>>();
    let estimate = |len: usize| estimate_memory_size::<i64>(len, fixed).unwrap() + 64;
    let base = current_balance();
    let start = Instant::now();
    let mut seen = HashSet::new();
    let mut reserved = estimate(0);
    for &id in ids {
        if seen.len() < seen.capacity() {
            assert!(seen.insert(id));
        } else {
            assert!(!seen.contains(&id));
            reserved = reserved.max(estimate(seen.len() + 1));
            seen.try_reserve(1).unwrap();
            assert!(seen.insert(id));
        }
    }
    let time = start.elapsed();
    let heap = heap_since(base);
    black_box(&seen);
    Run {
        time,
        reserved,
        peak_reserved: reserved,
        heap,
    }
}

// The earlier revision: sort, probe and pre-admit each batch with layout-derived headroom, then
// union a batch treemap. Its accounting reads `statistics` byte fields as reported.
mod pre_admission {
    use roaring::RoaringTreemap;

    const PARTITION: usize = 256;
    const CONTAINER: usize = 64;

    fn reserved(seen: &RoaringTreemap) -> usize {
        let mut total = std::mem::size_of::<RoaringTreemap>();
        for (_, bitmap) in seen.bitmaps() {
            let s = bitmap.statistics();
            total += PARTITION
                + s.n_containers as usize * CONTAINER
                + (s.n_bytes_array_containers
                    + s.n_bytes_run_containers
                    + s.n_bytes_bitset_containers) as usize;
        }
        total
    }

    fn payload_headroom(existing: u64, projected: u64) -> usize {
        if existing > 4096 {
            0
        } else if projected <= 4096 {
            (projected.max(4).next_power_of_two() * 8) as usize
        } else {
            32 * 1024
        }
    }

    fn headroom(seen: &RoaringTreemap, ids: &[u64]) -> usize {
        let (mut total, mut last, mut index) = (0usize, None, 0usize);
        while index < ids.len() {
            let key = ids[index] >> 16;
            let partition = (ids[index] >> 32) as u32;
            if last != Some(partition) {
                total += PARTITION;
                last = Some(partition);
            }
            let mut end = index + 1;
            while end < ids.len() && ids[end] >> 16 == key {
                end += 1;
            }
            let start = key << 16;
            let existing = seen.range_cardinality(start..=(start | u16::MAX as u64));
            total += CONTAINER + payload_headroom(existing, existing + (end - index) as u64);
            // The batch treemap built from these ids.
            total += CONTAINER + payload_headroom(0, (end - index) as u64);
            index = end;
        }
        total
    }

    pub(super) fn run(ids: &[i64]) -> super::Run {
        let base = super::current_balance();
        let start = std::time::Instant::now();
        let mut seen = RoaringTreemap::new();
        let mut retained = reserved(&seen);
        let mut peak = retained;
        for chunk in ids.chunks(super::BATCH_ROWS) {
            let mut batch: Vec<u64> = chunk.iter().map(|&id| id as u64).collect();
            batch.sort_unstable();
            assert!(batch.windows(2).all(|pair| pair[0] != pair[1]));
            assert!(!batch.iter().any(|id| seen.contains(*id)));
            let admitted =
                retained + headroom(&seen, &batch) + batch.capacity() * std::mem::size_of::<u64>();
            peak = peak.max(admitted);
            let batch = RoaringTreemap::from_sorted_iter(batch).unwrap();
            seen |= &batch;
            retained = reserved(&seen);
        }
        let time = start.elapsed();
        let heap = super::heap_since(base);
        std::hint::black_box(&seen);
        super::Run {
            time,
            reserved: retained,
            peak_reserved: peak,
            heap,
        }
    }
}

// A RoaringTreemap with every bitmap measured again after each batch.
fn treemap_full_scan(ids: &[i64]) -> Run {
    let base = current_balance();
    let start = Instant::now();
    let mut seen = RoaringTreemap::new();
    let mut reserved = 0;
    for chunk in ids.chunks(BATCH_ROWS) {
        for &id in chunk {
            assert!(seen.insert(id as u64));
        }
        reserved = seen
            .bitmaps()
            .map(|(_, bitmap)| {
                let s = bitmap.statistics();
                (s.n_bytes_array_containers / 2 + u64::from(s.n_bitset_containers) * 8192) as usize
            })
            .sum();
    }
    let time = start.elapsed();
    let heap = heap_since(base);
    black_box(&seen);
    Run {
        time,
        reserved,
        peak_reserved: reserved,
        heap,
    }
}

// This revision: per-partition bitmaps, each batch priced and reserved before it is inserted.
fn matched_row_ids(ids: &[i64]) -> Run {
    use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, UnboundedMemoryPool};
    let pool: std::sync::Arc<dyn MemoryPool> = std::sync::Arc::new(UnboundedMemoryPool::default());
    let mut reservation = MemoryConsumer::new("bench").register(&pool);
    let base = current_balance();
    let start = Instant::now();
    let mut seen = MatchedRowIds::default();
    let mut peak_reserved = 0;
    for chunk in ids.chunks(BATCH_ROWS) {
        seen.insert_batch(chunk.iter().copied(), chunk.len(), &mut reservation)
            .unwrap();
        peak_reserved = peak_reserved.max(pool.reserved());
    }
    let time = start.elapsed();
    let heap = heap_since(base);
    black_box(&seen);
    Run {
        time,
        reserved: reservation.size(),
        peak_reserved,
        heap,
    }
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

#[test]
#[ignore = "benchmark; run with --ignored --nocapture --test-threads=1"]
fn merge_rows_cardinality_benchmark() {
    static DENSE: Layout = Layout::Stride(1);
    static SPARK_200: Layout = Layout::SparkPartitions(200);
    let layouts = [
        ("dense", DENSE),
        ("one-in-8", Layout::Stride(8)),
        ("one-in-200", Layout::Stride(200)),
        ("spark-8-partitions", Layout::SparkPartitions(8)),
        ("spark-200-partitions", SPARK_200),
        ("spark-2k-partitions", Layout::SparkPartitions(2_048)),
        ("spark-16k-partitions", Layout::SparkPartitions(16_384)),
        ("shuffled-dense", Layout::Shuffled(&DENSE)),
        ("shuffled-spark-200", Layout::Shuffled(&SPARK_200)),
    ];
    let variants: [(&str, fn(&[i64]) -> Run); 4] = [
        ("HashSet (main)", hash_set),
        ("pre-admission treemap", pre_admission::run),
        ("treemap, full rescan", treemap_full_scan),
        ("this PR", matched_row_ids),
    ];

    println!("# MergeRows cardinality benchmark\n");
    println!(
        "{IDS} distinct ids, {BATCH_ROWS} ids per batch (Comet's default batch size), median of \
         {ROUNDS} rounds with the variant order rotated each round. Real heap is the state held \
         after the last batch, from Comet's accounting allocator (about 64 KiB resolution).\n"
    );
    println!(
        "| layout | variant | median ms | reserved MiB | peak reserved MiB | real heap MiB | reserved / heap |"
    );
    println!("|---|---|---:|---:|---:|---:|---:|");
    for (name, layout) in layouts {
        let ids = ids(layout);
        let mut runs: Vec<Vec<Run>> = (0..variants.len()).map(|_| Vec::new()).collect();
        for round in 0..ROUNDS {
            for offset in 0..variants.len() {
                let v = (round + offset) % variants.len();
                runs[v].push((variants[v].1)(&ids));
            }
        }
        for (v, (variant, _)) in variants.iter().enumerate() {
            let mut times: Vec<Duration> = runs[v].iter().map(|run| run.time).collect();
            times.sort_unstable();
            let last = runs[v].last().unwrap();
            println!(
                "| {name} | {variant} | {:.1} | {:.2} | {:.2} | {:.2} | {:.2} |",
                times[times.len() / 2].as_secs_f64() * 1000.0,
                mib(last.reserved),
                mib(last.peak_reserved),
                mib(last.heap),
                last.reserved as f64 / last.heap.max(1) as f64
            );
        }
    }
}
