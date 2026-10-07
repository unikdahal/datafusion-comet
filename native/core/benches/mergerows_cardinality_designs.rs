// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use datafusion::common::{HashMap, HashSet};
use roaring::{RoaringBitmap, RoaringTreemap};

fn ids(layout: &str, n: usize) -> Vec<i64> {
    match layout {
        "dense" => (0..n as i64).collect(),
        "one-in-8" => (0..n as i64).map(|i| i * 8).collect(),
        "one-in-200" => (0..n as i64).map(|i| i * 200).collect(),
        "spark-8" => (0..n as i64).map(|i| ((i % 8) << 33) + i / 8).collect(),
        "spark-200" => (0..n as i64).map(|i| ((i % 200) << 33) + i / 200).collect(),
        "spark-2k" => (0..n as i64).map(|i| ((i % 2_048) << 33) + i / 2_048).collect(),
        "spark-16k" => (0..n as i64).map(|i| ((i % 16_384) << 33) + i / 16_384).collect(),
        _ => unreachable!(),
    }
}

fn hash(ids: &[i64]) -> usize {
    let mut seen = HashSet::new();
    for &id in ids {
        assert!(seen.insert(id));
    }
    seen.len()
}

fn treemap(ids: &[i64]) -> u64 {
    let mut seen = RoaringTreemap::new();
    for &id in ids {
        assert!(seen.insert(id as u64));
    }
    seen.len()
}

fn partitioned_direct(ids: &[i64]) -> u64 {
    let mut seen: HashMap<u32, RoaringBitmap> = HashMap::new();
    for &id in ids {
        let id = id as u64;
        assert!(seen.entry((id >> 32) as u32).or_default().insert(id as u32));
    }
    seen.values().map(RoaringBitmap::len).sum()
}

fn partitioned_sorted_batches(ids: &[i64]) -> u64 {
    let mut seen: HashMap<u32, RoaringBitmap> = HashMap::new();
    let mut batch = Vec::with_capacity(4096);
    for chunk in ids.chunks(4096) {
        batch.clear();
        batch.extend(chunk.iter().map(|&id| id as u64));
        batch.sort_unstable();
        for &id in &batch {
            assert!(seen.entry((id >> 32) as u32).or_default().insert(id as u32));
        }
    }
    seen.values().map(RoaringBitmap::len).sum()
}

fn batch_treemap(ids: &[i64]) -> u64 {
    let mut seen = RoaringTreemap::new();
    for chunk in ids.chunks(4096) {
        let mut batch = RoaringTreemap::new();
        for &id in chunk {
            let id = id as u64;
            assert!(batch.insert(id));
            assert!(!seen.contains(id));
        }
        seen |= batch;
    }
    seen.len()
}

fn bench(c: &mut Criterion) {
    const N: usize = 1_000_000;
    for layout in ["dense", "one-in-8", "one-in-200", "spark-8", "spark-200", "spark-2k", "spark-16k"] {
        let input = ids(layout, N);
        let mut group = c.benchmark_group(format!("cardinality/{layout}"));
        group.sample_size(10);
        group.warm_up_time(std::time::Duration::from_secs(1));
        group.measurement_time(std::time::Duration::from_secs(2));
        for (name, f) in [
            ("hash", hash as fn(&[i64]) -> usize),
        ] {
            group.bench_with_input(BenchmarkId::new(name, N), &input, |b, v| {
                b.iter_batched(|| v.as_slice(), |x| black_box(f(x)), BatchSize::SmallInput)
            });
        }
        group.bench_with_input(BenchmarkId::new("treemap", N), &input, |b, v| {
            b.iter(|| black_box(treemap(black_box(v))))
        });
        group.bench_with_input(BenchmarkId::new("partitioned-direct", N), &input, |b, v| {
            b.iter(|| black_box(partitioned_direct(black_box(v))))
        });
        group.bench_with_input(BenchmarkId::new("partitioned-sorted-batches", N), &input, |b, v| {
            b.iter(|| black_box(partitioned_sorted_batches(black_box(v))))
        });
        group.bench_with_input(BenchmarkId::new("batch-treemap", N), &input, |b, v| {
            b.iter(|| black_box(batch_treemap(black_box(v))))
        });
        group.finish();
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
