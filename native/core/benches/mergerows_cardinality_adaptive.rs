// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::common::{HashMap, HashSet};
use roaring::RoaringBitmap;

fn ids(layout: &str, n: usize) -> Vec<u64> {
    match layout {
        "dense" => (0..n as u64).collect(),
        "one-in-8" => (0..n as u64).map(|i| i * 8).collect(),
        "one-in-200" => (0..n as u64).map(|i| i * 200).collect(),
        "spark-8" => (0..n as u64).map(|i| ((i % 8) << 33) + i / 8).collect(),
        "spark-200" => (0..n as u64).map(|i| ((i % 200) << 33) + i / 200).collect(),
        "spark-2k" => (0..n as u64).map(|i| ((i % 2048) << 33) + i / 2048).collect(),
        "spark-16k" => (0..n as u64).map(|i| ((i % 16384) << 33) + i / 16384).collect(),
        "spark-200-grouped" => (0..n as u64).map(|i| ((i / 5000) << 33) + (i % 5000)).collect(),
        "spark-16k-grouped" => (0..n as u64).map(|i| ((i / 64) << 33) + (i % 64)).collect(),
        "one-in-200-shuffled" => (0..n as u64).map(|i| ((i * 16_777_619) % n as u64) * 200).collect(),
        _ => unreachable!(),
    }
}

enum Mode {
    Hash(HashSet<u64>),
    SingleHigh(u32, HashSet<u32>),
    Bitmap(HashMap<u32, RoaringBitmap>),
}

impl Mode {
    fn len(&self) -> usize {
        match self {
            Mode::Hash(s) => s.len(),
            Mode::SingleHigh(_,s) => s.len(),
            Mode::Bitmap(m) => m.values().map(|b| b.len() as usize).sum(),
        }
    }
    fn insert(&mut self, id: u64) {
        if let Mode::SingleHigh(hi, _) = self {
            if *hi != (id >> 32) as u32 {
                let Mode::SingleHigh(old_hi, lows) =
                    std::mem::replace(self, Mode::Hash(HashSet::new())) else { unreachable!() };
                let mut full = HashSet::with_capacity(lows.len());
                for low in lows {
                    assert!(full.insert(((old_hi as u64) << 32) | (low as u64)));
                }
                *self = Mode::Hash(full);
            }
        }
        match self {
            Mode::Hash(s) => assert!(s.insert(id)),
            Mode::SingleHigh(_,s) => assert!(s.insert(id as u32)),
            Mode::Bitmap(m) => assert!(m.entry((id >> 32) as u32).or_default().insert(id as u32)),
        }
    }
    fn switch_bitmap_to_hash(&mut self) {
        let Mode::Bitmap(bitmaps) =
            std::mem::replace(self, Mode::Hash(HashSet::new())) else { unreachable!() };
        let mut full = HashSet::with_capacity(
            bitmaps.values().map(|b| b.len() as usize).sum()
        );
        for (hi, bitmap) in bitmaps {
            for low in bitmap.iter() {
                assert!(full.insert(((hi as u64) << 32) | (low as u64)));
            }
        }
        *self = Mode::Hash(full);
    }
}

fn choose(sample: &[u64], allow_u32: bool) -> Mode {
    let mut his = HashSet::new();
    let mut containers: HashMap<u64, usize> = HashMap::new();
    let mut max_per_container = 0;
    for &id in sample {
        his.insert((id >> 32) as u32);
        let count = containers.entry(id >> 16).or_default();
        *count += 1;
        max_per_container = max_per_container.max(*count);
    }
    if his.len() <= 64 && max_per_container >= 384 {
        Mode::Bitmap(HashMap::new())
    } else if allow_u32 && his.len() == 1 {
        Mode::SingleHigh((sample[0] >> 32) as u32, HashSet::new())
    } else {
        Mode::Hash(HashSet::new())
    }
}

fn baseline(ids: &[u64]) -> usize {
    let mut set = HashSet::new();
    for &id in ids { assert!(set.insert(id)); }
    set.len()
}

fn adaptive(ids: &[u64], switch: bool, single_high: bool) -> usize {
    if ids.is_empty() { return 0; }
    let mut mode = choose(&ids[..ids.len().min(4096)], single_high);
    for chunk in ids.chunks(4096) {
        for &id in chunk { mode.insert(id); }
        if switch {
            if let Mode::Bitmap(bitmap) = &mode {
                if bitmap.len() > 64 {
                    mode.switch_bitmap_to_hash();
                }
            }
        }
    }
    mode.len()
}

fn bench(c: &mut Criterion) {
    const N: usize = 1_000_000;
    for layout in [
        "dense", "one-in-8", "one-in-200",
        "spark-8", "spark-200", "spark-2k", "spark-16k",
        "spark-200-grouped", "spark-16k-grouped", "one-in-200-shuffled",
    ] {
        let data = ids(layout, N);
        let mut group = c.benchmark_group(format!("adaptive/{layout}"));
        group.sample_size(10);
        group.warm_up_time(std::time::Duration::from_secs(1));
        group.measurement_time(std::time::Duration::from_secs(2));
        for (name, f) in [
            ("hash", baseline as fn(&[u64]) -> usize),
            ("adaptive-prefix", (|x| adaptive(x, false, false)) as fn(&[u64]) -> usize),
            ("adaptive-switch", (|x| adaptive(x, true, false)) as fn(&[u64]) -> usize),
            ("adaptive-u32-switch", (|x| adaptive(x, true, true)) as fn(&[u64]) -> usize),
        ] {
            group.bench_with_input(BenchmarkId::new(name, N), &data, |b, v| {
                b.iter(|| black_box(f(black_box(v))));
            });
        }
        group.finish();
    }
}
criterion_group!(benches, bench);
criterion_main!(benches);
