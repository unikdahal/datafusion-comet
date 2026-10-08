// Exact-set scaling experiment. GitHub CI only; intentionally outside PR #36.
use datafusion::common::{HashMap, HashSet};
use roaring::{RoaringBitmap, RoaringTreemap};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::time::Instant;
static ACTIVE: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);
struct Tracked;
unsafe impl GlobalAlloc for Tracked {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() && ACTIVE.load(Ordering::Relaxed) {
            let n = LIVE.fetch_add(l.size() as isize, Ordering::Relaxed) + l.size() as isize;
            PEAK.fetch_max(n, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        if ACTIVE.load(Ordering::Relaxed) { LIVE.fetch_sub(l.size() as isize, Ordering::Relaxed); }
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        let out = unsafe { System.realloc(p, l, new) };
        if !out.is_null() && ACTIVE.load(Ordering::Relaxed) {
            let n = LIVE.fetch_add(new as isize - l.size() as isize, Ordering::Relaxed)
                + new as isize - l.size() as isize;
            PEAK.fetch_max(n, Ordering::Relaxed);
        }
        out
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(l) };
        if !p.is_null() && ACTIVE.load(Ordering::Relaxed) {
            let n = LIVE.fetch_add(l.size() as isize, Ordering::Relaxed) + l.size() as isize;
            PEAK.fetch_max(n, Ordering::Relaxed);
        }
        p
    }
}
#[global_allocator]
static ALLOC: Tracked = Tracked;
fn ids(layout: &str, n: usize) -> Vec<u64> {
    let (partitions, stride, shuffled) = match layout {
        "dense" => (1u64, 1u64, false),
        "sparse-8" => (1, 8, false),
        "sparse-200" => (1, 200, false),
        "spark-8" => (8, 1, false),
        "spark-200" => (200, 1, false),
        "spark-2k" => (2048, 1, false),
        "spark-16k" => (16384, 1, false),
        "spark-200-shuffled" => (200, 1, true),
        "spark-16k-shuffled" => (16384, 1, true),
        _ => unreachable!(),
    };
    (0..n as u64).map(|i| {
        let j = if shuffled { i.wrapping_mul(1_048_583) % n as u64 } else { i };
        if stride != 1 { j * stride } else { ((j % partitions) << 33) + j / partitions }
    }).collect()
}
fn insert(design: &str, input: &[u64]) -> (usize, usize) {
    match design {
        "hash" => {
            let mut set = HashSet::new();
            for &id in input { assert!(set.insert(id)); }
            let retained = LIVE.load(Ordering::Relaxed) as usize;
            let count = set.len();
            std::hint::black_box(&set);
            drop(set);
            (count, retained)
        }
        "treemap" => {
            let mut set = RoaringTreemap::new();
            for &id in input { assert!(set.insert(id)); }
            let retained = LIVE.load(Ordering::Relaxed) as usize;
            let count = set.len() as usize;
            std::hint::black_box(&set);
            drop(set);
            (count, retained)
        }
        "partitioned-direct" | "partitioned-sorted" => {
            let mut set: HashMap<u32, RoaringBitmap> = HashMap::new();
            if design == "partitioned-sorted" {
                let mut batch = Vec::with_capacity(4096);
                for chunk in input.chunks(4096) {
                    batch.clear();
                    batch.extend_from_slice(chunk);
                    batch.sort_unstable();
                    for &id in &batch {
                        assert!(set.entry((id >> 32) as u32).or_default().insert(id as u32));
                    }
                }
                let count = set.values().map(RoaringBitmap::len).sum::<u64>() as usize;
                let retained = LIVE.load(Ordering::Relaxed) as usize;
                std::hint::black_box(&set);
                drop(batch);
                drop(set);
                (count, retained)
            } else {
                for &id in input {
                    assert!(set.entry((id >> 32) as u32).or_default().insert(id as u32));
                }
                let count = set.values().map(RoaringBitmap::len).sum::<u64>() as usize;
                let retained = LIVE.load(Ordering::Relaxed) as usize;
                std::hint::black_box(&set);
                drop(set);
                (count, retained)
            }
        }
        _ => unreachable!(),
    }
}
fn sample(design: &str, input: &[u64]) -> (f64, usize, usize) {
    LIVE.store(0, Ordering::Relaxed);
    PEAK.store(0, Ordering::Relaxed);
    let t = Instant::now();
    ACTIVE.store(true, Ordering::SeqCst);
    let (count, retained) = insert(design, input);
    ACTIVE.store(false, Ordering::SeqCst);
    assert_eq!(count, input.len());
    assert_eq!(LIVE.load(Ordering::Relaxed), 0);
    (t.elapsed().as_secs_f64() * 1000.0, retained, PEAK.load(Ordering::Relaxed) as usize)
}
fn main() {
    const N: usize = 5_000_000;
    let layouts = ["dense", "sparse-8", "sparse-200", "spark-8", "spark-200", "spark-2k", "spark-16k", "spark-200-shuffled", "spark-16k-shuffled"];
    let designs = ["hash", "treemap", "partitioned-direct", "partitioned-sorted"];
    println!("SCALING_BENCH,n,layout,design,median_ms,retained_bytes,peak_allocated_bytes");
    for layout in layouts {
        let input = ids(layout, N);
        for &design in &designs { let _ = sample(design, &input); }
        let mut results: Vec<Vec<(f64, usize, usize)>> = vec![Vec::new(); designs.len()];
        for rep in 0..5 {
            for off in 0..designs.len() {
                let idx = (off + rep) % designs.len();
                results[idx].push(sample(designs[idx], &input));
            }
        }
        for (i, design) in designs.iter().enumerate() {
            results[i].sort_by(|a,b| a.0.total_cmp(&b.0));
            let (time, retained, peak) = results[i][2];
            println!("SCALING_BENCH,{N},{layout},{design},{time:.3},{retained},{peak}");
        }
    }
}
