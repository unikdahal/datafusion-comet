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
        "sparse-200-shuffled" => (1, 200, true),
        "spark-8" => (8, 1, false),
        "spark-200" => (200, 1, false),
        "spark-2k" => (2048, 1, false),
        "spark-16k" => (16384, 1, false),
        "spark-200-shuffled" => (200, 1, true),
        "spark-16k-shuffled" => (16384, 1, true),
        "spark-200-high-offset" => (200, 1, false),
        _ => unreachable!(),
    };
    (0..n as u64).map(|i| {
        let j = if shuffled { i.wrapping_mul(1_048_583) % n as u64 } else { i };
        if stride != 1 { j * stride } else { ((j % partitions) << 33) + j / partitions + if layout == "spark-200-high-offset" { 100_000_000 } else { 0 } }
    }).collect()
}

enum Segment {
    Dense { words: Vec<u64>, count: usize },
    Roaring(RoaringBitmap),
}
fn segmented_adaptive(input: &[u64]) -> (usize, usize) {
    let mut sets: HashMap<u32, Segment> = HashMap::new();
    for &id in input {
        let high = (id >> 32) as u32;
        let low = id as u32;
        let segment = sets.entry(high)
            .or_insert_with(|| Segment::Dense { words: Vec::new(), count: 0 });
        match segment {
            Segment::Roaring(bitmap) => { assert!(bitmap.insert(low)); }
            Segment::Dense { words, count } => {
                // Reject bitsets whose span becomes expensive relative to membership density.
                // One megabit of headroom accommodates reordering within a normal Spark partition.
                let allowable_span = 1_048_576usize.saturating_add(32 * (*count + 1));
                if (low as usize) >= 64_000_000 || (low as usize) > allowable_span {
                    let mut bitmap = RoaringBitmap::new();
                    for (word_index, &original) in words.iter().enumerate() {
                        let mut word = original;
                        while word != 0 {
                            let bit = word.trailing_zeros() as usize;
                            assert!(bitmap.insert((word_index * 64 + bit) as u32));
                            word &= word - 1;
                        }
                    }
                    assert!(bitmap.insert(low));
                    *segment = Segment::Roaring(bitmap);
                } else {
                    let index = (low >> 6) as usize;
                    if index >= words.len() { words.resize(index + 1, 0); }
                    let flag = 1u64 << (low & 63);
                    assert_eq!(words[index] & flag, 0);
                    words[index] |= flag;
                    *count += 1;
                }
            }
        }
    }
    let retained = LIVE.load(Ordering::Relaxed) as usize;
    let count: usize = sets.values().map(|segment| match segment {
        Segment::Dense { count, .. } => *count,
        Segment::Roaring(bitmap) => bitmap.len() as usize,
    }).sum();
    std::hint::black_box(&sets);
    drop(sets);
    (count, retained)
}


enum AnchoredSegment {
    Dense { base_word: u32, words: Vec<u64>, count: usize },
    Roaring { bitmap: RoaringBitmap, prefer_append: bool },
}

fn segmented_anchored(input: &[u64]) -> (usize, usize) {
    const MAX_WORDS: usize = 131_072;
    let mut partitions: HashMap<u32, AnchoredSegment> = HashMap::new();
    for &id in input {
        let high = (id >> 32) as u32;
        let low = id as u32;
        let word = low >> 6;
        let entry = partitions.entry(high).or_insert_with(|| AnchoredSegment::Dense {
            base_word: word,
            words: Vec::new(),
            count: 0,
        });
        match entry {
            AnchoredSegment::Roaring { bitmap, prefer_append } => {
                if !*prefer_append || bitmap.try_push(low).is_err() {
                    assert!(bitmap.insert(low));
                }
            }
            AnchoredSegment::Dense { base_word, words, count } => {
                let current_end = (*base_word as u64) + words.len().saturating_sub(1) as u64;
                let minimum = (*base_word).min(word);
                let maximum = current_end.max(word as u64);
                let proposed_words = (maximum - minimum as u64 + 1) as usize;
                let poor_density =
                    proposed_words > 16usize.saturating_add(2usize.saturating_mul(*count + 1));
                if proposed_words > MAX_WORDS || poor_density {
                    let mut bitmap = RoaringBitmap::new();
                    for (i, &bits) in words.iter().enumerate() {
                        let mut remaining = bits;
                        while remaining != 0 {
                            let bit = remaining.trailing_zeros() as u32;
                            assert!(bitmap.insert(((*base_word as u64 + i as u64) * 64 + bit as u64) as u32));
                            remaining &= remaining - 1;
                        }
                    }
                    if !poor_density || bitmap.try_push(low).is_err() {
                        assert!(bitmap.insert(low));
                    }
                    *entry = AnchoredSegment::Roaring { bitmap, prefer_append: poor_density };
                } else {
                    if minimum < *base_word {
                        let offset = (*base_word - minimum) as usize;
                        let mut next = vec![0u64; proposed_words];
                        next[offset..offset + words.len()].copy_from_slice(words);
                        *words = next;
                        *base_word = minimum;
                    } else if proposed_words > words.len() {
                        words.resize(proposed_words, 0);
                    }
                    let idx = (word - *base_word) as usize;
                    let flag = 1u64 << (low & 63);
                    assert_eq!(words[idx] & flag, 0);
                    words[idx] |= flag;
                    *count += 1;
                }
            }
        }
    }
    let retained = LIVE.load(Ordering::Relaxed) as usize;
    let count = partitions.values().map(|p| match p {
        AnchoredSegment::Dense {count, ..} => *count,
        AnchoredSegment::Roaring { bitmap, .. } => bitmap.len() as usize,
    }).sum();
    std::hint::black_box(&partitions);
    drop(partitions);
    (count, retained)
}

enum AnchoredHashSegment {
    Dense { base_word: u32, words: Vec<u64>, count: usize },
    Sparse(HashSet<u32>),
}

fn segmented_anchored_hash(input: &[u64]) -> (usize, usize) {
    const MAX_WORDS: usize = 131_072;
    let mut partitions: HashMap<u32, AnchoredHashSegment> = HashMap::new();
    for &id in input {
        let high = (id >> 32) as u32;
        let low = id as u32;
        let word = low >> 6;
        let entry = partitions.entry(high).or_insert_with(|| AnchoredHashSegment::Dense {
            base_word: word,
            words: Vec::new(),
            count: 0,
        });
        match entry {
            AnchoredHashSegment::Sparse(bitmap) => {
                assert!(bitmap.insert(low));
            }
            AnchoredHashSegment::Dense { base_word, words, count } => {
                let current_end = (*base_word as u64) + words.len().saturating_sub(1) as u64;
                let minimum = (*base_word).min(word);
                let maximum = current_end.max(word as u64);
                let proposed_words = (maximum - minimum as u64 + 1) as usize;
                if proposed_words > MAX_WORDS
                    || proposed_words > 16usize.saturating_add(2usize.saturating_mul(*count + 1))
                {
                    let mut bitmap = HashSet::<u32>::new();
                    for (i, &bits) in words.iter().enumerate() {
                        let mut remaining = bits;
                        while remaining != 0 {
                            let bit = remaining.trailing_zeros() as u32;
                            assert!(bitmap.insert(((*base_word as u64 + i as u64) * 64 + bit as u64) as u32));
                            remaining &= remaining - 1;
                        }
                    }
                    assert!(bitmap.insert(low));
                    *entry = AnchoredHashSegment::Sparse(bitmap);
                } else {
                    if minimum < *base_word {
                        let offset = (*base_word - minimum) as usize;
                        let mut next = vec![0u64; proposed_words];
                        next[offset..offset + words.len()].copy_from_slice(words);
                        *words = next;
                        *base_word = minimum;
                    } else if proposed_words > words.len() {
                        words.resize(proposed_words, 0);
                    }
                    let idx = (word - *base_word) as usize;
                    let flag = 1u64 << (low & 63);
                    assert_eq!(words[idx] & flag, 0);
                    words[idx] |= flag;
                    *count += 1;
                }
            }
        }
    }
    let retained = LIVE.load(Ordering::Relaxed) as usize;
    let count = partitions.values().map(|p| match p {
        AnchoredHashSegment::Dense {count, ..} => *count,
        AnchoredHashSegment::Sparse(b) => b.len() as usize,
    }).sum();
    std::hint::black_box(&partitions);
    drop(partitions);
    (count, retained)
}

fn insert(design: &str, input: &[u64]) -> (usize, usize) {
    match design {
        "segmented-adaptive" => segmented_adaptive(input),
        "segmented-anchored" => segmented_anchored(input),
        "segmented-anchored-hash" => segmented_anchored_hash(input),
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
        "partitioned-append" => {
            let mut set: HashMap<u32, RoaringBitmap> = HashMap::new();
            for &id in input {
                let bitmap = set.entry((id >> 32) as u32).or_default();
                if bitmap.try_push(id as u32).is_err() {
                    assert!(bitmap.insert(id as u32));
                }
            }
            let retained = LIVE.load(Ordering::Relaxed) as usize;
            let count = set.values().map(RoaringBitmap::len).sum::<u64>() as usize;
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
    let layouts = ["dense", "sparse-8", "sparse-200", "sparse-200-shuffled", "spark-8", "spark-200", "spark-2k", "spark-16k", "spark-200-shuffled", "spark-16k-shuffled", "spark-200-high-offset"];
    let designs = ["hash", "treemap", "partitioned-direct", "partitioned-append", "partitioned-sorted", "segmented-adaptive", "segmented-anchored", "segmented-anchored-hash"];
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
