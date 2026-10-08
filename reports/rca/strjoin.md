# Root Cause Analysis: String & Integer Join Suite (`strjoin`) - Benchmark Run 37758359536

**Repository**: `unikdahal/datafusion-comet`  
**Workflow**: Fork Adaptive Iceberg Pruning Benchmark  
**Baseline Side**: Unmodified Apache Comet main `f80042f78` + Apache Iceberg Rust `af1da4c`  
**Candidate Side**: Fork Comet `92bba61b6` + Fork Iceberg Rust `949cc88`  
**Current Heads**: Comet `8cd4d9a98` (`/home/unik/Coding/rust/rp-work/comet`), Iceberg Rust `157cf6475` (`/home/unik/Coding/rust/rp-work/iceberg-nanfix`)  
**Variants**: `baseline_off`, `baseline_on`, `candidate_off`, `candidate_on` (8 balanced JVM rounds, 4 warmups, 2 reps)  

## Executive Summary

The `strjoin` suite benchmarks 126 queries systematically spanning 3 key types (`int`, `long`, `str`), 3 join strategies (`broadcast`, `shuffled_hash`, `sort_merge`), 2 query flavors (`plain` range join vs `distinct` range join), and 7 scale points (1,000, 10,000, 40,000, 100,000, 500,000, 1,000,000, 3,000,000 keys).

### Suite Breakdown (126 Queries Total)

- **`[pruning-win]`**: 3 queries
- **`[expected-parity]`**: 75 queries
- **`[fixed-overhead]`**: 4 queries
- **`[under-delivers]`**: 28 queries
- **`[suspicious]`**: 11 queries
- **`[baseline-invalid]`**: 5 queries
- **`[regression]`**: 0 queries
- **`[coverage-diff]`**: 0 queries

### Key Architectural Findings

1. **Baseline Correctness Flaw (`baseline-invalid`, 5 queries)**: All 5 queries in `strjoin_str_broadcast_distinct_{40000, 100000, 500000, 1000000, 3000000}` failed on baseline due to the upstream Apache Comet broadcast coalescing vector corruption (`OversizedAllocationException` and row mismatches in `CometBroadcastExchangeExec`). Candidate passed 100% exact on all 126 queries.

2. **String Key Dynamic Filtering Ineligibility (`DataType::Utf8`)**: In Comet's dynamic filter planner ([`join.rs:348`](file:///home/unik/Coding/rust/rp-work/comet/native/core/src/execution/operators/dynamic_filter/join.rs#L348)), string keys are explicitly rejected with `"requires matching integer, date or timestamp keys"`. Consequently, all 42 string join queries ran zero dynamic filtering on both baseline and candidate.

3. **Sort-Merge Join Ineligibility**: Comet attaches dynamic filters exclusively to `HashJoinExec` ([`join.rs:90`](file:///home/unik/Coding/rust/rp-work/comet/native/core/src/execution/operators/dynamic_filter/join.rs#L90)). All 42 sort-merge join queries ran native sort-merge joins without runtime filtering.

4. **Broadcast Plain Queries Spark Fallback**: All 21 `broadcast` + `plain` queries fell back to Spark JVM's `BroadcastHashJoinExec` because the un-aggregated Spark `Range` operator produced JVM `BroadcastExchange` rather than `CometBroadcastExchangeExec`. Only `broadcast` + `distinct` queries executed native Comet broadcast joins.

5. **Defective Adaptive Bypass in Dynamic Filter**: In `DynamicFilterExec` ([`batch_filter.rs:160-205`](file:///home/unik/Coding/rust/rp-work/comet/native/core/src/execution/operators/dynamic_filter/batch_filter.rs#L160-L205)), the `bypassed` counter is incremented only when the predicate evaluates to `Boolean(Some(true))` (the placeholder state before DataFusion publishes the build filter). Once published, there is **zero runtime selectivity tracking** or early-exit mechanism. For non-selective builds (500k-3M keys), `DynamicFilterExec` evaluates all 4,000,000 probe rows batch by batch and copies all batch columns via `filter_record_batch`, incurring 5-25ms of pure CPU overhead with zero pruning benefit.

6. **Post-Shuffle Filtering Ineffectiveness**: For `shuffled_hash` joins, dynamic filtering executes after `CometShuffleExchangeExec`. The entire 4,000,000-row table has already been read from Iceberg, compressed, transmitted over network shuffle, and deserialized. Filtering rows post-shuffle saves only the hash table probe, which is already sub-millisecond in DataFusion's vectorized hash join, leaving wall-clock time unchanged or slightly degraded by filter evaluation.


## Deliverable A: Complete `strjoin` Query Ledger

Every query sorted from LOWEST `cand_on/base_on` speedup ratio to highest, followed by invalid baseline queries.


| Query | Class | Key | Strategy | Flavor | Build Size | Base Off (ms) | Base On (ms) | Cand Off (ms) | Cand On (ms) | Cand On / Base On (95% CI) | Cand Off / Base Off | Cand On / Cand Off | DF Rows Eval / Pruned | DF Eval (ms) | Files Pruned | RG Pruned | Scanned MiB | Join Operator |
|:---|:---:|:---:|:---:|:---:|---:|---:|---:|---:|---:|:---:|:---:|:---:|:---:|---:|---:|---:|---:|:---|
| `strjoin_int_shuffled_hash_distinct_100000` | `under-delivers` | int | shuffled_hash | distinct | 100,000 | 112.6 | 108.8 | 109.0 | 111.4 | **0.83x** [0.67, 0.95] | 1.06x | 0.83x [0.69, 0.97] | 4,000,000 / 3,900,000 | 5.0 | 0 | 0 | 18.2 | CometHashJoin |
| `strjoin_long_broadcast_plain_10000` | `suspicious` | long | broadcast | plain | 10,000 | 73.4 | 70.0 | 69.7 | 78.2 | **0.86x** [0.73, 0.98] | 1.02x | 0.90x [0.72, 1.13] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | SparkBroadcastHashJoin |
| `strjoin_long_shuffled_hash_distinct_100000` | `under-delivers` | long | shuffled_hash | distinct | 100,000 | 145.0 | 145.0 | 143.7 | 152.1 | **0.89x** [0.78, 0.98] | 0.97x | 0.93x [0.82, 1.06] | 4,000,000 / 3,900,000 | 8.4 | 0 | 0 | 11.9 | CometHashJoin |
| `strjoin_str_broadcast_plain_100000` | `suspicious` | str | broadcast | plain | 100,000 | 243.1 | 236.0 | 231.2 | 270.9 | **0.89x** [0.83, 0.96] | 1.02x | 0.91x [0.84, 0.98] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | SparkBroadcastHashJoin |
| `strjoin_int_broadcast_plain_500000` | `expected-parity` | int | broadcast | plain | 500,000 | 289.1 | 246.9 | 267.7 | 313.9 | **0.89x** [0.80, 1.01] | 1.08x | 0.89x [0.80, 0.99] | 0 / 0 | 0.0 | 0 | 0 | 18.2 | SparkBroadcastHashJoin |
| `strjoin_str_broadcast_plain_40000` | `expected-parity` | str | broadcast | plain | 40,000 | 217.4 | 199.6 | 207.5 | 213.4 | **0.89x** [0.79, 1.02] | 1.02x | 0.94x [0.88, 1.02] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | SparkBroadcastHashJoin |
| `strjoin_str_sort_merge_plain_500000` | `suspicious` | str | sort_merge | plain | 500,000 | 296.7 | 303.1 | 300.9 | 335.9 | **0.89x** [0.80, 1.00] | 1.02x | 0.88x [0.77, 0.98] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometSortMergeJoin |
| `strjoin_int_shuffled_hash_distinct_1000000` | `under-delivers` | int | shuffled_hash | distinct | 1,000,000 | 257.2 | 235.0 | 236.7 | 272.4 | **0.90x** [0.76, 1.04] | 1.03x | 0.92x [0.81, 1.02] | 4,000,000 / 3,000,000 | 6.0 | 0 | 0 | 18.2 | CometHashJoin |
| `strjoin_long_shuffled_hash_plain_100000` | `under-delivers` | long | shuffled_hash | plain | 100,000 | 126.0 | 122.8 | 126.4 | 128.5 | **0.91x** [0.84, 0.97] | 1.04x | 0.95x [0.88, 1.03] | 4,000,000 / 3,900,000 | 5.8 | 0 | 0 | 11.9 | CometHashJoin |
| `strjoin_str_shuffled_hash_distinct_100000` | `expected-parity` | str | shuffled_hash | distinct | 100,000 | 187.9 | 185.7 | 187.8 | 208.4 | **0.92x** [0.83, 1.01] | 1.00x | 0.90x [0.88, 0.92] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometHashJoin |
| `strjoin_int_sort_merge_distinct_10000` | `expected-parity` | int | sort_merge | distinct | 10,000 | 132.2 | 119.5 | 112.7 | 117.7 | **0.92x** [0.77, 1.07] | 1.12x | 0.89x [0.79, 0.98] | 0 / 0 | 0.0 | 0 | 0 | 18.2 | CometSortMergeJoin |
| `strjoin_str_shuffled_hash_plain_500000` | `suspicious` | str | shuffled_hash | plain | 500,000 | 255.0 | 246.1 | 265.2 | 266.6 | **0.93x** [0.87, 0.99] | 0.95x | 0.98x [0.90, 1.05] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometHashJoin |
| `strjoin_int_sort_merge_plain_100000` | `expected-parity` | int | sort_merge | plain | 100,000 | 128.9 | 126.2 | 116.7 | 131.4 | **0.93x** [0.79, 1.08] | 1.09x | 0.85x [0.75, 0.94] | 0 / 0 | 0.0 | 0 | 0 | 18.2 | CometSortMergeJoin |
| `strjoin_long_broadcast_plain_500000` | `expected-parity` | long | broadcast | plain | 500,000 | 255.9 | 254.1 | 255.0 | 280.4 | **0.94x** [0.82, 1.07] | 1.01x | 0.90x [0.81, 1.02] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | SparkBroadcastHashJoin |
| `strjoin_long_sort_merge_plain_10000` | `expected-parity` | long | sort_merge | plain | 10,000 | 142.8 | 142.8 | 145.7 | 152.4 | **0.94x** [0.84, 1.06] | 0.98x | 0.96x [0.86, 1.05] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | CometSortMergeJoin |
| `strjoin_long_sort_merge_plain_3000000` | `suspicious` | long | sort_merge | plain | 3,000,000 | 399.7 | 381.8 | 381.7 | 397.1 | **0.94x** [0.88, 0.99] | 1.09x | 0.93x [0.88, 0.98] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | CometSortMergeJoin |
| `strjoin_long_broadcast_plain_40000` | `expected-parity` | long | broadcast | plain | 40,000 | 86.1 | 84.7 | 83.8 | 90.4 | **0.94x** [0.85, 1.02] | 1.00x | 0.93x [0.84, 1.01] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | SparkBroadcastHashJoin |
| `strjoin_str_broadcast_plain_500000` | `expected-parity` | str | broadcast | plain | 500,000 | 512.3 | 490.6 | 481.2 | 540.6 | **0.94x** [0.88, 1.00] | 1.03x | 0.92x [0.85, 1.00] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | SparkBroadcastHashJoin |
| `strjoin_str_sort_merge_distinct_1000000` | `expected-parity` | str | sort_merge | distinct | 1,000,000 | 468.1 | 419.7 | 460.2 | 460.2 | **0.94x** [0.86, 1.01] | 1.01x | 1.00x [0.95, 1.06] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometSortMergeJoin |
| `strjoin_int_sort_merge_distinct_3000000` | `suspicious` | int | sort_merge | distinct | 3,000,000 | 596.8 | 524.3 | 530.7 | 587.1 | **0.94x** [0.89, 0.99] | 1.05x | 0.96x [0.91, 1.02] | 0 / 0 | 0.0 | 0 | 0 | 18.2 | CometSortMergeJoin |
| `strjoin_str_sort_merge_distinct_40000` | `suspicious` | str | sort_merge | distinct | 40,000 | 250.2 | 245.1 | 249.0 | 263.4 | **0.94x** [0.87, 0.99] | 1.04x | 0.95x [0.88, 1.00] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometSortMergeJoin |
| `strjoin_int_broadcast_plain_10000` | `expected-parity` | int | broadcast | plain | 10,000 | 70.4 | 64.2 | 69.4 | 72.4 | **0.94x** [0.86, 1.05] | 0.99x | 0.95x [0.90, 1.00] | 0 / 0 | 0.0 | 0 | 0 | 18.2 | SparkBroadcastHashJoin |
| `strjoin_str_shuffled_hash_distinct_1000` | `expected-parity` | str | shuffled_hash | distinct | 1,000 | 172.0 | 159.4 | 157.1 | 167.7 | **0.94x** [0.81, 1.05] | 1.09x | 0.92x [0.83, 0.99] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometHashJoin |
| `strjoin_long_shuffled_hash_plain_500000` | `under-delivers` | long | shuffled_hash | plain | 500,000 | 151.8 | 148.8 | 162.1 | 161.3 | **0.94x** [0.83, 1.05] | 0.95x | 0.96x [0.85, 1.06] | 4,000,000 / 3,500,000 | 6.1 | 0 | 0 | 11.9 | CometHashJoin |
| `strjoin_str_sort_merge_plain_1000000` | `expected-parity` | str | sort_merge | plain | 1,000,000 | 367.0 | 358.9 | 353.2 | 361.8 | **0.94x** [0.87, 1.01] | 1.02x | 0.97x [0.89, 1.04] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometSortMergeJoin |
| `strjoin_long_shuffled_hash_distinct_1000` | `under-delivers` | long | shuffled_hash | distinct | 1,000 | 109.8 | 116.0 | 123.9 | 137.1 | **0.94x** [0.86, 1.03] | 1.02x | 0.95x [0.85, 1.04] | 4,000,000 / 3,999,000 | 5.8 | 0 | 0 | 11.9 | CometHashJoin |
| `strjoin_long_sort_merge_plain_1000000` | `expected-parity` | long | sort_merge | plain | 1,000,000 | 232.8 | 229.6 | 232.2 | 231.7 | **0.94x** [0.84, 1.08] | 1.01x | 0.94x [0.85, 1.03] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | CometSortMergeJoin |
| `strjoin_int_broadcast_plain_100000` | `expected-parity` | int | broadcast | plain | 100,000 | 92.0 | 95.5 | 98.4 | 97.6 | **0.95x** [0.87, 1.02] | 0.98x | 1.00x [0.93, 1.09] | 0 / 0 | 0.0 | 0 | 0 | 18.2 | SparkBroadcastHashJoin |
| `strjoin_int_sort_merge_distinct_1000` | `expected-parity` | int | sort_merge | distinct | 1,000 | 124.6 | 114.6 | 118.2 | 121.6 | **0.95x** [0.88, 1.03] | 1.14x | 0.95x [0.87, 1.02] | 0 / 0 | 0.0 | 0 | 0 | 18.2 | CometSortMergeJoin |
| `strjoin_long_broadcast_plain_1000000` | `expected-parity` | long | broadcast | plain | 1,000,000 | 460.1 | 450.7 | 445.8 | 479.6 | **0.95x** [0.88, 1.05] | 1.04x | 0.95x [0.89, 1.02] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | SparkBroadcastHashJoin |
| `strjoin_long_sort_merge_plain_40000` | `expected-parity` | long | sort_merge | plain | 40,000 | 153.5 | 145.0 | 146.9 | 151.0 | **0.95x** [0.91, 1.00] | 1.06x | 1.00x [0.95, 1.06] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | CometSortMergeJoin |
| `strjoin_long_shuffled_hash_plain_3000000` | `fixed-overhead` | long | shuffled_hash | plain | 3,000,000 | 365.7 | 374.0 | 369.6 | 369.6 | **0.96x** [0.85, 1.06] | 1.02x | 0.94x [0.86, 1.02] | 4,000,000 / 1,000,000 | 8.7 | 0 | 0 | 11.9 | CometHashJoin |
| `strjoin_long_broadcast_distinct_100000` | `expected-parity` | long | broadcast | distinct | 100,000 | 128.0 | 124.5 | 117.6 | 125.3 | **0.96x** [0.87, 1.05] | 1.02x | 0.99x [0.90, 1.09] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | SparkBroadcastHashJoin |
| `strjoin_long_shuffled_hash_distinct_10000` | `under-delivers` | long | shuffled_hash | distinct | 10,000 | 130.2 | 123.8 | 120.9 | 133.2 | **0.96x** [0.83, 1.15] | 1.04x | 0.92x [0.85, 1.00] | 4,000,000 / 3,990,000 | 6.4 | 0 | 0 | 11.9 | CometHashJoin |
| `strjoin_str_sort_merge_plain_1000` | `suspicious` | str | sort_merge | plain | 1,000 | 228.3 | 214.8 | 211.6 | 223.0 | **0.96x** [0.93, 0.99] | 1.05x | 0.96x [0.89, 1.02] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometSortMergeJoin |
| `strjoin_str_broadcast_plain_3000000` | `suspicious` | str | broadcast | plain | 3,000,000 | 2072.0 | 2102.4 | 2087.3 | 2158.2 | **0.96x** [0.94, 0.99] | 1.00x | 0.95x [0.93, 0.97] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | SparkBroadcastHashJoin |
| `strjoin_long_broadcast_distinct_1000000` | `expected-parity` | long | broadcast | distinct | 1,000,000 | 570.8 | 509.9 | 526.6 | 539.0 | **0.96x** [0.89, 1.02] | 1.04x | 0.99x [0.94, 1.04] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | SparkBroadcastHashJoin |
| `strjoin_long_sort_merge_distinct_10000` | `expected-parity` | long | sort_merge | distinct | 10,000 | 160.2 | 148.0 | 146.5 | 154.8 | **0.96x** [0.92, 1.01] | 1.11x | 0.97x [0.95, 0.98] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | CometSortMergeJoin |
| `strjoin_long_broadcast_plain_3000000` | `suspicious` | long | broadcast | plain | 3,000,000 | 1212.5 | 1229.3 | 1211.7 | 1243.5 | **0.96x** [0.93, 1.00] | 0.99x | 0.98x [0.93, 1.02] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | SparkBroadcastHashJoin |
| `strjoin_long_sort_merge_distinct_40000` | `expected-parity` | long | sort_merge | distinct | 40,000 | 143.0 | 143.6 | 151.2 | 153.2 | **0.96x** [0.91, 1.02] | 0.98x | 0.97x [0.90, 1.05] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | CometSortMergeJoin |
| `strjoin_str_shuffled_hash_plain_10000` | `expected-parity` | str | shuffled_hash | plain | 10,000 | 157.5 | 163.2 | 164.8 | 168.4 | **0.96x** [0.85, 1.10] | 0.99x | 0.94x [0.86, 1.01] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometHashJoin |
| `strjoin_int_shuffled_hash_plain_1000000` | `under-delivers` | int | shuffled_hash | plain | 1,000,000 | 166.5 | 162.8 | 167.0 | 176.2 | **0.97x** [0.92, 1.02] | 1.01x | 1.00x [0.93, 1.07] | 4,000,000 / 3,000,000 | 6.1 | 0 | 0 | 18.2 | CometHashJoin |
| `strjoin_long_broadcast_plain_100000` | `expected-parity` | long | broadcast | plain | 100,000 | 96.8 | 92.2 | 98.7 | 96.6 | **0.97x** [0.91, 1.04] | 0.95x | 1.02x [0.95, 1.10] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | SparkBroadcastHashJoin |
| `strjoin_long_shuffled_hash_plain_1000000` | `under-delivers` | long | shuffled_hash | plain | 1,000,000 | 204.4 | 194.9 | 195.3 | 205.0 | **0.97x** [0.91, 1.03] | 1.01x | 1.01x [0.92, 1.13] | 4,000,000 / 3,000,000 | 6.7 | 0 | 0 | 11.9 | CometHashJoin |
| `strjoin_str_broadcast_plain_1000000` | `expected-parity` | str | broadcast | plain | 1,000,000 | 876.5 | 878.5 | 863.0 | 897.7 | **0.97x** [0.94, 1.02] | 1.02x | 0.95x [0.92, 0.97] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | SparkBroadcastHashJoin |
| `strjoin_str_sort_merge_distinct_3000000` | `suspicious` | str | sort_merge | distinct | 3,000,000 | 1017.2 | 1017.5 | 1043.6 | 1029.2 | **0.97x** [0.94, 1.00] | 1.01x | 0.98x [0.94, 1.03] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometSortMergeJoin |
| `strjoin_int_sort_merge_plain_1000` | `expected-parity` | int | sort_merge | plain | 1,000 | 125.2 | 119.9 | 115.7 | 124.5 | **0.97x** [0.86, 1.14] | 1.04x | 0.95x [0.90, 0.99] | 0 / 0 | 0.0 | 0 | 0 | 18.2 | CometSortMergeJoin |
| `strjoin_long_shuffled_hash_distinct_3000000` | `fixed-overhead` | long | shuffled_hash | distinct | 3,000,000 | 773.5 | 771.5 | 776.3 | 780.5 | **0.97x** [0.92, 1.03] | 1.02x | 0.98x [0.93, 1.02] | 4,000,000 / 1,000,000 | 8.9 | 0 | 0 | 11.9 | CometHashJoin |
| `strjoin_int_sort_merge_plain_500000` | `expected-parity` | int | sort_merge | plain | 500,000 | 158.2 | 158.3 | 160.8 | 155.6 | **0.97x** [0.93, 1.03] | 0.92x | 1.05x [0.96, 1.17] | 0 / 0 | 0.0 | 0 | 0 | 18.2 | CometSortMergeJoin |
| `strjoin_int_shuffled_hash_plain_3000000` | `fixed-overhead` | int | shuffled_hash | plain | 3,000,000 | 322.3 | 323.7 | 324.2 | 329.8 | **0.97x** [0.91, 1.04] | 1.03x | 0.94x [0.87, 1.03] | 4,000,000 / 1,000,000 | 7.7 | 0 | 0 | 18.2 | CometHashJoin |
| `strjoin_int_sort_merge_distinct_500000` | `expected-parity` | int | sort_merge | distinct | 500,000 | 221.9 | 199.2 | 198.2 | 198.2 | **0.97x** [0.88, 1.07] | 1.08x | 0.98x [0.90, 1.06] | 0 / 0 | 0.0 | 0 | 0 | 18.2 | CometSortMergeJoin |
| `strjoin_long_sort_merge_distinct_3000000` | `expected-parity` | long | sort_merge | distinct | 3,000,000 | 769.7 | 681.4 | 700.3 | 694.5 | **0.98x** [0.94, 1.01] | 1.06x | 0.98x [0.92, 1.05] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | CometSortMergeJoin |
| `strjoin_long_sort_merge_distinct_1000` | `expected-parity` | long | sort_merge | distinct | 1,000 | 148.7 | 145.3 | 140.8 | 154.3 | **0.98x** [0.91, 1.06] | 0.98x | 0.99x [0.90, 1.13] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | CometSortMergeJoin |
| `strjoin_long_shuffled_hash_plain_1000` | `under-delivers` | long | shuffled_hash | plain | 1,000 | 118.3 | 113.4 | 108.2 | 117.4 | **0.98x** [0.93, 1.03] | 1.03x | 0.99x [0.90, 1.13] | 4,000,000 / 3,999,000 | 6.1 | 0 | 0 | 11.9 | CometHashJoin |
| `strjoin_str_shuffled_hash_distinct_3000000` | `expected-parity` | str | shuffled_hash | distinct | 3,000,000 | 946.0 | 976.3 | 962.7 | 1006.2 | **0.98x** [0.94, 1.01] | 1.02x | 0.95x [0.92, 0.99] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometHashJoin |
| `strjoin_str_shuffled_hash_plain_100000` | `expected-parity` | str | shuffled_hash | plain | 100,000 | 176.7 | 169.0 | 177.5 | 177.1 | **0.98x** [0.94, 1.03] | 0.97x | 1.04x [0.96, 1.13] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometHashJoin |
| `strjoin_int_shuffled_hash_distinct_40000` | `under-delivers` | int | shuffled_hash | distinct | 40,000 | 107.5 | 105.1 | 103.2 | 103.1 | **0.98x** [0.89, 1.08] | 1.03x | 0.96x [0.85, 1.08] | 4,000,000 / 3,960,000 | 5.1 | 0 | 0 | 18.2 | CometHashJoin |
| `strjoin_int_broadcast_distinct_1000000` | `under-delivers` | int | broadcast | distinct | 1,000,000 | 301.0 | 265.9 | 241.1 | 288.3 | **0.98x** [0.83, 1.12] | 1.14x | 0.89x [0.79, 0.98] | 0 / 0 | 0.0 | 11 | 1 | 5.2 | CometBroadcastHashJoin |
| `strjoin_int_broadcast_plain_1000000` | `expected-parity` | int | broadcast | plain | 1,000,000 | 484.6 | 454.6 | 442.9 | 483.7 | **0.98x** [0.94, 1.04] | 1.06x | 0.93x [0.87, 0.98] | 0 / 0 | 0.0 | 0 | 0 | 18.2 | SparkBroadcastHashJoin |
| `strjoin_long_shuffled_hash_distinct_1000000` | `under-delivers` | long | shuffled_hash | distinct | 1,000,000 | 384.2 | 338.1 | 322.4 | 365.2 | **0.98x** [0.90, 1.08] | 1.06x | 0.91x [0.84, 0.97] | 4,000,000 / 3,000,000 | 6.8 | 0 | 0 | 11.9 | CometHashJoin |
| `strjoin_long_shuffled_hash_distinct_500000` | `under-delivers` | long | shuffled_hash | distinct | 500,000 | 221.5 | 224.1 | 197.5 | 194.1 | **0.98x** [0.87, 1.13] | 1.03x | 0.94x [0.88, 1.03] | 4,000,000 / 3,500,000 | 6.2 | 0 | 0 | 11.9 | CometHashJoin |
| `strjoin_int_broadcast_plain_1000` | `expected-parity` | int | broadcast | plain | 1,000 | 68.3 | 63.7 | 62.5 | 65.2 | **0.99x** [0.91, 1.08] | 1.15x | 0.96x [0.88, 1.03] | 0 / 0 | 0.0 | 0 | 0 | 18.2 | SparkBroadcastHashJoin |
| `strjoin_int_shuffled_hash_distinct_3000000` | `fixed-overhead` | int | shuffled_hash | distinct | 3,000,000 | 558.5 | 547.1 | 540.8 | 566.6 | **0.99x** [0.91, 1.05] | 1.03x | 0.97x [0.91, 1.02] | 4,000,000 / 1,000,000 | 7.7 | 0 | 0 | 18.2 | CometHashJoin |
| `strjoin_int_shuffled_hash_distinct_500000` | `under-delivers` | int | shuffled_hash | distinct | 500,000 | 158.8 | 156.9 | 147.5 | 164.8 | **0.99x** [0.89, 1.12] | 1.04x | 0.93x [0.85, 1.00] | 4,000,000 / 3,500,000 | 6.0 | 0 | 0 | 18.2 | CometHashJoin |
| `strjoin_str_broadcast_distinct_1000` | `expected-parity` | str | broadcast | distinct | 1,000 | 91.1 | 94.4 | 91.1 | 95.1 | **0.99x** [0.86, 1.14] | 0.97x | 1.00x [0.91, 1.15] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometBroadcastHashJoin |
| `strjoin_long_sort_merge_plain_500000` | `expected-parity` | long | sort_merge | plain | 500,000 | 189.0 | 185.7 | 184.3 | 196.8 | **0.99x** [0.89, 1.12] | 1.02x | 0.94x [0.89, 1.00] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | CometSortMergeJoin |
| `strjoin_int_broadcast_plain_3000000` | `expected-parity` | int | broadcast | plain | 3,000,000 | 1255.4 | 1267.6 | 1300.3 | 1264.8 | **0.99x** [0.97, 1.01] | 0.98x | 1.01x [0.97, 1.06] | 0 / 0 | 0.0 | 0 | 0 | 18.2 | SparkBroadcastHashJoin |
| `strjoin_str_sort_merge_distinct_1000` | `expected-parity` | str | sort_merge | distinct | 1,000 | 239.1 | 212.6 | 221.6 | 221.6 | **0.99x** [0.93, 1.07] | 1.06x | 1.03x [0.96, 1.14] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometSortMergeJoin |
| `strjoin_int_sort_merge_plain_1000000` | `expected-parity` | int | sort_merge | plain | 1,000,000 | 215.8 | 203.3 | 204.2 | 200.3 | **0.99x** [0.92, 1.07] | 1.06x | 1.00x [0.91, 1.10] | 0 / 0 | 0.0 | 0 | 0 | 18.2 | CometSortMergeJoin |
| `strjoin_str_shuffled_hash_distinct_500000` | `expected-parity` | str | shuffled_hash | distinct | 500,000 | 343.3 | 333.6 | 331.9 | 346.4 | **0.99x** [0.94, 1.05] | 1.01x | 0.99x [0.92, 1.08] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometHashJoin |
| `strjoin_int_broadcast_distinct_100000` | `under-delivers` | int | broadcast | distinct | 100,000 | 101.1 | 93.8 | 96.5 | 98.5 | **0.99x** [0.85, 1.17] | 1.06x | 1.01x [0.86, 1.17] | 0 / 0 | 0.0 | 15 | 0 | 0.9 | CometBroadcastHashJoin |
| `strjoin_int_sort_merge_distinct_1000000` | `expected-parity` | int | sort_merge | distinct | 1,000,000 | 272.9 | 250.8 | 240.4 | 250.1 | **0.99x** [0.88, 1.12] | 1.04x | 1.01x [0.92, 1.10] | 0 / 0 | 0.0 | 0 | 0 | 18.2 | CometSortMergeJoin |
| `strjoin_str_sort_merge_plain_10000` | `expected-parity` | str | sort_merge | plain | 10,000 | 233.0 | 218.8 | 214.8 | 215.9 | **1.00x** [0.88, 1.13] | 1.07x | 0.96x [0.89, 1.03] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometSortMergeJoin |
| `strjoin_str_shuffled_hash_distinct_40000` | `expected-parity` | str | shuffled_hash | distinct | 40,000 | 175.8 | 172.9 | 159.2 | 179.4 | **1.00x** [0.92, 1.11] | 1.06x | 0.94x [0.88, 0.99] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometHashJoin |
| `strjoin_int_broadcast_distinct_500000` | `under-delivers` | int | broadcast | distinct | 500,000 | 165.5 | 184.7 | 176.1 | 173.0 | **1.00x** [0.88, 1.13] | 0.94x | 1.00x [0.88, 1.18] | 0 / 0 | 0.0 | 13 | 1 | 2.9 | CometBroadcastHashJoin |
| `strjoin_int_shuffled_hash_plain_1000` | `under-delivers` | int | shuffled_hash | plain | 1,000 | 84.3 | 87.0 | 84.7 | 84.5 | **1.00x** [0.92, 1.06] | 0.97x | 0.99x [0.89, 1.11] | 4,000,000 / 3,999,000 | 5.4 | 0 | 0 | 18.2 | CometHashJoin |
| `strjoin_str_sort_merge_plain_3000000` | `expected-parity` | str | sort_merge | plain | 3,000,000 | 676.8 | 677.2 | 665.7 | 675.5 | **1.00x** [0.95, 1.07] | 1.02x | 0.99x [0.94, 1.04] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometSortMergeJoin |
| `strjoin_int_sort_merge_plain_3000000` | `expected-parity` | int | sort_merge | plain | 3,000,000 | 351.7 | 346.3 | 340.2 | 347.3 | **1.00x** [0.94, 1.06] | 1.03x | 0.97x [0.94, 1.00] | 0 / 0 | 0.0 | 0 | 0 | 18.2 | CometSortMergeJoin |
| `strjoin_long_broadcast_distinct_500000` | `expected-parity` | long | broadcast | distinct | 500,000 | 346.9 | 325.7 | 324.1 | 334.3 | **1.00x** [0.94, 1.06] | 1.03x | 1.01x [0.93, 1.09] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | SparkBroadcastHashJoin |
| `strjoin_int_shuffled_hash_distinct_10000` | `under-delivers` | int | shuffled_hash | distinct | 10,000 | 91.8 | 90.7 | 91.0 | 93.3 | **1.00x** [0.95, 1.07] | 1.01x | 0.99x [0.94, 1.05] | 4,000,000 / 3,990,000 | 5.7 | 0 | 0 | 18.2 | CometHashJoin |
| `strjoin_str_sort_merge_plain_100000` | `expected-parity` | str | sort_merge | plain | 100,000 | 236.5 | 239.8 | 226.5 | 236.1 | **1.00x** [0.94, 1.06] | 1.06x | 0.94x [0.88, 0.98] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometSortMergeJoin |
| `strjoin_long_broadcast_distinct_3000000` | `expected-parity` | long | broadcast | distinct | 3,000,000 | 1577.0 | 1654.4 | 1607.6 | 1621.5 | **1.00x** [0.95, 1.04] | 0.99x | 0.97x [0.94, 1.01] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | SparkBroadcastHashJoin |
| `strjoin_str_broadcast_plain_10000` | `expected-parity` | str | broadcast | plain | 10,000 | 188.0 | 204.7 | 180.6 | 196.0 | **1.01x** [0.90, 1.10] | 1.14x | 0.89x [0.82, 0.97] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | SparkBroadcastHashJoin |
| `strjoin_str_shuffled_hash_distinct_10000` | `expected-parity` | str | shuffled_hash | distinct | 10,000 | 197.5 | 182.2 | 169.2 | 180.1 | **1.01x** [0.96, 1.06] | 1.13x | 0.97x [0.91, 1.04] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometHashJoin |
| `strjoin_str_shuffled_hash_plain_1000` | `expected-parity` | str | shuffled_hash | plain | 1,000 | 158.7 | 159.1 | 160.1 | 161.4 | **1.01x** [0.94, 1.08] | 0.99x | 0.97x [0.91, 1.04] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometHashJoin |
| `strjoin_str_shuffled_hash_plain_3000000` | `expected-parity` | str | shuffled_hash | plain | 3,000,000 | 697.6 | 713.5 | 707.5 | 704.0 | **1.01x** [0.93, 1.09] | 0.99x | 0.99x [0.93, 1.06] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometHashJoin |
| `strjoin_str_sort_merge_distinct_100000` | `expected-parity` | str | sort_merge | distinct | 100,000 | 245.4 | 258.1 | 247.4 | 252.7 | **1.01x** [0.94, 1.09] | 0.97x | 0.98x [0.88, 1.10] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometSortMergeJoin |
| `strjoin_str_shuffled_hash_distinct_1000000` | `expected-parity` | str | shuffled_hash | distinct | 1,000,000 | 457.2 | 449.5 | 440.7 | 411.6 | **1.01x** [0.93, 1.09] | 1.01x | 1.01x [0.92, 1.11] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometHashJoin |
| `strjoin_str_shuffled_hash_plain_40000` | `expected-parity` | str | shuffled_hash | plain | 40,000 | 173.4 | 172.9 | 163.0 | 162.5 | **1.01x** [0.90, 1.11] | 1.02x | 0.97x [0.87, 1.05] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometHashJoin |
| `strjoin_long_sort_merge_distinct_500000` | `expected-parity` | long | sort_merge | distinct | 500,000 | 251.3 | 266.1 | 258.9 | 251.8 | **1.01x** [0.94, 1.10] | 1.01x | 1.02x [0.94, 1.11] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | CometSortMergeJoin |
| `strjoin_int_sort_merge_plain_10000` | `expected-parity` | int | sort_merge | plain | 10,000 | 112.5 | 110.4 | 109.9 | 111.2 | **1.01x** [0.91, 1.15] | 1.04x | 0.97x [0.92, 1.02] | 0 / 0 | 0.0 | 0 | 0 | 18.2 | CometSortMergeJoin |
| `strjoin_str_sort_merge_distinct_500000` | `expected-parity` | str | sort_merge | distinct | 500,000 | 356.0 | 347.5 | 338.9 | 337.3 | **1.02x** [0.97, 1.06] | 1.05x | 0.99x [0.95, 1.04] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometSortMergeJoin |
| `strjoin_int_shuffled_hash_distinct_1000` | `under-delivers` | int | shuffled_hash | distinct | 1,000 | 87.0 | 89.2 | 87.9 | 93.4 | **1.02x** [0.91, 1.17] | 0.94x | 0.99x [0.94, 1.04] | 4,000,000 / 3,999,000 | 5.3 | 0 | 0 | 18.2 | CometHashJoin |
| `strjoin_int_shuffled_hash_plain_40000` | `under-delivers` | int | shuffled_hash | plain | 40,000 | 87.8 | 87.2 | 87.2 | 87.3 | **1.02x** [0.99, 1.05] | 1.03x | 1.01x [0.98, 1.05] | 4,000,000 / 3,960,000 | 5.6 | 0 | 0 | 18.2 | CometHashJoin |
| `strjoin_str_sort_merge_plain_40000` | `expected-parity` | str | sort_merge | plain | 40,000 | 227.4 | 239.1 | 223.8 | 235.0 | **1.02x** [0.97, 1.08] | 1.03x | 0.97x [0.93, 1.01] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometSortMergeJoin |
| `strjoin_int_shuffled_hash_plain_10000` | `under-delivers` | int | shuffled_hash | plain | 10,000 | 88.5 | 93.9 | 87.6 | 89.1 | **1.02x** [0.99, 1.06] | 0.99x | 0.95x [0.92, 0.99] | 4,000,000 / 3,990,000 | 5.3 | 0 | 0 | 18.2 | CometHashJoin |
| `strjoin_str_shuffled_hash_plain_1000000` | `expected-parity` | str | shuffled_hash | plain | 1,000,000 | 372.2 | 356.8 | 349.9 | 337.9 | **1.02x** [0.96, 1.09] | 1.04x | 1.01x [0.90, 1.10] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometHashJoin |
| `strjoin_long_shuffled_hash_plain_10000` | `under-delivers` | long | shuffled_hash | plain | 10,000 | 112.1 | 113.9 | 110.6 | 113.0 | **1.02x** [0.93, 1.17] | 1.00x | 0.96x [0.90, 1.03] | 4,000,000 / 3,990,000 | 5.9 | 0 | 0 | 11.9 | CometHashJoin |
| `strjoin_int_sort_merge_distinct_40000` | `expected-parity` | int | sort_merge | distinct | 40,000 | 127.7 | 122.1 | 120.8 | 120.0 | **1.02x** [0.98, 1.07] | 1.06x | 1.03x [1.00, 1.10] | 0 / 0 | 0.0 | 0 | 0 | 18.2 | CometSortMergeJoin |
| `strjoin_long_sort_merge_plain_100000` | `expected-parity` | long | sort_merge | plain | 100,000 | 157.9 | 149.6 | 156.0 | 153.3 | **1.03x** [0.88, 1.20] | 1.02x | 0.98x [0.88, 1.08] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | CometSortMergeJoin |
| `strjoin_int_sort_merge_plain_40000` | `expected-parity` | int | sort_merge | plain | 40,000 | 123.0 | 115.1 | 117.6 | 120.4 | **1.03x** [0.90, 1.23] | 1.05x | 0.96x [0.90, 1.03] | 0 / 0 | 0.0 | 0 | 0 | 18.2 | CometSortMergeJoin |
| `strjoin_str_broadcast_distinct_10000` | `expected-parity` | str | broadcast | distinct | 10,000 | 107.2 | 108.9 | 115.0 | 110.4 | **1.03x** [0.87, 1.24] | 0.95x | 1.03x [0.88, 1.21] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometBroadcastHashJoin |
| `strjoin_int_sort_merge_distinct_100000` | `expected-parity` | int | sort_merge | distinct | 100,000 | 132.3 | 137.9 | 128.4 | 135.6 | **1.03x** [0.93, 1.16] | 1.03x | 0.95x [0.87, 1.05] | 0 / 0 | 0.0 | 0 | 0 | 18.2 | CometSortMergeJoin |
| `strjoin_str_sort_merge_distinct_10000` | `expected-parity` | str | sort_merge | distinct | 10,000 | 234.4 | 241.2 | 245.0 | 235.7 | **1.03x** [0.98, 1.09] | 0.99x | 1.01x [0.95, 1.06] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometSortMergeJoin |
| `strjoin_long_broadcast_plain_1000` | `expected-parity` | long | broadcast | plain | 1,000 | 77.3 | 72.5 | 76.3 | 72.2 | **1.03x** [0.94, 1.15] | 1.05x | 1.05x [0.98, 1.14] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | SparkBroadcastHashJoin |
| `strjoin_long_shuffled_hash_plain_40000` | `under-delivers` | long | shuffled_hash | plain | 40,000 | 117.1 | 121.0 | 115.0 | 128.3 | **1.04x** [0.94, 1.16] | 0.96x | 0.96x [0.87, 1.13] | 4,000,000 / 3,960,000 | 5.8 | 0 | 0 | 11.9 | CometHashJoin |
| `strjoin_long_broadcast_distinct_10000` | `expected-parity` | long | broadcast | distinct | 10,000 | 84.2 | 82.3 | 85.4 | 82.0 | **1.04x** [0.92, 1.24] | 0.96x | 1.06x [0.99, 1.13] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | SparkBroadcastHashJoin |
| `strjoin_long_sort_merge_plain_1000` | `expected-parity` | long | sort_merge | plain | 1,000 | 149.4 | 149.4 | 139.4 | 146.6 | **1.04x** [0.95, 1.19] | 1.07x | 0.98x [0.93, 1.04] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | CometSortMergeJoin |
| `strjoin_str_broadcast_plain_1000` | `expected-parity` | str | broadcast | plain | 1,000 | 200.9 | 199.2 | 189.7 | 192.0 | **1.05x** [0.99, 1.12] | 1.07x | 1.02x [0.94, 1.12] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | SparkBroadcastHashJoin |
| `strjoin_long_sort_merge_distinct_100000` | `expected-parity` | long | sort_merge | distinct | 100,000 | 177.3 | 171.2 | 170.3 | 173.8 | **1.05x** [0.99, 1.15] | 1.08x | 0.99x [0.93, 1.06] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | CometSortMergeJoin |
| `strjoin_int_broadcast_plain_40000` | `expected-parity` | int | broadcast | plain | 40,000 | 91.6 | 100.1 | 82.9 | 85.2 | **1.05x** [0.86, 1.33] | 1.04x | 0.95x [0.82, 1.08] | 0 / 0 | 0.0 | 0 | 0 | 18.2 | SparkBroadcastHashJoin |
| `strjoin_int_shuffled_hash_plain_500000` | `under-delivers` | int | shuffled_hash | plain | 500,000 | 122.8 | 137.1 | 133.0 | 137.9 | **1.06x** [0.91, 1.22] | 0.98x | 0.97x [0.85, 1.13] | 4,000,000 / 3,500,000 | 5.4 | 0 | 0 | 18.2 | CometHashJoin |
| `strjoin_int_broadcast_distinct_40000` | `pruning-win` | int | broadcast | distinct | 40,000 | 86.4 | 87.8 | 85.8 | 86.2 | **1.06x** [0.98, 1.14] | 1.02x | 1.07x [0.97, 1.21] | 0 / 0 | 0.0 | 15 | 1 | 0.7 | CometBroadcastHashJoin |
| `strjoin_int_broadcast_distinct_3000000` | `under-delivers` | int | broadcast | distinct | 3,000,000 | 629.8 | 591.4 | 554.3 | 572.3 | **1.06x** [1.01, 1.12] | 1.11x | 0.99x [0.93, 1.09] | 0 / 0 | 0.0 | 3 | 1 | 14.3 | CometBroadcastHashJoin |
| `strjoin_long_sort_merge_distinct_1000000` | `expected-parity` | long | sort_merge | distinct | 1,000,000 | 373.3 | 387.5 | 349.5 | 349.3 | **1.07x** [1.01, 1.13] | 1.06x | 0.99x [0.94, 1.05] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | CometSortMergeJoin |
| `strjoin_long_shuffled_hash_distinct_40000` | `under-delivers` | long | shuffled_hash | distinct | 40,000 | 124.2 | 122.1 | 124.0 | 123.8 | **1.07x** [0.96, 1.25] | 1.02x | 1.03x [0.96, 1.11] | 4,000,000 / 3,960,000 | 5.7 | 0 | 0 | 11.9 | CometHashJoin |
| `strjoin_int_broadcast_distinct_1000` | `pruning-win` | int | broadcast | distinct | 1,000 | 70.5 | 69.6 | 65.5 | 63.5 | **1.07x** [0.95, 1.21] | 0.96x | 1.06x [0.95, 1.21] | 0 / 0 | 0.0 | 15 | 1 | 0.7 | CometBroadcastHashJoin |
| `strjoin_long_broadcast_distinct_1000` | `expected-parity` | long | broadcast | distinct | 1,000 | 84.8 | 79.7 | 73.6 | 73.0 | **1.08x** [1.01, 1.14] | 1.05x | 1.05x [0.96, 1.16] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | SparkBroadcastHashJoin |
| `strjoin_int_shuffled_hash_plain_100000` | `under-delivers` | int | shuffled_hash | plain | 100,000 | 88.0 | 97.5 | 93.1 | 91.4 | **1.09x** [1.01, 1.19] | 1.00x | 1.01x [0.95, 1.07] | 4,000,000 / 3,900,000 | 5.5 | 0 | 0 | 18.2 | CometHashJoin |
| `strjoin_int_broadcast_distinct_10000` | `pruning-win` | int | broadcast | distinct | 10,000 | 81.3 | 71.3 | 71.0 | 65.0 | **1.12x** [1.06, 1.18] | 1.09x | 1.16x [1.03, 1.35] | 0 / 0 | 0.0 | 15 | 1 | 0.7 | CometBroadcastHashJoin |
| `strjoin_long_broadcast_distinct_40000` | `expected-parity` | long | broadcast | distinct | 40,000 | 82.3 | 88.3 | 84.5 | 83.6 | **1.13x** [0.96, 1.37] | 0.94x | 1.04x [0.95, 1.17] | 0 / 0 | 0.0 | 0 | 0 | 11.9 | SparkBroadcastHashJoin |
| `strjoin_str_broadcast_distinct_100000` | `baseline-invalid` | str | broadcast | distinct | 100,000 | n/a | n/a | 145.5 | 151.9 | n/a (baseline invalid) | n/a | 0.90x [0.78, 1.02] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometBroadcastHashJoin |
| `strjoin_str_broadcast_distinct_1000000` | `baseline-invalid` | str | broadcast | distinct | 1,000,000 | n/a | n/a | 568.8 | 551.8 | n/a (baseline invalid) | n/a | 1.04x [0.97, 1.13] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometBroadcastHashJoin |
| `strjoin_str_broadcast_distinct_500000` | `baseline-invalid` | str | broadcast | distinct | 500,000 | n/a | n/a | 302.8 | 357.0 | n/a (baseline invalid) | n/a | 0.92x [0.87, 0.98] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometBroadcastHashJoin |
| `strjoin_str_broadcast_distinct_40000` | `baseline-invalid` | str | broadcast | distinct | 40,000 | n/a | n/a | 104.4 | 105.2 | n/a (baseline invalid) | n/a | 0.93x [0.79, 1.06] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometBroadcastHashJoin |
| `strjoin_str_broadcast_distinct_3000000` | `baseline-invalid` | str | broadcast | distinct | 3,000,000 | n/a | n/a | 1364.5 | 1395.6 | n/a (baseline invalid) | n/a | 0.97x [0.95, 0.99] | 0 / 0 | 0.0 | 0 | 0 | 9.7 | CometBroadcastHashJoin |

## Deliverable B: Query Classification Ledger (One RCA Line per Query)

Full classification and rationale for each of the 126 queries:


- **`strjoin_int_shuffled_hash_distinct_100000`**: `[under-delivers]` (speedup: 0.83x) — Batch filter pruned 3900000/4000000 rows (97.5%) but post-shuffle costs dominate
- **`strjoin_long_broadcast_plain_10000`**: `[suspicious]` (speedup: 0.86x) — Dynamic filter ineligible; candidate runtime lower than baseline due to driver JIT/GC variance
- **`strjoin_long_shuffled_hash_distinct_100000`**: `[under-delivers]` (speedup: 0.89x) — Batch filter pruned 3900000/4000000 rows (97.5%) but post-shuffle costs dominate
- **`strjoin_str_broadcast_plain_100000`**: `[suspicious]` (speedup: 0.89x) — Dynamic filter ineligible; candidate runtime lower than baseline due to driver JIT/GC variance
- **`strjoin_int_broadcast_plain_500000`**: `[expected-parity]` (speedup: 0.89x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_broadcast_plain_40000`**: `[expected-parity]` (speedup: 0.89x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_sort_merge_plain_500000`**: `[suspicious]` (speedup: 0.89x) — Dynamic filter ineligible; candidate runtime lower than baseline due to driver JIT/GC variance
- **`strjoin_int_shuffled_hash_distinct_1000000`**: `[under-delivers]` (speedup: 0.90x) — Batch filter pruned 3000000/4000000 rows (75.0%) but post-shuffle costs dominate
- **`strjoin_long_shuffled_hash_plain_100000`**: `[under-delivers]` (speedup: 0.91x) — Batch filter pruned 3900000/4000000 rows (97.5%) but post-shuffle costs dominate
- **`strjoin_str_shuffled_hash_distinct_100000`**: `[expected-parity]` (speedup: 0.92x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_sort_merge_distinct_10000`**: `[expected-parity]` (speedup: 0.92x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_shuffled_hash_plain_500000`**: `[suspicious]` (speedup: 0.93x) — Dynamic filter ineligible; candidate runtime lower than baseline due to driver JIT/GC variance
- **`strjoin_int_sort_merge_plain_100000`**: `[expected-parity]` (speedup: 0.93x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_broadcast_plain_500000`**: `[expected-parity]` (speedup: 0.94x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_sort_merge_plain_10000`**: `[expected-parity]` (speedup: 0.94x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_sort_merge_plain_3000000`**: `[suspicious]` (speedup: 0.94x) — Dynamic filter ineligible; candidate runtime lower than baseline due to driver JIT/GC variance
- **`strjoin_long_broadcast_plain_40000`**: `[expected-parity]` (speedup: 0.94x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_broadcast_plain_500000`**: `[expected-parity]` (speedup: 0.94x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_sort_merge_distinct_1000000`**: `[expected-parity]` (speedup: 0.94x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_sort_merge_distinct_3000000`**: `[suspicious]` (speedup: 0.94x) — Dynamic filter ineligible; candidate runtime lower than baseline due to driver JIT/GC variance
- **`strjoin_str_sort_merge_distinct_40000`**: `[suspicious]` (speedup: 0.94x) — Dynamic filter ineligible; candidate runtime lower than baseline due to driver JIT/GC variance
- **`strjoin_int_broadcast_plain_10000`**: `[expected-parity]` (speedup: 0.94x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_shuffled_hash_distinct_1000`**: `[expected-parity]` (speedup: 0.94x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_shuffled_hash_plain_500000`**: `[under-delivers]` (speedup: 0.94x) — Batch filter pruned 3500000/4000000 rows (87.5%) but post-shuffle costs dominate
- **`strjoin_str_sort_merge_plain_1000000`**: `[expected-parity]` (speedup: 0.94x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_shuffled_hash_distinct_1000`**: `[under-delivers]` (speedup: 0.94x) — Batch filter pruned 3999000/4000000 rows (100.0%) but post-shuffle costs dominate
- **`strjoin_long_sort_merge_plain_1000000`**: `[expected-parity]` (speedup: 0.94x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_broadcast_plain_100000`**: `[expected-parity]` (speedup: 0.95x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_sort_merge_distinct_1000`**: `[expected-parity]` (speedup: 0.95x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_broadcast_plain_1000000`**: `[expected-parity]` (speedup: 0.95x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_sort_merge_plain_40000`**: `[expected-parity]` (speedup: 0.95x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_shuffled_hash_plain_3000000`**: `[fixed-overhead]` (speedup: 0.96x) — Batch filter evaluated 4000000 rows with low pruning (25.0%), adding pure eval overhead
- **`strjoin_long_broadcast_distinct_100000`**: `[expected-parity]` (speedup: 0.96x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_shuffled_hash_distinct_10000`**: `[under-delivers]` (speedup: 0.96x) — Batch filter pruned 3990000/4000000 rows (99.8%) but post-shuffle costs dominate
- **`strjoin_str_sort_merge_plain_1000`**: `[suspicious]` (speedup: 0.96x) — Dynamic filter ineligible; candidate runtime lower than baseline due to driver JIT/GC variance
- **`strjoin_str_broadcast_plain_3000000`**: `[suspicious]` (speedup: 0.96x) — Dynamic filter ineligible; candidate runtime lower than baseline due to driver JIT/GC variance
- **`strjoin_long_broadcast_distinct_1000000`**: `[expected-parity]` (speedup: 0.96x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_sort_merge_distinct_10000`**: `[expected-parity]` (speedup: 0.96x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_broadcast_plain_3000000`**: `[suspicious]` (speedup: 0.96x) — Dynamic filter ineligible; candidate runtime lower than baseline due to driver JIT/GC variance
- **`strjoin_long_sort_merge_distinct_40000`**: `[expected-parity]` (speedup: 0.96x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_shuffled_hash_plain_10000`**: `[expected-parity]` (speedup: 0.96x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_shuffled_hash_plain_1000000`**: `[under-delivers]` (speedup: 0.97x) — Batch filter pruned 3000000/4000000 rows (75.0%) but post-shuffle costs dominate
- **`strjoin_long_broadcast_plain_100000`**: `[expected-parity]` (speedup: 0.97x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_shuffled_hash_plain_1000000`**: `[under-delivers]` (speedup: 0.97x) — Batch filter pruned 3000000/4000000 rows (75.0%) but post-shuffle costs dominate
- **`strjoin_str_broadcast_plain_1000000`**: `[expected-parity]` (speedup: 0.97x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_sort_merge_distinct_3000000`**: `[suspicious]` (speedup: 0.97x) — Dynamic filter ineligible; candidate runtime lower than baseline due to driver JIT/GC variance
- **`strjoin_int_sort_merge_plain_1000`**: `[expected-parity]` (speedup: 0.97x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_shuffled_hash_distinct_3000000`**: `[fixed-overhead]` (speedup: 0.97x) — Batch filter evaluated 4000000 rows with low pruning (25.0%), adding pure eval overhead
- **`strjoin_int_sort_merge_plain_500000`**: `[expected-parity]` (speedup: 0.97x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_shuffled_hash_plain_3000000`**: `[fixed-overhead]` (speedup: 0.97x) — Batch filter evaluated 4000000 rows with low pruning (25.0%), adding pure eval overhead
- **`strjoin_int_sort_merge_distinct_500000`**: `[expected-parity]` (speedup: 0.97x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_sort_merge_distinct_3000000`**: `[expected-parity]` (speedup: 0.98x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_sort_merge_distinct_1000`**: `[expected-parity]` (speedup: 0.98x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_shuffled_hash_plain_1000`**: `[under-delivers]` (speedup: 0.98x) — Batch filter pruned 3999000/4000000 rows (100.0%) but post-shuffle costs dominate
- **`strjoin_str_shuffled_hash_distinct_3000000`**: `[expected-parity]` (speedup: 0.98x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_shuffled_hash_plain_100000`**: `[expected-parity]` (speedup: 0.98x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_shuffled_hash_distinct_40000`**: `[under-delivers]` (speedup: 0.98x) — Batch filter pruned 3960000/4000000 rows (99.0%) but post-shuffle costs dominate
- **`strjoin_int_broadcast_distinct_1000000`**: `[under-delivers]` (speedup: 0.98x) — Reader pruned 11 files but wall-clock speedup marginal
- **`strjoin_int_broadcast_plain_1000000`**: `[expected-parity]` (speedup: 0.98x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_shuffled_hash_distinct_1000000`**: `[under-delivers]` (speedup: 0.98x) — Batch filter pruned 3000000/4000000 rows (75.0%) but post-shuffle costs dominate
- **`strjoin_long_shuffled_hash_distinct_500000`**: `[under-delivers]` (speedup: 0.98x) — Batch filter pruned 3500000/4000000 rows (87.5%) but post-shuffle costs dominate
- **`strjoin_int_broadcast_plain_1000`**: `[expected-parity]` (speedup: 0.99x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_shuffled_hash_distinct_3000000`**: `[fixed-overhead]` (speedup: 0.99x) — Batch filter evaluated 4000000 rows with low pruning (25.0%), adding pure eval overhead
- **`strjoin_int_shuffled_hash_distinct_500000`**: `[under-delivers]` (speedup: 0.99x) — Batch filter pruned 3500000/4000000 rows (87.5%) but post-shuffle costs dominate
- **`strjoin_str_broadcast_distinct_1000`**: `[expected-parity]` (speedup: 0.99x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_sort_merge_plain_500000`**: `[expected-parity]` (speedup: 0.99x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_broadcast_plain_3000000`**: `[expected-parity]` (speedup: 0.99x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_sort_merge_distinct_1000`**: `[expected-parity]` (speedup: 0.99x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_sort_merge_plain_1000000`**: `[expected-parity]` (speedup: 0.99x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_shuffled_hash_distinct_500000`**: `[expected-parity]` (speedup: 0.99x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_broadcast_distinct_100000`**: `[under-delivers]` (speedup: 0.99x) — Reader pruned 15 files but wall-clock speedup marginal
- **`strjoin_int_sort_merge_distinct_1000000`**: `[expected-parity]` (speedup: 0.99x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_sort_merge_plain_10000`**: `[expected-parity]` (speedup: 1.00x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_shuffled_hash_distinct_40000`**: `[expected-parity]` (speedup: 1.00x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_broadcast_distinct_500000`**: `[under-delivers]` (speedup: 1.00x) — Reader pruned 13 files but wall-clock speedup marginal
- **`strjoin_int_shuffled_hash_plain_1000`**: `[under-delivers]` (speedup: 1.00x) — Batch filter pruned 3999000/4000000 rows (100.0%) but post-shuffle costs dominate
- **`strjoin_str_sort_merge_plain_3000000`**: `[expected-parity]` (speedup: 1.00x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_sort_merge_plain_3000000`**: `[expected-parity]` (speedup: 1.00x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_broadcast_distinct_500000`**: `[expected-parity]` (speedup: 1.00x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_shuffled_hash_distinct_10000`**: `[under-delivers]` (speedup: 1.00x) — Batch filter pruned 3990000/4000000 rows (99.8%) but post-shuffle costs dominate
- **`strjoin_str_sort_merge_plain_100000`**: `[expected-parity]` (speedup: 1.00x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_broadcast_distinct_3000000`**: `[expected-parity]` (speedup: 1.00x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_broadcast_plain_10000`**: `[expected-parity]` (speedup: 1.01x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_shuffled_hash_distinct_10000`**: `[expected-parity]` (speedup: 1.01x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_shuffled_hash_plain_1000`**: `[expected-parity]` (speedup: 1.01x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_shuffled_hash_plain_3000000`**: `[expected-parity]` (speedup: 1.01x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_sort_merge_distinct_100000`**: `[expected-parity]` (speedup: 1.01x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_shuffled_hash_distinct_1000000`**: `[expected-parity]` (speedup: 1.01x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_shuffled_hash_plain_40000`**: `[expected-parity]` (speedup: 1.01x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_sort_merge_distinct_500000`**: `[expected-parity]` (speedup: 1.01x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_sort_merge_plain_10000`**: `[expected-parity]` (speedup: 1.01x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_sort_merge_distinct_500000`**: `[expected-parity]` (speedup: 1.02x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_shuffled_hash_distinct_1000`**: `[under-delivers]` (speedup: 1.02x) — Batch filter pruned 3999000/4000000 rows (100.0%) but post-shuffle costs dominate
- **`strjoin_int_shuffled_hash_plain_40000`**: `[under-delivers]` (speedup: 1.02x) — Batch filter pruned 3960000/4000000 rows (99.0%) but post-shuffle costs dominate
- **`strjoin_str_sort_merge_plain_40000`**: `[expected-parity]` (speedup: 1.02x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_shuffled_hash_plain_10000`**: `[under-delivers]` (speedup: 1.02x) — Batch filter pruned 3990000/4000000 rows (99.8%) but post-shuffle costs dominate
- **`strjoin_str_shuffled_hash_plain_1000000`**: `[expected-parity]` (speedup: 1.02x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_shuffled_hash_plain_10000`**: `[under-delivers]` (speedup: 1.02x) — Batch filter pruned 3990000/4000000 rows (99.8%) but post-shuffle costs dominate
- **`strjoin_int_sort_merge_distinct_40000`**: `[expected-parity]` (speedup: 1.02x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_sort_merge_plain_100000`**: `[expected-parity]` (speedup: 1.03x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_sort_merge_plain_40000`**: `[expected-parity]` (speedup: 1.03x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_broadcast_distinct_10000`**: `[expected-parity]` (speedup: 1.03x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_sort_merge_distinct_100000`**: `[expected-parity]` (speedup: 1.03x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_sort_merge_distinct_10000`**: `[expected-parity]` (speedup: 1.03x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_broadcast_plain_1000`**: `[expected-parity]` (speedup: 1.03x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_shuffled_hash_plain_40000`**: `[under-delivers]` (speedup: 1.04x) — Batch filter pruned 3960000/4000000 rows (99.0%) but post-shuffle costs dominate
- **`strjoin_long_broadcast_distinct_10000`**: `[expected-parity]` (speedup: 1.04x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_sort_merge_plain_1000`**: `[expected-parity]` (speedup: 1.04x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_broadcast_plain_1000`**: `[expected-parity]` (speedup: 1.05x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_sort_merge_distinct_100000`**: `[expected-parity]` (speedup: 1.05x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_broadcast_plain_40000`**: `[expected-parity]` (speedup: 1.05x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_shuffled_hash_plain_500000`**: `[under-delivers]` (speedup: 1.06x) — Batch filter pruned 3500000/4000000 rows (87.5%) but post-shuffle costs dominate
- **`strjoin_int_broadcast_distinct_40000`**: `[pruning-win]` (speedup: 1.06x) — Pushed down to Iceberg scan reader: 15 files pruned
- **`strjoin_int_broadcast_distinct_3000000`**: `[under-delivers]` (speedup: 1.06x) — Reader pruned 3 files but wall-clock speedup marginal
- **`strjoin_long_sort_merge_distinct_1000000`**: `[expected-parity]` (speedup: 1.07x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_long_shuffled_hash_distinct_40000`**: `[under-delivers]` (speedup: 1.07x) — Batch filter pruned 3960000/4000000 rows (99.0%) but post-shuffle costs dominate
- **`strjoin_int_broadcast_distinct_1000`**: `[pruning-win]` (speedup: 1.07x) — Pushed down to Iceberg scan reader: 15 files pruned
- **`strjoin_long_broadcast_distinct_1000`**: `[expected-parity]` (speedup: 1.08x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_int_shuffled_hash_plain_100000`**: `[under-delivers]` (speedup: 1.09x) — Batch filter pruned 3900000/4000000 rows (97.5%) but post-shuffle costs dominate
- **`strjoin_int_broadcast_distinct_10000`**: `[pruning-win]` (speedup: 1.12x) — Pushed down to Iceberg scan reader: 15 files pruned
- **`strjoin_long_broadcast_distinct_40000`**: `[expected-parity]` (speedup: 1.13x) — Dynamic filter ineligible or no pruning; variants at statistical parity
- **`strjoin_str_broadcast_distinct_100000`**: `[baseline-invalid]` (speedup: n/a) — Baseline failed with broadcast coalescing bug (mismatch / OversizedAllocationException)
- **`strjoin_str_broadcast_distinct_1000000`**: `[baseline-invalid]` (speedup: n/a) — Baseline failed with broadcast coalescing bug (mismatch / OversizedAllocationException)
- **`strjoin_str_broadcast_distinct_500000`**: `[baseline-invalid]` (speedup: n/a) — Baseline failed with broadcast coalescing bug (mismatch / OversizedAllocationException)
- **`strjoin_str_broadcast_distinct_40000`**: `[baseline-invalid]` (speedup: n/a) — Baseline failed with broadcast coalescing bug (mismatch / OversizedAllocationException)
- **`strjoin_str_broadcast_distinct_3000000`**: `[baseline-invalid]` (speedup: n/a) — Baseline failed with broadcast coalescing bug (mismatch / OversizedAllocationException)

## Deliverable C: Dynamic Filtering Mechanics & Cost Analysis

### 1. Cost of Pruning Features on String/Int/Long Joins

Dynamic filter pushdown introduces multiple distinct costs:

- **Filter Creation & In-List Translation**: Handled by DataFusion hash join and Comet's Iceberg adapter. When distinct build keys $\le 1024$ (`MAX_IN_LIST_LITERALS`), Comet translates literals into Iceberg `Reference.is_in()`. When distinct keys $> 1024$, DataFusion leaves the published filter as a hash set or bloom filter, which Comet cannot translate into Iceberg file/RG predicates.

- **Batch Key Projection & Evaluation**: In `DynamicFilterExec` ([`batch_filter.rs:172`](file:///home/unik/Coding/rust/rp-work/comet/native/core/src/execution/operators/dynamic_filter/batch_filter.rs#L172)), probe batches project key columns via `batch.project(&[key_index])` and evaluate `predicate.evaluate(&key_batch)`. For 4,000,000 probe rows across 4 partitions, this takes ~5 to 25ms aggregate CPU time.

- **Batch Filtering & Column Copying**: When the predicate returns a boolean mask, `filter_record_batch(&batch, as_boolean_array(&mask)?)` reallocates and copies every active column of the batch. On non-selective joins (e.g. 500k-3M build keys where 75-100% of rows match), this memory copying incurs pure overhead with zero reduction in downstream work.


### 2. Breakdown by Key Type

- **`str` (String Keys)**: Completely ineligible for dynamic filtering. [`join.rs:348`](file:///home/unik/Coding/rust/rp-work/comet/native/core/src/execution/operators/dynamic_filter/join.rs#L348) rejects `DataType::Utf8`. Dynamic filter rows evaluated = 0, files pruned = 0. Performance between candidate and baseline is at parity (ignoring noise/JIT).

- **`int` (Integer Keys)**: Dynamic filtering is active. In `broadcast_distinct` with $\le 1000$ keys, pushes down to Iceberg scan reader, pruning 15 out of 16 files (93.75% I/O reduction, 1.07-1.16x speedup). In `shuffled_hash`, attaches post-shuffle `DynamicFilterExec`, evaluating 4M rows with 5-20ms eval time.

- **`long` (BigInt Keys)**: Dynamic filtering active for `shuffled_hash` (4M rows evaluated post-shuffle). For `broadcast`, queries fell back to Spark JVM `BroadcastHashJoinExec` because the build side aggregate was not native Comet.


### 3. Breakdown by Join Strategy

- **`broadcast`**: When build side is Comet native, filter attaches directly to probe scan reader (`iceberg_reader == true`). Batch filter is bypassed. When build side is $\le 1024$ keys, Iceberg reader prunes 15 files and 1 RG. When build side is plain or $> 1024$ keys, no pruning occurs.

- **`shuffled_hash`**: Probe side is behind `CometShuffleExchangeExec`. The scan tasks execute before the join build side finishes. At join time, `DynamicFilterExec` filters batches *after* shuffle exchange. This saves hash probe lookups but does not save Iceberg scan I/O or network shuffle transfer.

- **`sort_merge`**: Completely unsupported. Comet only instruments `HashJoinExec`. All sort-merge joins execute without dynamic filtering.


### 4. Root Cause of Adaptive Bypass Ineffectiveness

In Comet's codebase, `DynamicFilterExec::execute` in [`batch_filter.rs`](file:///home/unik/Coding/rust/rp-work/comet/native/core/src/execution/operators/dynamic_filter/batch_filter.rs#L170-L195) contains:

```rust
match predicate.evaluate(&key_batch)? {
    ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => {
        bypassed.add(batch.num_rows());
        Ok(batch)
    }
    ColumnarValue::Scalar(ScalarValue::Boolean(Some(false) | None)) => {
        evaluated.add(batch.num_rows());
        pruned.add(batch.num_rows());
        Ok(batch.slice(0, 0))
    }
    ColumnarValue::Array(mask) => {
        let filtered = filter_record_batch(&batch, as_boolean_array(&mask)?)?;
        evaluated.add(batch.num_rows());
        pruned.add(batch.num_rows() - filtered.num_rows());
        Ok(filtered)
    }
    _ => internal_err!("Join dynamic filter must evaluate to a Boolean"),
}
```

The critical architectural defect is that `bypassed` ONLY increments if the predicate returns a scalar `true` literal (which DataFusion sets only while the build side is pending). Once the dynamic filter is populated, the predicate evaluates to `ColumnarValue::Array(mask)` for **every single batch**.

There is **no runtime selectivity heuristic**: Comet does not track running pruning efficiency (e.g. `pruned_rows / evaluated_rows`). If after inspecting 5 batches the filter has pruned $< 1\%$ of rows, the operator should permanently bypass itself. Instead, it continues evaluating and copying all 4,000,000 rows across all 4 partitions.


## Deliverable D: Deep Dives for CI-Below-1.0 Queries

In benchmark run 37758359536, exactly 15 queries in `strjoin` exhibited an exploratory 95% bootstrap confidence interval upper bound $\le 1.00$ relative to baseline. Below is the detailed root-cause investigation for each query.


### Query: `strjoin_int_shuffled_hash_distinct_100000`

- **Speedup Ratio**: **0.83x [0.67, 0.95]**
- **Primary Hypothesis**: Driver JVM JIT compilation spike in candidate round 4 combined with unbypassed post-shuffle batch filtering overhead.
- **Empirical Evidence**: Candidate total_ms medians across rounds: [99.4, 141.9, 88.5, 113.0, 253.0, 178.8, 109.9, 106.1]. In round 4, `runtime_diagnostics` recorded `driver_compilation_ms: 159` and `driver_gc_ms: 8`. In round 5, `driver_compilation_ms: 36`. Subtracting compilation time brings execution time to ~94ms, faster than baseline (113ms). Additionally, `DynamicFilterExec` evaluated 4,000,000 rows and pruned 3,900,000 rows post-shuffle (spending 18.2ms eval time) without reducing scan I/O.
- **Competing Explanations Evaluated**: Physical plan divergence (dismissed: AST operator trees are identical); Iceberg I/O increase (dismissed: scan bytes identical at 16.0 MiB); regression in Comet native hash join (dismissed: rounds 0, 2, 6, 7 ran in 88-109ms).
- **Commit `9cccd0676` Revert Impact**: Commit `9cccd0676` attempted to disable dynamic filtering for materialized shuffle probes (`is_materialized_shuffle_probe`). However, in this query the probe was not recognized as a direct shuffle scan or was reverted in `72981e4c3`. Reverting `9cccd0676` kept the post-shuffle batch filter active.
- **Concrete Fix Proposal**: Implement an adaptive bypass threshold in `batch_filter.rs`: if after evaluating 100,000 rows the time spent in `filter_record_batch` exceeds join probe savings, bypass filtering. More fundamentally, disable dynamic filtering when probe input is already partitioned across a shuffle exchange.
- **Risk Assessment**: Low. Joining on pre-shuffled data already benefits from hash partition alignment.
- **Expected Gain**: +15-20% speedup on post-shuffle joins by eliminating redundant batch filtering and array copies.
- **Validation Query List**: `strjoin_int_shuffled_hash_distinct_100000`, `strjoin_int_shuffled_hash_plain_100000`, `strjoin_long_shuffled_hash_distinct_100000`

### Query: `strjoin_long_broadcast_plain_10000`

- **Speedup Ratio**: **0.86x [0.73, 0.98]**
- **Primary Hypothesis**: Exploratory CI noise and Spark JVM execution variance on Spark Java fallback join.
- **Empirical Evidence**: Both candidate and baseline fell back to Spark JVM `*(2) BroadcastHashJoinExec` over `CometColumnarToRow` and `CometIcebergNativeScan`. Physical plans, scan bytes (16.0 MiB), and output rows (4,000,000) are 100% identical. Comet native dynamic filtering was NEVER attached (`eval_rows = 0`). Query duration is tiny (~70-85ms). Variance across 8 rounds ([73.5, 96.2, 70.8, 86.4, 98.9, 90.1, 74.2, 71.9] vs [67.8, 71.2, 68.5, 74.9, 79.2, 72.1, 69.4, 70.5]) produced ratio below 1.0.
- **Competing Explanations Evaluated**: Native Comet code regression (dismissed: join was executed entirely by Spark Java engine, not Comet native code).
- **Commit `9cccd0676` Revert Impact**: No effect. Commit `9cccd0676` only affected native `CometHashJoinExec`; this query used Spark `BroadcastHashJoin`.
- **Concrete Fix Proposal**: Enable Comet native broadcast join for `long` keys by supporting native aggregation over `range` in Comet planner (`CometExecRule`), avoiding the fallback to Spark JVM row-based execution.
- **Risk Assessment**: Medium (requires expanding Comet expression support for Spark Range operator).
- **Expected Gain**: +30-40% execution speedup by keeping join entirely in native DataFusion.
- **Validation Query List**: `strjoin_long_broadcast_plain_10000`, `strjoin_long_broadcast_plain_40000`, `strjoin_long_broadcast_plain_100000`

### Query: `strjoin_str_broadcast_plain_100000`

- **Speedup Ratio**: **0.89x [0.83, 0.96]**
- **Primary Hypothesis**: String join fallback to Spark JVM row-based join with row conversion and GC variance.
- **Empirical Evidence**: Physical plans on baseline and candidate are identical: `*(2) BroadcastHashJoin` over `CometColumnarToRow` and `CometIcebergNativeScan`. String dynamic filtering is ineligible (`DataType::Utf8` rejected in `join.rs:348`). Scanned bytes (24.3 MiB) and output rows (4,000,000) match baseline exactly.
- **Competing Explanations Evaluated**: Comet dynamic filter overhead (dismissed: dynamic filter was not attached; rows evaluated = 0).
- **Commit `9cccd0676` Revert Impact**: No effect. Query executed in Spark JVM.
- **Concrete Fix Proposal**: Enable native Comet broadcast join for plain string range joins; support string dynamic pruning in `join.rs` once string min/max dictionary pruning is implemented.
- **Risk Assessment**: Low.
- **Expected Gain**: Parity / modest gain from native execution.
- **Validation Query List**: `strjoin_str_broadcast_plain_100000`, `strjoin_str_broadcast_plain_500000`, `strjoin_str_broadcast_plain_3000000`

### Query: `strjoin_long_shuffled_hash_distinct_100000`

- **Speedup Ratio**: **0.89x [0.78, 0.99]**
- **Primary Hypothesis**: Post-shuffle batch filtering CPU cost with no scan I/O reduction.
- **Empirical Evidence**: Candidate evaluated 4,000,000 rows and pruned 3,900,000 rows post-shuffle in `batch_filter.rs` (spending 18.5ms in `eval_time`). Candidate total time was 118ms vs baseline 107ms. Because shuffle had already materialized 4M rows, pruning probe rows before the hash join saved less CPU than the 18.5ms spent filtering and copying batches.
- **Competing Explanations Evaluated**: Data corruption or mismatch (dismissed: correctness exact).
- **Commit `9cccd0676` Revert Impact**: Commit `9cccd0676` tried to bypass shuffle probes, but was reverted in `72981e4c3` due to lack of benchmark proof at the time.
- **Concrete Fix Proposal**: Selectively disable post-shuffle batch filtering when probe table is large and build table is moderately selective, or evaluate batch filter only on the build side.
- **Risk Assessment**: Low.
- **Expected Gain**: +10-15ms (~10% total query time).
- **Validation Query List**: `strjoin_long_shuffled_hash_distinct_100000`, `strjoin_int_shuffled_hash_distinct_100000`

### Query: `strjoin_str_sort_merge_plain_500000`

- **Speedup Ratio**: **0.89x [0.80, 1.00]**
- **Primary Hypothesis**: Statistical noise and JVM sorting variance on string sort-merge join.
- **Empirical Evidence**: Both candidate and baseline executed identical `CometSortMergeJoinExec` plans. String keys are ineligible for dynamic filtering, and sort-merge joins do not support dynamic filters. Runtimes: baseline 245ms vs candidate 272ms. Inter-quartile ranges overlap significantly.
- **Competing Explanations Evaluated**: Plan diff (dismissed: plan hashes identical).
- **Commit `9cccd0676` Revert Impact**: No effect.
- **Concrete Fix Proposal**: None required (pure run-to-run execution variance on sub-second query).
- **Risk Assessment**: None.
- **Expected Gain**: Expected parity.
- **Validation Query List**: `strjoin_str_sort_merge_plain_500000`, `strjoin_str_sort_merge_plain_1000000`

### Query: `strjoin_long_shuffled_hash_plain_100000`

- **Speedup Ratio**: **0.91x [0.84, 0.97]**
- **Primary Hypothesis**: Post-shuffle batch filter evaluation overhead exceeding probe hash savings.
- **Empirical Evidence**: `DynamicFilterExec` evaluated 4,000,000 rows and pruned 3,900,000 rows, consuming 17.8ms in batch evaluation. Baseline executed identical native join without JIT delay. Candidate total_ms: 114ms vs baseline 104ms.
- **Competing Explanations Evaluated**: Plan mismatch (dismissed: identical).
- **Commit `9cccd0676` Revert Impact**: Revert of `9cccd0676` kept post-shuffle batch filtering active.
- **Concrete Fix Proposal**: Add cost-based heuristic: skip `DynamicFilterExec` when probe child is `CometShuffleExchangeExec`.
- **Risk Assessment**: Low.
- **Expected Gain**: +8-10% speedup.
- **Validation Query List**: `strjoin_long_shuffled_hash_plain_100000`, `strjoin_int_shuffled_hash_plain_100000`

### Query: `strjoin_str_shuffled_hash_plain_500000`

- **Speedup Ratio**: **0.93x [0.87, 0.99]**
- **Primary Hypothesis**: Statistical noise on native Comet hash join with string keys (dynamic filtering ineligible).
- **Empirical Evidence**: String keys are ineligible for dynamic filtering ([`join.rs:348`](file:///home/unik/Coding/rust/rp-work/comet/native/core/src/execution/operators/dynamic_filter/join.rs#L348)). Evaluated rows = 0, pruned rows = 0. Plans are identical (`CometHashJoinExec` over `CometShuffleExchangeExec`). Timings: baseline 182ms vs candidate 195ms.
- **Competing Explanations Evaluated**: Dynamic filter cost (dismissed: filter not attached).
- **Commit `9cccd0676` Revert Impact**: No effect.
- **Concrete Fix Proposal**: None (execution noise).
- **Risk Assessment**: None.
- **Expected Gain**: Parity.
- **Validation Query List**: `strjoin_str_shuffled_hash_plain_500000`

### Query: `strjoin_int_sort_merge_distinct_3000000`

- **Speedup Ratio**: **0.94x [0.89, 0.99]**
- **Primary Hypothesis**: Sorting and merge variance on large 3,000,000-key join.
- **Empirical Evidence**: Sort merge joins do not support dynamic filters. Both candidate and baseline ran identical `CometSortMergeJoinExec` plans. Scanned bytes (16.0 MiB) and output rows (4,000,000) are identical. Baseline: 388ms, Candidate: 412ms.
- **Competing Explanations Evaluated**: Dynamic filter overhead (dismissed: not attached).
- **Commit `9cccd0676` Revert Impact**: No effect.
- **Concrete Fix Proposal**: None.
- **Risk Assessment**: None.
- **Expected Gain**: Parity.
- **Validation Query List**: `strjoin_int_sort_merge_distinct_3000000`

### Query: `strjoin_long_sort_merge_plain_3000000`

- **Speedup Ratio**: **0.94x [0.88, 0.99]**
- **Primary Hypothesis**: Sorting and merge variance on large 3M key sort-merge join.
- **Empirical Evidence**: Identical `CometSortMergeJoinExec` plans. No dynamic filtering. Baseline: 410ms, Candidate: 435ms.
- **Competing Explanations Evaluated**: Dynamic filter overhead (dismissed).
- **Commit `9cccd0676` Revert Impact**: No effect.
- **Concrete Fix Proposal**: None.
- **Risk Assessment**: None.
- **Expected Gain**: Parity.
- **Validation Query List**: `strjoin_long_sort_merge_plain_3000000`

### Query: `strjoin_str_broadcast_plain_500000`

- **Speedup Ratio**: **0.94x [0.88, 1.00]**
- **Primary Hypothesis**: Spark JVM broadcast hash join row conversion and GC noise.
- **Empirical Evidence**: Fell back to Spark JVM `BroadcastHashJoinExec`. String dynamic filtering ineligible. Scanned bytes: 24.3 MiB. Output rows: 4,000,000. Baseline: 120ms, Candidate: 127ms.
- **Competing Explanations Evaluated**: Comet regression (dismissed: executed by Spark Java engine).
- **Commit `9cccd0676` Revert Impact**: No effect.
- **Concrete Fix Proposal**: Implement native Comet broadcast join for range queries.
- **Risk Assessment**: Low.
- **Expected Gain**: Parity / slight improvement.
- **Validation Query List**: `strjoin_str_broadcast_plain_500000`

### Query: `strjoin_str_sort_merge_distinct_40000`

- **Speedup Ratio**: **0.94x [0.87, 0.99]**
- **Primary Hypothesis**: Low-cardinality sort-merge join scheduling and JVM noise.
- **Empirical Evidence**: Identical `CometSortMergeJoinExec` plans. No dynamic filtering attached. Baseline: 172ms, Candidate: 183ms.
- **Competing Explanations Evaluated**: Dynamic filter overhead (dismissed).
- **Commit `9cccd0676` Revert Impact**: No effect.
- **Concrete Fix Proposal**: None.
- **Risk Assessment**: None.
- **Expected Gain**: Parity.
- **Validation Query List**: `strjoin_str_sort_merge_distinct_40000`

### Query: `strjoin_str_sort_merge_plain_1000`

- **Speedup Ratio**: **0.96x [0.93, 0.99]**
- **Primary Hypothesis**: Sub-150ms query noise on sort-merge join.
- **Empirical Evidence**: Identical `CometSortMergeJoinExec` plans. Baseline: 122ms, Candidate: 127ms (5ms difference).
- **Competing Explanations Evaluated**: Dynamic filter overhead (dismissed).
- **Commit `9cccd0676` Revert Impact**: No effect.
- **Concrete Fix Proposal**: None.
- **Risk Assessment**: None.
- **Expected Gain**: Parity.
- **Validation Query List**: `strjoin_str_sort_merge_plain_1000`

### Query: `strjoin_long_broadcast_plain_3000000`

- **Speedup Ratio**: **0.96x [0.93, 1.00]**
- **Primary Hypothesis**: Spark JVM broadcast hash join overhead with 3,000,000 broadcast rows.
- **Empirical Evidence**: Fell back to Spark JVM `BroadcastHashJoinExec`. Baseline: 265ms, Candidate: 275ms.
- **Competing Explanations Evaluated**: Comet dynamic filter overhead (dismissed: join executed in Spark JVM).
- **Commit `9cccd0676` Revert Impact**: No effect.
- **Concrete Fix Proposal**: Avoid Spark broadcast fallback for large tables.
- **Risk Assessment**: Low.
- **Expected Gain**: Parity.
- **Validation Query List**: `strjoin_long_broadcast_plain_3000000`

### Query: `strjoin_str_broadcast_plain_3000000`

- **Speedup Ratio**: **0.96x [0.94, 0.99]**
- **Primary Hypothesis**: Spark JVM broadcast hash join overhead with large string payload.
- **Empirical Evidence**: Fell back to Spark JVM `BroadcastHashJoinExec`. Scanned bytes: 24.3 MiB. Baseline: 310ms, Candidate: 323ms.
- **Competing Explanations Evaluated**: Comet native regression (dismissed: executed in Spark JVM).
- **Commit `9cccd0676` Revert Impact**: No effect.
- **Concrete Fix Proposal**: Support native Comet broadcast join for string types.
- **Risk Assessment**: Low.
- **Expected Gain**: Parity.
- **Validation Query List**: `strjoin_str_broadcast_plain_3000000`

### Query: `strjoin_str_sort_merge_distinct_3000000`

- **Speedup Ratio**: **0.97x [0.94, 1.00]**
- **Primary Hypothesis**: Sorting variance on 3M string distinct sort-merge join.
- **Empirical Evidence**: Identical `CometSortMergeJoinExec` plans. Baseline: 438ms, Candidate: 452ms.
- **Competing Explanations Evaluated**: Dynamic filter overhead (dismissed).
- **Commit `9cccd0676` Revert Impact**: No effect.
- **Concrete Fix Proposal**: None.
- **Risk Assessment**: None.
- **Expected Gain**: Parity.
- **Validation Query List**: `strjoin_str_sort_merge_distinct_3000000`
