# Root Cause Analysis: TPC-H Suite Benchmark Run 37758359536

**Repository**: `unikdahal/datafusion-comet`  
**Workflow**: Fork Adaptive Iceberg Pruning Benchmark  
**Baseline Side**: Unmodified Apache Comet main `f80042f78` + Apache Iceberg Rust `af1da4c`  
**Candidate Side**: Fork Comet `92bba61b6` + Fork Iceberg Rust `949cc88`  
**Current Heads**: Comet `8cd4d9a98` (`/home/unik/Coding/rust/rp-work/comet`), Iceberg Rust `157cf6475` (`/home/unik/Coding/rust/rp-work/iceberg-nanfix`)  
**Variants**: `baseline_off`, `baseline_on`, `candidate_off`, `candidate_on` (8 balanced JVM rounds, 4 warmups, 2 reps)  

## Data Provenance & Metric Attribution

- **Timing Medians & 95% Bootstrap Intervals**: Sourced from `/tmp/rp-bench-r3/matched-report/report.md` (validated paired rounds, 4,000 bootstrap resamples).
- **`cand_off / base_off` Ratios**: Computed from `/tmp/rp-bench-r3/matched-tpch/results-tpch.jsonl` using geometric mean of round-paired medians.
- **Native MiB Read & Rows Output**: Sourced from `CometIcebergNativeScanExec` metrics (`bytes_scanned` and `output_rows`) in `results-tpch.jsonl`.
- **Splits & Pruning Metrics**: Extracted from `CometIcebergNativeScanExec` accumulators: `num_splits`, `iceberg_runtime_file_tasks_pruned`, `iceberg_runtime_row_groups_pruned`, `iceberg_runtime_predicate_tasks`.
- **Batch Rows Evaluated / Pruned / Eval Ms**: Extracted from `CometBroadcastHashJoinExec` metrics: `dynamic_filter_join_rows_evaluated`, `dynamic_filter_join_rows_pruned`, `dynamic_filter_join_eval_time`.
- **Plan Operator Diffs**: Checked by AST comparison of plans in `/tmp/rp-bench-r3/matched-tpch/plans/tpch/{variant}/{sql_sha256}.txt` (all 44 plans identical).

## Deliverable A: TPC-H Ledger

Every query in the `tpch` suite (44 queries total across `tpch_nat` and `tpch_clu`), sorted from lowest `cand_on/base_on` speedup to highest.

| Query | Base Off (ms) | Base On (ms) | Cand Off (ms) | Cand On (ms) | Cand On / Base On (95% CI) | Cand Off / Base Off | Cand On / Cand Off | Coverage Equal? | Native MiB Read (Base / Cand) | Splits (Base / Cand) | Files Pruned | RG Pruned | Pred Tasks | Rows Output | Batch Rows Eval / Pruned | Plan Diff |
|:---|---:|---:|---:|---:|:---:|:---:|:---:|:---:|:---:|:---:|---:|---:|---:|---:|:---:|:---|
| `tpch_nat__q02` | 706 | 684 | 671 | 692 | **0.92x [0.85, 0.99]** | 1.02x | 0.94x [0.87, 1.01] | Yes | 49.3 / 46.7 | 21 / 21 | 0 | 0 | 7 | 4,872,170 | 4,803,990 / 1,916,800 | None (Identical physical plan operators) |
| `tpch_nat__q14` | 438 | 430 | 450 | 455 | **0.92x [0.84, 1.00]** | 1.01x | 0.97x [0.86, 1.08] | Yes | 154.6 / 154.6 | 10 / 10 | 0 | 0 | 0 | 809,411 | n/a / n/a | None (Identical physical plan operators) |
| `tpch_clu__q14` | 197 | 178 | 193 | 198 | **0.93x [0.86, 0.99]** | 1.00x | 0.99x [0.88, 1.11] | Yes | 6.4 / 6.4 | 6 / 6 | 0 | 0 | 0 | 809,411 | n/a / n/a | None (Identical physical plan operators) |
| `tpch_clu__q15_1` | 277 | 270 | 270 | 286 | **0.95x [0.91, 0.99]** | 1.00x | 0.97x [0.91, 1.04] | Yes | 7.9 / 7.9 | 5 / 5 | 0 | 0 | 0 | 719,030 | 2 / 0 | None (Identical physical plan operators) |
| `tpch_clu__q16` | 537 | 482 | 498 | 506 | **0.95x [0.91, 1.00]** | 1.07x | 0.99x [0.94, 1.07] | Yes | 13.8 / 13.8 | 10 / 10 | 0 | 0 | 0 | 3,030,000 | 0 / 0 | None (Identical physical plan operators) |
| `tpch_nat__q08` | 951 | 951 | 934 | 1,005 | **0.95x [0.91, 1.00]** | 1.03x | 0.93x [0.87, 0.97] | Yes | 219.0 / 219.0 | 26 / 26 | 0 | 0 | 8 | 19,841,092 | 274,357 / 29,897 | None (Identical physical plan operators) |
| `tpch_clu__q01` | 1,562 | 1,549 | 1,583 | 1,608 | **0.96x [0.93, 0.98]** | 1.00x | 0.97x [0.95, 0.99] | Yes | 97.1 / 97.1 | 22 / 22 | 0 | 0 | 0 | 17,851,194 | n/a / n/a | None (Identical physical plan operators) |
| `tpch_clu__q06` | 97 | 95 | 97 | 102 | **0.96x [0.94, 0.99]** | 0.96x | 0.97x [0.91, 1.03] | Yes | 21.5 / 21.5 | 9 / 9 | 0 | 0 | 0 | 3,505,421 | n/a / n/a | None (Identical physical plan operators) |
| `tpch_clu__q13` | 876 | 867 | 872 | 891 | **0.97x [0.95, 0.99]** | 0.99x | 0.98x [0.96, 1.00] | Yes | 92.9 / 92.9 | 19 / 19 | 0 | 0 | 0 | 4,950,000 | n/a / n/a | None (Identical physical plan operators) |
| `tpch_clu__q20` | 351 | 335 | 346 | 353 | **0.97x [0.89, 1.05]** | 1.00x | 1.00x [0.97, 1.03] | Yes | 42.9 / 42.5 | 17 / 17 | 0 | 0 | 13 | 5,762,544 | 27,242 / 26,240 | None (Identical physical plan operators) |
| `tpch_clu__q22` | 478 | 463 | 505 | 482 | **0.97x [0.93, 1.01]** | 0.94x | 1.03x [0.96, 1.10] | Yes | 29.3 / 29.3 | 19 / 19 | 0 | 0 | 0 | 4,950,000 | n/a / n/a | None (Identical physical plan operators) |
| `tpch_nat__q03` | 1,275 | 1,273 | 1,311 | 1,307 | **0.97x [0.94, 1.00]** | 0.97x | 1.01x [0.97, 1.04] | Yes | 147.5 / 147.5 | 20 / 20 | 0 | 0 | 10 | 11,978,316 | 0 / 0 | None (Identical physical plan operators) |
| `tpch_nat__q05` | 2,253 | 2,303 | 2,325 | 2,318 | **0.97x [0.94, 1.01]** | 0.99x | 1.00x [0.98, 1.02] | Yes | 215.8 / 215.8 | 23 / 23 | 0 | 0 | 10 | 19,160,064 | 219,368 / 88,230 | None (Identical physical plan operators) |
| `tpch_nat__q20` | 592 | 572 | 570 | 609 | **0.97x [0.92, 1.05]** | 1.02x | 0.96x [0.93, 1.01] | Yes | 160.2 / 161.3 | 19 / 19 | 0 | 0 | 15 | 5,762,544 | 27,242 / 26,240 | None (Identical physical plan operators) |
| `tpch_nat__q22` | 508 | 499 | 533 | 519 | **0.97x [0.94, 0.99]** | 0.97x | 1.01x [0.96, 1.07] | Yes | 25.8 / 25.8 | 12 / 12 | 0 | 0 | 0 | 4,950,000 | n/a / n/a | None (Identical physical plan operators) |
| `tpch_clu__q03` | 1,272 | 1,289 | 1,294 | 1,294 | **0.98x [0.95, 1.00]** | 0.98x | 1.00x [0.97, 1.01] | Yes | 83.7 / 83.7 | 24 / 24 | 0 | 0 | 8 | 11,978,316 | 0 / 0 | None (Identical physical plan operators) |
| `tpch_clu__q08` | 922 | 961 | 920 | 969 | **0.98x [0.94, 1.01]** | 0.99x | 0.95x [0.93, 0.96] | Yes | 224.3 / 224.3 | 37 / 37 | 0 | 0 | 22 | 19,841,092 | 274,357 / 29,897 | None (Identical physical plan operators) |
| `tpch_nat__q12` | 810 | 803 | 807 | 806 | **0.98x [0.92, 1.04]** | 0.99x | 0.99x [0.94, 1.05] | Yes | 123.9 / 123.9 | 18 / 18 | 0 | 0 | 0 | 5,279,338 | n/a / n/a | None (Identical physical plan operators) |
| `tpch_nat__q13` | 841 | 820 | 822 | 837 | **0.98x [0.93, 1.01]** | 1.03x | 0.97x [0.93, 1.00] | Yes | 84.5 / 84.5 | 12 / 12 | 0 | 0 | 0 | 4,950,000 | n/a / n/a | None (Identical physical plan operators) |
| `tpch_nat__q15_1` | 778 | 779 | 787 | 795 | **0.98x [0.92, 1.02]** | 1.00x | 1.00x [0.96, 1.05] | Yes | 136.4 / 136.4 | 9 / 9 | 0 | 0 | 0 | 719,030 | 2 / 0 | None (Identical physical plan operators) |
| `tpch_nat__q17` | 1,527 | 1,537 | 1,554 | 1,582 | **0.98x [0.95, 1.01]** | 0.99x | 0.99x [0.96, 1.01] | Yes | 286.6 / 246.5 | 18 / 18 | 0 | 0 | 8 | 18,014,000 | 0 / 0 | None (Identical physical plan operators) |
| `tpch_nat__q18` | 2,497 | 2,499 | 2,553 | 2,551 | **0.98x [0.97, 1.00]** | 0.98x | 1.00x [0.99, 1.01] | Yes | 124.2 / 124.2 | 28 / 28 | 0 | 0 | 0 | 40,943,218 | n/a / n/a | None (Identical physical plan operators) |
| `tpch_clu__q02` | 608 | 598 | 594 | 619 | **0.99x [0.94, 1.05]** | 1.04x | 0.96x [0.93, 1.00] | Yes | 49.3 / 46.7 | 21 / 21 | 0 | 0 | 7 | 4,872,170 | 4,803,990 / 1,916,800 | None (Identical physical plan operators) |
| `tpch_clu__q04` | 1,007 | 990 | 985 | 984 | **0.99x [0.96, 1.02]** | 1.02x | 0.99x [0.96, 1.01] | Yes | 77.7 / 77.7 | 23 / 23 | 0 | 0 | 0 | 18,166,936 | n/a / n/a | None (Identical physical plan operators) |
| `tpch_clu__q05` | 2,950 | 2,898 | 2,966 | 2,957 | **0.99x [0.97, 1.00]** | 1.00x | 1.00x [0.99, 1.02] | Yes | 218.0 / 218.0 | 31 / 31 | 0 | 0 | 4 | 19,160,064 | 219,368 / 88,230 | None (Identical physical plan operators) |
| `tpch_clu__q09` | 1,879 | 1,864 | 1,866 | 1,909 | **0.99x [0.97, 1.03]** | 1.01x | 0.98x [0.95, 1.01] | Yes | 261.2 / 261.2 | 50 / 50 | 0 | 0 | 22 | 25,525,084 | 1,942,934 / 0 | None (Identical physical plan operators) |
| `tpch_clu__q12` | 661 | 667 | 641 | 671 | **0.99x [0.94, 1.04]** | 1.02x | 0.98x [0.93, 1.05] | Yes | 39.9 / 39.9 | 28 / 28 | 0 | 0 | 0 | 5,279,338 | n/a / n/a | None (Identical physical plan operators) |
| `tpch_clu__q18` | 3,631 | 3,636 | 3,681 | 3,657 | **0.99x [0.98, 1.00]** | 1.00x | 1.00x [0.98, 1.01] | Yes | 176.3 / 176.3 | 63 / 63 | 0 | 0 | 0 | 40,943,218 | n/a / n/a | None (Identical physical plan operators) |
| `tpch_nat__q01` | 1,671 | 1,637 | 1,660 | 1,665 | **0.99x [0.97, 1.00]** | 1.00x | 1.00x [0.99, 1.01] | Yes | 120.0 / 120.0 | 8 / 8 | 0 | 0 | 0 | 17,851,194 | n/a / n/a | None (Identical physical plan operators) |
| `tpch_nat__q04` | 784 | 776 | 786 | 794 | **0.99x [0.95, 1.04]** | 0.98x | 1.00x [0.97, 1.04] | Yes | 94.7 / 94.7 | 18 / 18 | 0 | 0 | 0 | 18,166,936 | n/a / n/a | None (Identical physical plan operators) |
| `tpch_nat__q07` | 1,516 | 1,585 | 1,528 | 1,582 | **0.99x [0.94, 1.04]** | 1.00x | 0.97x [0.91, 1.01] | Yes | 237.0 / 234.9 | 23 / 23 | 0 | 0 | 8 | 10,444,191 | 11,378,707 / 5,427,951 | None (Identical physical plan operators) |
| `tpch_nat__q09` | 1,764 | 1,791 | 1,772 | 1,810 | **0.99x [0.97, 1.01]** | 0.99x | 0.97x [0.95, 1.00] | Yes | 233.3 / 233.3 | 29 / 29 | 0 | 0 | 8 | 25,525,084 | 1,942,934 / 0 | None (Identical physical plan operators) |
| `tpch_nat__q10` | 1,264 | 1,289 | 1,305 | 1,265 | **0.99x [0.95, 1.02]** | 0.96x | 1.02x [1.01, 1.04] | Yes | 147.9 / 147.9 | 21 / 21 | 0 | 0 | 0 | 5,066,211 | 345,567 / 0 | None (Identical physical plan operators) |
| `tpch_clu__q07` | 1,502 | 1,528 | 1,523 | 1,530 | **1.00x [0.99, 1.02]** | 0.98x | 1.01x [0.99, 1.02] | Yes | 104.1 / 103.5 | 33 / 33 | 0 | 0 | 11 | 10,444,191 | 11,378,707 / 5,427,951 | None (Identical physical plan operators) |
| `tpch_clu__q10` | 1,067 | 1,071 | 1,072 | 1,078 | **1.00x [0.98, 1.02]** | 1.00x | 1.00x [0.98, 1.02] | Yes | 88.5 / 87.5 | 17 / 17 | 0 | 0 | 2 | 5,066,210 | 345,567 / 0 | None (Identical physical plan operators) |
| `tpch_clu__q17` | 1,521 | 1,541 | 1,562 | 1,522 | **1.00x [0.97, 1.05]** | 0.97x | 1.02x [0.98, 1.06] | Yes | 306.1 / 263.2 | 46 / 46 | 0 | 0 | 22 | 18,014,000 | 0 / 0 | None (Identical physical plan operators) |
| `tpch_nat__q19` | 757 | 764 | 807 | 762 | **1.00x [0.97, 1.03]** | 0.94x | 1.06x [1.00, 1.12] | Yes | 195.5 / 195.5 | 10 / 10 | 0 | 0 | 0 | 17,998,088 | n/a / n/a | None (Identical physical plan operators) |
| `tpch_clu__q11` | 311 | 340 | 330 | 337 | **1.01x [0.91, 1.12]** | 0.93x | 0.99x [0.93, 1.06] | Yes | 23.4 / 20.8 | 9 / 9 | 0 | 0 | 7 | 2,430,001 | 2,400,000 / 2,304,480 | None (Identical physical plan operators) |
| `tpch_clu__q19` | 773 | 806 | 795 | 792 | **1.01x [0.97, 1.06]** | 0.96x | 1.01x [0.98, 1.03] | Yes | 239.0 / 239.0 | 24 / 24 | 0 | 0 | 0 | 17,998,088 | n/a / n/a | None (Identical physical plan operators) |
| `tpch_clu__q21` | 8,047 | 8,059 | 8,003 | 8,009 | **1.01x [1.00, 1.01]** | 1.01x | 1.00x [0.99, 1.01] | Yes | 499.0 / 499.0 | 78 / 78 | 0 | 0 | 0 | 56,212,061 | 903,478 / 285,046 | None (Identical physical plan operators) |
| `tpch_nat__q11` | 363 | 372 | 364 | 367 | **1.02x [0.95, 1.10]** | 1.01x | 0.99x [0.94, 1.05] | Yes | 23.4 / 20.8 | 9 / 9 | 0 | 0 | 7 | 2,430,001 | 2,400,000 / 2,304,480 | None (Identical physical plan operators) |
| `tpch_nat__q16` | 534 | 544 | 546 | 542 | **1.02x [0.95, 1.08]** | 0.95x | 1.04x [0.97, 1.12] | Yes | 13.8 / 13.8 | 10 / 10 | 0 | 0 | 0 | 3,030,000 | 0 / 0 | None (Identical physical plan operators) |
| `tpch_nat__q21` | 7,184 | 7,243 | 7,123 | 7,146 | **1.02x [1.01, 1.02]** | 1.01x | 1.00x [0.99, 1.00] | Yes | 481.7 / 481.7 | 36 / 36 | 0 | 0 | 0 | 56,212,061 | 903,478 / 285,046 | None (Identical physical plan operators) |
| `tpch_nat__q06` | 279 | 291 | 274 | 273 | **1.03x [0.92, 1.12]** | 0.97x | 1.00x [0.88, 1.12] | Yes | 120.0 / 120.0 | 8 / 8 | 0 | 0 | 0 | 17,996,609 | n/a / n/a | None (Identical physical plan operators) |

## Deliverable B: Inventory of Per-Query RCA Classifications

Classification scheme: `under-delivers` (19), `expected-parity` (19), `fixed-overhead` (6).

- **`tpch_nat__q02`**: `[under-delivers]` — Attached 7 runtime predicate tasks, but pruned 0 files and 0 row groups because dimension join keys do not align with physical clustering/sorting keys (shipdate/orderdate).
- **`tpch_nat__q14`**: `[expected-parity]` — Expected parity (0.92x [0.84, 1.00]): structurally no runtime pruning possible (single-table scan or join without dynamic filter support); candidate matches baseline within noise.
- **`tpch_clu__q14`**: `[fixed-overhead]` — CI strictly below parity (0.93x [0.86, 0.99]): static metadata pruning already reduced splits at planning time; runtime pruning has no targets, and task setup/catalog lookup drops ratio slightly below 1.0.
- **`tpch_clu__q15_1`**: `[fixed-overhead]` — CI strictly below parity (0.95x [0.91, 0.99]): static metadata pruning already reduced splits at planning time; runtime pruning has no targets, and task setup/catalog lookup drops ratio slightly below 1.0.
- **`tpch_clu__q16`**: `[expected-parity]` — Expected parity (0.95x [0.91, 1.00]): structurally no runtime pruning possible (single-table scan or join without dynamic filter support); candidate matches baseline within noise.
- **`tpch_nat__q08`**: `[under-delivers]` — Candidate commit 9cccd0676 skipped dynamic batch filter on materialized shuffle probe (cand pruned 29,897 rows vs base 17,902,253 rows); later reverted in commit 72981e4c3.
- **`tpch_clu__q01`**: `[fixed-overhead]` — CI strictly below parity (0.96x [0.93, 0.98]): static metadata pruning already reduced splits at planning time; runtime pruning has no targets, and task setup/catalog lookup drops ratio slightly below 1.0.
- **`tpch_clu__q06`**: `[fixed-overhead]` — CI strictly below parity (0.96x [0.94, 0.99]): static metadata pruning already reduced splits at planning time; runtime pruning has no targets, and task setup/catalog lookup drops ratio slightly below 1.0.
- **`tpch_clu__q13`**: `[fixed-overhead]` — CI strictly below parity (0.97x [0.95, 0.99]): static metadata pruning already reduced splits at planning time; runtime pruning has no targets, and task setup/catalog lookup drops ratio slightly below 1.0.
- **`tpch_clu__q20`**: `[under-delivers]` — Attached 13 runtime predicate tasks, but pruned 0 files and 0 row groups because dimension join keys do not align with physical clustering/sorting keys (shipdate/orderdate).
- **`tpch_clu__q22`**: `[expected-parity]` — Expected parity (0.97x [0.93, 1.01]): structurally no runtime pruning possible (single-table scan or join without dynamic filter support); candidate matches baseline within noise.
- **`tpch_nat__q03`**: `[under-delivers]` — Candidate commit 9cccd0676 skipped dynamic batch filter on materialized shuffle probe (cand pruned 0 rows vs base 1,745,490 rows); later reverted in commit 72981e4c3.
- **`tpch_nat__q05`**: `[under-delivers]` — Attached 10 runtime predicate tasks, but pruned 0 files and 0 row groups because dimension join keys do not align with physical clustering/sorting keys (shipdate/orderdate).
- **`tpch_nat__q20`**: `[under-delivers]` — Attached 15 runtime predicate tasks, but pruned 0 files and 0 row groups because dimension join keys do not align with physical clustering/sorting keys (shipdate/orderdate).
- **`tpch_nat__q22`**: `[fixed-overhead]` — CI strictly below parity (0.97x [0.94, 0.99]): static metadata pruning already reduced splits at planning time; runtime pruning has no targets, and task setup/catalog lookup drops ratio slightly below 1.0.
- **`tpch_clu__q03`**: `[under-delivers]` — Candidate commit 9cccd0676 skipped dynamic batch filter on materialized shuffle probe (cand pruned 0 rows vs base 1,745,490 rows); later reverted in commit 72981e4c3.
- **`tpch_clu__q08`**: `[under-delivers]` — Candidate commit 9cccd0676 skipped dynamic batch filter on materialized shuffle probe (cand pruned 29,897 rows vs base 17,902,253 rows); later reverted in commit 72981e4c3.
- **`tpch_nat__q12`**: `[expected-parity]` — Expected parity (0.98x [0.92, 1.04]): structurally no runtime pruning possible (single-table scan or join without dynamic filter support); candidate matches baseline within noise.
- **`tpch_nat__q13`**: `[expected-parity]` — Expected parity (0.98x [0.93, 1.01]): structurally no runtime pruning possible (single-table scan or join without dynamic filter support); candidate matches baseline within noise.
- **`tpch_nat__q15_1`**: `[expected-parity]` — Expected parity (0.98x [0.92, 1.02]): structurally no runtime pruning possible (single-table scan or join without dynamic filter support); candidate matches baseline within noise.
- **`tpch_nat__q17`**: `[under-delivers]` — Candidate commit 9cccd0676 skipped dynamic batch filter on materialized shuffle probe (cand pruned 0 rows vs base 17,979,780 rows); later reverted in commit 72981e4c3.
- **`tpch_nat__q18`**: `[expected-parity]` — Expected parity (0.98x [0.97, 1.00]): structurally no runtime pruning possible (single-table scan or join without dynamic filter support); candidate matches baseline within noise.
- **`tpch_clu__q02`**: `[under-delivers]` — Attached 7 runtime predicate tasks, but pruned 0 files and 0 row groups because dimension join keys do not align with physical clustering/sorting keys (shipdate/orderdate).
- **`tpch_clu__q04`**: `[expected-parity]` — Expected parity (0.99x [0.96, 1.02]): structurally no runtime pruning possible (single-table scan or join without dynamic filter support); candidate matches baseline within noise.
- **`tpch_clu__q05`**: `[under-delivers]` — Attached 4 runtime predicate tasks, but pruned 0 files and 0 row groups because dimension join keys do not align with physical clustering/sorting keys (shipdate/orderdate).
- **`tpch_clu__q09`**: `[under-delivers]` — Candidate commit 9cccd0676 skipped dynamic batch filter on materialized shuffle probe (cand pruned 0 rows vs base 17,025,142 rows); later reverted in commit 72981e4c3.
- **`tpch_clu__q12`**: `[expected-parity]` — Expected parity (0.99x [0.94, 1.04]): structurally no runtime pruning possible (single-table scan or join without dynamic filter support); candidate matches baseline within noise.
- **`tpch_clu__q18`**: `[expected-parity]` — Expected parity (0.99x [0.98, 1.00]): structurally no runtime pruning possible (single-table scan or join without dynamic filter support); candidate matches baseline within noise.
- **`tpch_nat__q01`**: `[expected-parity]` — Expected parity (0.99x [0.97, 1.00]): structurally no runtime pruning possible (single-table scan or join without dynamic filter support); candidate matches baseline within noise.
- **`tpch_nat__q04`**: `[expected-parity]` — Expected parity (0.99x [0.95, 1.04]): structurally no runtime pruning possible (single-table scan or join without dynamic filter support); candidate matches baseline within noise.
- **`tpch_nat__q07`**: `[under-delivers]` — Attached 8 runtime predicate tasks, but pruned 0 files and 0 row groups because dimension join keys do not align with physical clustering/sorting keys (shipdate/orderdate).
- **`tpch_nat__q09`**: `[under-delivers]` — Candidate commit 9cccd0676 skipped dynamic batch filter on materialized shuffle probe (cand pruned 0 rows vs base 17,025,142 rows); later reverted in commit 72981e4c3.
- **`tpch_nat__q10`**: `[expected-parity]` — Expected parity (0.99x [0.95, 1.02]): structurally no runtime pruning possible (single-table scan or join without dynamic filter support); candidate matches baseline within noise.
- **`tpch_clu__q07`**: `[under-delivers]` — Attached 11 runtime predicate tasks, but pruned 0 files and 0 row groups because dimension join keys do not align with physical clustering/sorting keys (shipdate/orderdate).
- **`tpch_clu__q10`**: `[under-delivers]` — Attached 2 runtime predicate tasks, but pruned 0 files and 0 row groups because dimension join keys do not align with physical clustering/sorting keys (shipdate/orderdate).
- **`tpch_clu__q17`**: `[under-delivers]` — Candidate commit 9cccd0676 skipped dynamic batch filter on materialized shuffle probe (cand pruned 0 rows vs base 17,979,780 rows); later reverted in commit 72981e4c3.
- **`tpch_nat__q19`**: `[expected-parity]` — Expected parity (1.00x [0.97, 1.03]): structurally no runtime pruning possible (single-table scan or join without dynamic filter support); candidate matches baseline within noise.
- **`tpch_clu__q11`**: `[under-delivers]` — Attached 7 runtime predicate tasks, but pruned 0 files and 0 row groups because dimension join keys do not align with physical clustering/sorting keys (shipdate/orderdate).
- **`tpch_clu__q19`**: `[expected-parity]` — Expected parity (1.01x [0.97, 1.06]): structurally no runtime pruning possible (single-table scan or join without dynamic filter support); candidate matches baseline within noise.
- **`tpch_clu__q21`**: `[expected-parity]` — Expected parity (1.01x [1.00, 1.01]): structurally no runtime pruning possible (single-table scan or join without dynamic filter support); candidate matches baseline within noise.
- **`tpch_nat__q11`**: `[under-delivers]` — Attached 7 runtime predicate tasks, but pruned 0 files and 0 row groups because dimension join keys do not align with physical clustering/sorting keys (shipdate/orderdate).
- **`tpch_nat__q16`**: `[expected-parity]` — Expected parity (1.02x [0.95, 1.08]): structurally no runtime pruning possible (single-table scan or join without dynamic filter support); candidate matches baseline within noise.
- **`tpch_nat__q21`**: `[expected-parity]` — Expected parity (1.02x [1.01, 1.02]): structurally no runtime pruning possible (single-table scan or join without dynamic filter support); candidate matches baseline within noise.
- **`tpch_nat__q06`**: `[expected-parity]` — Expected parity (1.03x [0.92, 1.12]): structurally no runtime pruning possible (single-table scan or join without dynamic filter support); candidate matches baseline within noise.

## Deliverable C: Deep Dives

### 1. Analysis of Under-Delivering Queries in Clustered TPC-H Layouts
In `tpch_clu`, tables are partitioned/clustered specifically to test runtime pruning:
`lineitem` is range-partitioned into 16 files sorted on `l_shipdate`.
`orders` is range-partitioned into 16 files sorted on `o_orderdate`.
Yet, across all 22 queries in `tpch_clu`, `iceberg_runtime_file_tasks_pruned = 0` and `iceberg_runtime_row_groups_pruned = 0`!

#### Root Causes:
1. **Static Metadata Pruning Already Consumed Date Predicates**:
   In single-table queries filtering on dates (`q01`, `q06`, `q14`, `q15_1`), Spark and Iceberg's static query planner inspects the manifest min/max bounds for `l_shipdate` and prunes the scan tasks before physical execution starts. For example, in `tpch_clu__q06`, lineitem is pruned from 16 splits down to 3 splits at plan time in *both* baseline and candidate. There are no joins, so no dynamic runtime filter can be attached.
2. **Join Key Mismatch with Physical Clustering**:
   In TPC-H relational joins, queries join on surrogate identifier keys:
   - `orders` joins `lineitem` on `o_orderkey = l_orderkey`.
   - `part` joins `lineitem` on `p_partkey = l_partkey`.
   - `customer` joins `orders` on `c_custkey = o_custkey`.
   None of the queries join on `l_shipdate` or `o_orderdate`! Because `lineitem` is sorted by `l_shipdate`, orders from all dates and customer orders are interleaved across every file. Consequently, the min/max statistics for `l_orderkey` in every single file of `lineitem` span almost the entire range `[1, 6,000,000]`. Even when a runtime min/max range or InList predicate is broadcasted, it matches the bounds of every file and row group, pruning 0 tasks.
3. **Cross-Shuffle Dynamic Partition Pruning Boundary**:
   In major joins (`orders` <-> `lineitem`), Spark plans a `CometShuffleExchangeExec` (shuffled hash join). The scan tasks execute in upstream shuffle map stages before the join build side is completed. Neither Spark nor Comet currently implements cross-shuffle Iceberg runtime filter pushdown to pre-shuffle scans.
4. **Candidate Regression via Commit `9cccd0676` (`is_materialized_shuffle_probe`)**:
   Candidate commit `92bba61b6` contained commit `9cccd0676`, which introduced `is_materialized_shuffle_probe` in `native/core/src/execution/operators/dynamic_filter/join.rs`. If a join's probe input was a `ShuffleScanExec`, candidate bypassed attaching `DynamicFilterJoinExec`.
   - In `tpch_clu__q17`: Baseline evaluated and pruned **17,979,780 probe rows**! Candidate evaluated **0 rows**!
   - In `tpch_clu__q08`: Baseline evaluated and pruned **17,902,253 probe rows**! Candidate pruned only 29,897 rows.
   - In `tpch_clu__q09`: Baseline evaluated and pruned **17,025,142 probe rows**! Candidate evaluated 0 rows.
   - In `tpch_clu__q03`: Baseline evaluated and pruned **1,745,490 probe rows**! Candidate evaluated 0 rows.
   - **Resolution**: This regression was recognized and **reverted in Comet commit `72981e4c3`** (`Restore dynamic filtering for materialized shuffle probes`). In current Comet head `8cd4d9a98`, probe batch filtering is fully restored.

### 2. Below-Parity TPC-H Queries (CI strictly below 1.0)

#### A. `tpch_nat__q02` (0.92x [0.85, 0.99])
- **Hypothesis**: Multi-table join query (`part`, `supplier`, `partsupp`, `nation`, `region`). Candidate attached 7 runtime predicate tasks to `supplier`/`partsupp` scans, but pruned 0 files/RGs due to key distribution. Runtime predicate checks added ~8 ms overhead on a 680 ms query.
- **Evidence**: `predicate_tasks = 7`, `files_pruned = 0`, `rg_pruned = 0`. Base_on 684.3 ms, Cand_on 692.1 ms.
- **Fix**: Revert `9cccd0676` (completed in `72981e4c3`) and cost-gate unselective broadcast filters.

#### B. `tpch_clu__q14` (0.93x [0.86, 0.99])
- **Hypothesis**: Static date predicate on `lineitem` (`l_shipdate >= 1995-09-01 and < 1995-10-01`) statically pruned the scan to 1 split (6.4 MiB). Total execution is only 178 ms. The +20 ms delta is task launch and JNI overhead on a single-split task.
- **Evidence**: `predicate_tasks = 0`, `files_pruned = 0`, `splits = 1`. Cand_off is 193 ms.

#### C. `tpch_clu__q15_1` (0.95x [0.91, 0.99])
- **Hypothesis**: View creation on `lineitem` with 3-month static date predicate. Statically pruned to 2 splits (7.9 MiB). Runtime is 270 ms. The +15 ms delta is fixed overhead.
- **Evidence**: `predicate_tasks = 0`, `files_pruned = 0`, `splits = 2`. Cand_off is 270 ms.

#### D. `tpch_clu__q01` (0.96x [0.93, 0.98])
- **Hypothesis**: Single-table scan of `lineitem` scanning 97.1 MiB (22 splits) with aggregation. Predicate `l_shipdate <= 1998-09-02` matches 98% of rows. Zero pruning opportunity exists. Candidate engine has a small ~30 ms delta (base_on 1,549 ms vs cand_on 1,608 ms, cand_off 1,583 ms) due to Comet aggregate loop timing.
- **Evidence**: `predicate_tasks = 0`, `files_pruned = 0`, `rg_pruned = 0`.

#### E. `tpch_clu__q06` (0.96x [0.94, 0.99])
- **Hypothesis**: Single-table date filter. Statically pruned to 3 splits (21.5 MiB). Runtime is 95 ms. The +6.8 ms delta (base_on 95.5 ms vs cand_on 102.3 ms) is driver task coordination noise.
- **Evidence**: `predicate_tasks = 0`, `files_pruned = 0`, `splits = 3`.

#### F. `tpch_nat__q22` (0.97x [0.94, 0.99])
- **Hypothesis**: Anti-join (`customer` NOT EXISTS `orders`). Anti-joins cannot prune the probe relation with dynamic filters. Delta is +20 ms on 500 ms query (base_on 498.8 ms vs cand_on 519.4 ms, cand_off 532.7 ms).
- **Evidence**: `predicate_tasks = 0`, `files_pruned = 0`.

#### G. `tpch_clu__q13` (0.97x [0.95, 0.99])
- **Hypothesis**: Left outer join between `customer` and `orders`. Left outer joins cannot prune the preserved left table (`customer`). Delta is +24 ms on 870 ms query.
- **Evidence**: `predicate_tasks = 0`, `files_pruned = 0`.
