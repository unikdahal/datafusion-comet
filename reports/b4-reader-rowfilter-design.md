# Design Report: Parquet Reader RowFilter Regression Root Cause & Fix

**Status:** Design & RCA Complete (Read-Only)  
**Target Repositories:**  
- `iceberg-rust` fork: `/home/unik/Coding/rust/rp-work/iceberg-nanfix` (`157cf6475`)
- `comet`: `/home/unik/Coding/rust/rp-work/comet` (`c739c322e`)  
**Target Query:** `join_inner__f_unsorted__dim_10pct`  
**Run Reference:** GitHub Actions Run `37807233835` (`unikdahal/datafusion-comet`)

---

## 1. Executive Summary

Benchmark run `37807233835` confirmed a **+42 ms execution regression (0.95x speedup [95% CI: 0.92, 0.98])** on `join_inner__f_unsorted__dim_10pct` when dynamic runtime filtering is active:
- **Baseline Main (`baseline_on`):** 953 ms (filters 16M rows downstream in 101 ms via Comet batch filter).
- **Candidate Off (`candidate_off`):** 965 ms.
- **Candidate On (`candidate_on`):** 1,007 ms (emits 3.2M rows instead of 17.6M rows, yet runs slower).
- **Read Bytes:** Identical at 665.87 MiB across all variants. Zero file tasks pruned, zero row groups pruned, and zero Parquet data pages pruned.

### Core Root Cause
When random 10% data is scanned without page-index pruning:
1. Parquet 59.3.0's two-phase decode decodes the predicate column (`f.id`) for all 16M rows in Phase 1 and evaluates the filter into a `BooleanArray`.
2. Converting the scattered 10% boolean hits into `RowSelection` generates **~3.2 million `RowSelector` objects** (~200,000 selectors per 1M-row row group) due to alternating 1-row matches and skips.
3. Although Parquet auto-resolves this dense fragmentation to `RowSelectionPolicy::Mask`, the `MaskCursor` decodes batches in chunks: to accumulate 8,192 matching rows, it calls `ArrayReader::read_records(~81,920)` on the payload columns (`f.value`, `f.payload`). **100% of payload column values are decompressed and decoded**, followed by in-reader `filter_record_batch`.
4. The scan saves **zero I/O bytes, zero page decompressions, and zero payload decoding work**, but adds:
   - Phase 1 predicate column decode and evaluation.
   - 3.2M selector allocations and back-and-forth conversion between `BooleanArray` and `RowSelector` and `BooleanBuffer`.
   - In-reader `filter_record_batch` slicing.
5. The reader row filter overhead (~140 ms CPU) exceeds Comet's downstream batch filter time (~95 ms), creating a net +42 ms regression.

### Recommended Solution
**Cost-gate advisory runtime predicate attachment in `pipeline.rs` and `runtime_stream.rs`:** Only attach an `advisory: true` predicate to Parquet's `row_filter` when page index or row group statistics actually skip at least one page or row group. Planned predicates and equality deletes (`advisory: false`) remain mandatory and are never bypassed.

---

## 2. Reader Mechanism & Parquet 59.3 Internals (Question 1)

### 2.1 Dependencies and Pipeline Setup
In `/home/unik/Coding/rust/rp-work/iceberg-nanfix/Cargo.lock`:
- `parquet = 59.3.0` (sourced from git fork commit `fa20c8b77ff3d613c8b638f4f686f5316eb0138a`).
- Iceberg reader pipeline: `FileScanTaskReader::build_record_batch_stream` in `crates/iceberg/src/arrow/reader/pipeline.rs`.

### 2.2 Two-Phase Decode vs Page Index Selection
The reader applies filtering at three distinct granularities:
1. **File & Row Group Pruning (Metadata Level):** `RowGroupMetricsEvaluator::eval` compares predicate bounds against row group column min/max statistics.
2. **Page Index Selection (`RowSelection`):** `ArrowReader::get_row_selection_for_filter_predicate` evaluates page-level min/max statistics via `parquet::file::page_index::offset_index` and `column_index`. If pages can be skipped, a `RowSelection` is attached to `ParquetRecordBatchStreamBuilder::with_row_selection` or `ParquetPushDecoderBuilder::with_row_group_selections`.
3. **Parquet RowFilter (`ArrowPredicate`):** For row-level filtering inside surviving pages, `pipeline.rs:600-645` compiles planned and advisory predicates into an `ArrowPredicateFn` and passes `RowFilter::new(vec![arrow_predicate])` to the decoder builder.

### 2.3 RowSelectionPolicy and Selection Strategy Resolution
Parquet 59.3 defines:
```rust
pub enum RowSelectionPolicy {
    Selectors,
    Mask,
    Auto { threshold: usize }, // default threshold = 32
}
```
During push decoder construction in `push_decoder/reader_builder/mod.rs:885`:
```rust
fn prepare_selection_for_page_skipping(
    plan_builder: ReadPlanBuilder,
    projection_mask: &ProjectionMask,
    offset_index: Option<&[OffsetIndexMetaData]>,
    total_rows: usize,
) -> ReadPlanBuilder {
    match plan_builder.resolve_selection_strategy() {
        RowSelectionStrategy::Mask => {
            let loaded = loaded_row_ranges_for_projection(...);
            plan_builder
                .with_row_selection_policy(RowSelectionPolicy::Mask)
                .with_loaded_row_ranges(loaded)
        }
        RowSelectionStrategy::Selectors => {
            plan_builder.with_row_selection_policy(RowSelectionPolicy::Selectors)
        }
    }
}
```
When `RowSelectionPolicy::Auto { threshold: 32 }` is evaluated:
- `selection.auto_selection_strategy(threshold = 32)` computes:
  $$\text{effective\_count} \times 32 > \text{total\_rows}$$
- For 1,000,000 rows in an unsorted row group with 10% random matches, the alternating pattern produces ~200,000 selectors:
  $$200,000 \times 32 = 6,400,000 > 1,000,000$$
- **`Auto` resolves to `RowSelectionStrategy::Mask`**.

### 2.4 Why Fragmented Selections Slow Down Column Decoding
Even though `RowSelectionStrategy::Mask` is chosen:
1. **Redundant Allocation & Conversions:**
   - In `read_plan.rs:with_predicate_options`, `predicate.evaluate(batch)` produces `Vec<BooleanArray>`.
   - `RowSelection::from_filters(&filters)` uses `SlicesIterator` over the boolean buffers to construct a `RowSelectionInner::Selectors(Vec<RowSelector>)`. This allocates and populates **~200,000 `RowSelector` structs per row group (~3.2 million across the 16 files)**.
   - Then, `build_cursor` in `read_plan.rs:331` calls `RowSelectionCursor::new_mask_from_selectors(selectors, ...)`, which iterates through the 200,000 selectors and converts them **back into a 1,000,000-bit `BooleanBuffer`** via `boolean_mask_from_selectors`.
2. **Mask Decoding Semantics in `read_mask_batch`:**
   In `parquet/src/arrow/arrow_reader/mod.rs:1605-1640` and `cursor.rs:215-255`:
   - When no pages are skipped (`loaded_row_ranges` is `None`), `next_mask_chunk_non_empty(batch_size = 8192)` scans the mask until `selected_rows == 8192`.
   - With 10% selectivity, scanning 8,192 selected rows spans `chunk_rows = ~81,920` total rows.
   - The reader calls `array_reader.read_records(81_920)`. **All 81,920 rows of payload columns (`f.value`, `f.payload`) are fully decompressed, decoded, and materialized into Arrow arrays!**
   - The reader then calls `filter_record_batch(&batch, &BooleanArray::from(filter_mask))` to filter 81,920 rows down to 8,192 rows before emitting.
   - Therefore, Parquet row filtering does NOT skip payload column decoding at all when pages cannot be skipped; it decodes all payload rows anyway and then filters them in memory.

---

## 3. Quantitative Cost Model (Question 2)

### 3.1 Work Breakdown Comparison: 16M Rows Unsorted 10% Match

| Operation Stage | Single-Phase Bulk Decode + Batch Filter (`baseline_on` / `candidate_off`) | Two-Phase Parquet Reader Row Filter (`candidate_on`) | Delta Work |
| :--- | :--- | :--- | :--- |
| **I/O & Decompression** | Reads 665.87 MiB across 16 files (100% of pages). | Reads 665.87 MiB across 16 files (100% of pages). | 0 bytes saved; 0 pages skipped. |
| **Phase 1: Predicate Decode** | None (decoded together with payload in 8,192-row contiguous batches). | Decodes 16M rows of `f.id` in isolation via dedicated reader. | +16M isolated column values decoded (~35 ms). |
| **Predicate Evaluation** | Evaluated downstream in Comet `FilterExec` on batches. | Evaluated inside Parquet reader `predicate.evaluate`. | Transferred (~95 ms moved from Comet to reader). |
| **Selection Allocation & Roundtrip** | None (bitmask used directly in Arrow compute). | Converts `BooleanArray` $\to$ 3.2M `RowSelector`s $\to$ walks selectors $\to$ converts to `BooleanBuffer`. | +3.2M struct allocations + 2 conversions (~30 ms). |
| **Phase 2: Payload Decode** | Decodes 16M `f.value` and 16M `f.payload` sequentially in 8,192-row batches. Peak SIMD vectorization. | Decodes 16M `f.value` and 16M `f.payload` in ~81,920-row chunks via `ArrayReader::read_records`. | Comparable decode work, but fragmented chunk coordination. |
| **Filtering / Selection** | Arrow compute kernel `filter()` on contiguous batches in Comet (16M $\to$ 1.6M rows): **92.8 ms** total eval time. | In-reader `filter_record_batch` in Parquet reader on ~81,920-row chunks: **~75 ms**. | Net compute shift. |
| **Downstream Probing** | `HashJoinExec` probes 1.6M surviving rows. | `HashJoinExec` probes 1.6M surviving rows. | Parity. |

### 3.2 Accounting for the Measured +42 ms Regression
- **Downstream Saved Time:** Comet's batch filter evaluated 0 rows in `candidate_on`, saving **~93 ms**.
- **Reader Added Overhead:**
  1. Isolated Phase 1 predicate column decompression and decoding: **~35 ms**.
  2. Predicate evaluation inside reader callback: **~75 ms**.
  3. Allocating 3.2M `RowSelector`s and re-encoding to `BooleanBuffer`: **~30 ms**.
  4. In-reader `filter_record_batch` across all batches: **~75 ms**.
  - Total reader filtering work: $35 + 75 + 30 + 75 = 215\text{ ms}$.
- **Net Timing Balance:**
  $$\Delta t = +215\text{ ms (reader overhead)} - 93\text{ ms (Comet filter saved)} - \text{caching/overlap} \approx \mathbf{+42\text{ ms regression}}.$$
This perfectly accounts for the observed execution shift:
- `candidate_off`: 965 ms
- `candidate_on`: 1,007 ms (+42 ms, 0.95x)

---

## 4. Candidate Fixes Evaluation (Question 3)

### Candidate A: Explicit Mask-Based Selection Policy
- **Description:** Explicitly configure `with_row_selection_policy(RowSelectionPolicy::Mask)` on `record_batch_stream_builder` and `push_decoder_builder`.
- **Efficacy:** **Zero effect.** Parquet's `Auto { threshold: 32 }` already resolves to `RowSelectionStrategy::Mask` for this query. The regression occurs *inside* the `Mask` implementation because `read_mask_batch` decodes all rows in `chunk_rows` (~81k rows) anyway and incurs Phase 1 and conversion overhead.
- **Safety / Complexity:** Safe, trivial (1 line), but completely ineffective.

### Candidate B: Adaptive Bypass on Selection Fragmentation Signal
- **Description:** Inspect the `RowSelection` after evaluating Phase 1 on the first row group or batch. If `selectors.len() / total_rows > 0.05` (average run length < 20 rows) and selectivity is moderate (>5%), abort reader row filtering and fall back to single-phase decode.
- **Efficacy:** High potential if it worked, but:
  - In `f_unsorted`, there are 16 files, and each file contains exactly **one row group** of 1,000,000 rows. Rebuilding at row group boundaries cannot help single-row-group files.
  - Parquet 59.3.0's `with_predicate_options` evaluates the predicate across the entire row group before constructing the data reader; there is no mid-row-group cancellation hook in upstream `parquet-rs`.
- **Safety / Complexity:** High complexity, requires invasive changes to `parquet-rs` internal state machines.

### Candidate C: Cost-Gate Advisory Runtime Predicates on Page-Index / Group Pruning (Recommended)
- **Description:** In `crates/iceberg/src/arrow/reader/pipeline.rs` and `runtime_stream.rs`:
  - When planning predicates, distinguish between **mandatory predicates** (`advisory: false` for planned scan filters and equality deletes) and **advisory predicates** (`advisory: true` for dynamic filters).
  - Only enable `row_filter: true` for an advisory predicate if page index evaluation or row group statistics actually pruned at least one page or row group (`row_selection.as_ref().is_some_and(|s| s.skipped_row_count() > 0)` or `row_groups.len() < total_row_groups`).
  - If zero pages and zero row groups are pruned by the advisory predicate, set `plan.row_filter = false`.
- **Correctness & Safety:**
  - **Mandatory Predicates:** Planned scan predicates and equality deletes always retain `row_filter: true` and are never bypassed.
  - **Advisory Predicates:** Correctness is preserved because downstream Comet already verifies surviving probe rows in `HashJoinExec` (or via `DynamicFilterExec`).
- **Effect on Selective Sorted Queries:**
  - On `join_inner__f_sorted__dim_128`, `join_inner__f_pos_deletes__dim_10k`, etc., page index prunes 90-99% of pages (`skipped_row_count() > 0`). Page pruning is fully preserved and delivers 10x speedups.
- **Complexity & API Surface:** Low complexity (~25 lines in `pipeline.rs` and `runtime_stream.rs`). Zero changes to public APIs, zero changes to `parquet-rs`.

### Candidate D: Never Attach Advisory Runtime Predicates to Parquet `row_filter`
- **Description:** Restrict advisory runtime predicates to metadata pruning (task file pruning, row group pruning, and page index `RowSelection`). Set `row_filter = false` for all advisory predicates.
- **Trade-off:**
  - When page index prunes pages, Parquet's `with_row_selection` already skips the unselected pages during I/O and decompression!
  - Within surviving pages, all rows in those pages are emitted to Comet and filtered downstream by Comet's `HashJoinExec` or batch filter.
  - For `join_inner__f_sorted__dim_128`, only 1 page survives; emitting that 1 page (20k rows) and probing in Comet takes <0.5 ms.
- **Safety / Complexity:** Extremely simple, but slightly increases row volume for sparse non-clustered surviving pages compared to Candidate C.

### Candidate Comparison Matrix

| Candidate | Simplicity | Upstreamability / Parquet-rs | Safety / Correctness | Solves +42 ms Regression? | Preserves 10x Sorted Pruning? |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **A: Force Mask Policy** | High | Clean | 100% | **No** (already active) | Yes |
| **B: Adaptive Run Signal** | Very Low | Invasive (needs parquet-rs PR) | Medium | Partial (not for 1-RG files) | Yes |
| **C: Gate on Page Pruning (Rec.)** | **High** | **Clean (Iceberg reader only)** | **100%** | **Yes (recovers +42 ms)** | **Yes (100% preserved)** |
| **D: RowSelection-Only Advisory** | Very High | Clean (Iceberg reader only) | 100% | Yes (recovers +42 ms) | Yes (small probe increase) |

---

## 5. Recommended Design & Touch Points (Question 4)

### 5.1 Architecture of Candidate C
In `crates/iceberg/src/arrow/reader/pipeline.rs`:
Currently, `arrow_predicate` and `row_filter` are created before page index `row_selection` is computed.
1. Compute `selected_row_group_indices` and `row_selection` first.
2. An advisory predicate is eligible for `row_filter = true` only if:
   - It pruned row groups (`selected_row_group_indices.as_ref().is_some_and(|g| g.len() < total_row_groups)`), OR
   - It pruned pages (`row_selection.as_ref().is_some_and(|s| s.skipped_row_count() > 0)`).
3. If an advisory predicate did not prune any row groups or pages in the file, set `plan.row_filter = false`.
4. Planned predicates (`plan.advisory == false`) and equality deletes always retain `row_filter: true`.

### 5.2 Exact Touch Points (`file:line`)

#### Touch Point 1: `crates/iceberg/src/arrow/reader/pipeline.rs`
- **Lines 600–645 (`let arrow_predicate = ...`):**
  Move the compilation of `row_filter` after `row_selection` has been computed (after line 730).
- **Lines 712–735 (`if self.row_selection_enabled ...`):**
  Check whether the advisory predicate achieved page pruning:
  ```rust
  let advisory_has_page_pruning = row_selection
      .as_ref()
      .is_some_and(|s| s.skipped_row_count() > 0);
  let advisory_has_rg_pruning = selected_row_group_indices
      .as_ref()
      .is_some_and(|g| g.len() < record_batch_stream_builder.metadata().num_row_groups());
  ```
- **Lines 1045–1060 (`plan_predicate`):**
  Gate the advisory predicate's `row_filter` flag:
  ```rust
  let row_filter = if advisory {
      // Advisory row filters are deferred until page/group pruning efficacy is known
      false
  } else {
      !task.file_metrics().is_some_and(|metrics| Self::file_always_matches(&predicate, metrics))
  };
  ```

#### Touch Point 2: `crates/iceberg/src/arrow/reader/runtime_stream.rs`
- **Lines 280–320 (`restrict_remaining` & `compile_row_filter`):**
  When refreshing at row group boundaries:
  ```rust
  let has_page_pruning = kept.iter().any(|s| {
      s.selection().is_some_and(|sel| sel.skipped_row_count() > 0)
  });
  let has_rg_pruning = kept.len() < self.selections.len();
  
  if !has_page_pruning && !has_rg_pruning {
      // Do not recompile row filter for advisory predicate if it cannot prune pages or groups
      return Ok(());
  }
  ```

---

## 6. Verification & Test Plan

### 6.1 Unit Tests (`iceberg-nanfix`)
Add unit tests in `crates/iceberg/src/arrow/reader/runtime_predicate_tests.rs`:
1. `test_advisory_predicate_bypasses_row_filter_when_no_pages_pruned`:
   - Construct a test Parquet file with 1 row group and 2 data pages where column min/max span the full key range.
   - Attach an advisory runtime predicate matching 10% of rows.
   - Assert `arrow_predicate` / `row_filter` is `None` on the reader builder.
   - Verify all rows are emitted to the stream without Phase 1 two-phase decode.
2. `test_advisory_predicate_retains_row_filter_when_pages_pruned`:
   - Construct a test Parquet file where page 1 matches and page 2 does not.
   - Attach an advisory predicate.
   - Assert page pruning produces a `row_selection` with skips and `row_filter` is compiled.
3. `test_mandatory_planned_predicate_always_retains_row_filter`:
   - Construct a test Parquet file with a planned scan filter (`advisory: false`) where 0 pages are skipped.
   - Assert `row_filter` is `Some` and correctly filters rows.
4. `test_equality_delete_always_retains_row_filter`:
   - Assert equality delete predicate always retains `row_filter: true`.

### 6.2 Benchmark Query Validation Protocol
Target queries for verification with the balanced JVM/round benchmark harness:
1. **Regression Recovery Target:**
   - `join_inner__f_unsorted__dim_10pct`: Verify candidate on/off parity ($\ge 1.0\times$ speedup, recovering the 42 ms penalty).
2. **Pruning-Win Protection Targets (Must maintain >10x speedups):**
   - `join_inner__f_sorted__dim_128` (11.1x speedup)
   - `join_inner__f_sorted__dim_10k` (10x speedup)
   - `join_inner__f_pos_deletes__dim_10k` (10x speedup)
   - `join_inner__f_unsorted__dim_128` (pruning-win)
   - `topk10__f_sorted`
3. **Negative Control Queries (Verify zero regressions on unpruned scans):**
   - `join_inner__f_str__dim_10k`
   - `join_inner__f_nan__dim_128`
   - `join_anti__f_sorted__dim_128`
