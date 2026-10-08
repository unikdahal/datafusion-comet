# Adversarial Correctness Review: Iceberg Rust Fast-paths

**Target Commits:**
- `bad79bacb` — *Reuse coalesced Parquet ranges within runtime-pruned row groups*
- `288a0ee9e` — *Skip planned row decoding when file statistics prove every row matches*
- `949cc88ac` — *Avoid physical column decoding for empty output projections*

**Review Date:** October 2026  
**Auditor:** Adversarial Code Review Team (Read-Only Mode)

---

## 1. Executive Summary & Findings Matrix

This review evaluates three performance optimization commits targeting query execution in `iceberg-rust`. The commits introduce range coalescing reuse across push decoder stages, bypass Arrow row filter decoding when whole-file manifest statistics prove all rows match, and switch empty projections (`COUNT(*)`) to Parquet zero-column readers.

While these optimizations deliver substantial I/O and CPU reductions in microbenchmarks, our adversarial evaluation uncovered **one proven defect** causing query result corruption on floating-point data, **one unsafe architectural assumption** regarding metric scope in file splitting, and identified several edge cases requiring defensive hardening.

| ID | Commit | Area / Component | Description | Verdict | Exact Evidence |
|---|---|---|---|---|---|
| **F-01** | `288a0ee9e` | Strict Metrics / NaN Filtering | `not_in` and `not_eq` ignore `nan_value_counts`, returning `ROWS_MUST_MATCH` when `NaN` rows exist, corrupting query results | **`PROVEN-DEFECT`** | [`strict_metrics_evaluator.rs:363-398`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/expr/visitors/strict_metrics_evaluator.rs#L363-L398), [`pipeline.rs:975-996`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/arrow/reader/pipeline.rs#L975-L996) |
| **F-02** | `288a0ee9e` | Metric Scope / File Splits | File-level statistics bypass row filter for byte-range split tasks; safe only under monotonic/uniform invariants across splits | **`UNSAFE-ASSUMPTION`** | [`pipeline.rs:1042-1045`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/arrow/reader/pipeline.rs#L1042-L1045) |
| **F-03** | `288a0ee9e` | Strict Metrics / Truncated Bounds | String/binary lower & upper bounds truncated in Iceberg manifests safely lower/upper bound actual values | **`SAFE`** | [`strict_metrics_evaluator.rs:180-210`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/expr/visitors/strict_metrics_evaluator.rs#L180-L210), [`datum.rs:178-210`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/spec/values/datum.rs#L178-L210) |
| **F-04** | `288a0ee9e` | Schema Evolution / Promotion | Bound type mismatches (`int` vs `long`, precision) correctly disable fast path via `file_bounds_match_predicate` | **`SAFE`** | [`pipeline.rs:927-944`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/arrow/reader/pipeline.rs#L927-L944) |
| **F-05** | `288a0ee9e` | 3-Valued Logic & Deletes | Null semantics safely handled (`null_value_counts == 0` for `!=` and `NOT IN`). Positional and equality deletes correctly preserved | **`SAFE`** | [`pipeline.rs:525-540`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/arrow/reader/pipeline.rs#L525-L540), [`pipeline.rs:724-745`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/arrow/reader/pipeline.rs#L724-L745) |
| **F-06** | `949cc88ac` | Empty Projections / Row Counts | `ProjectionMask::none` preserves row counts with position deletes, deletion vectors, and equality deletes | **`SAFE`** | [`projection.rs:121-125`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/arrow/reader/projection.rs#L121-L125), [`row_filter.rs:44-88`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/arrow/reader/row_filter.rs#L44-L88) |
| **F-07** | `bad79bacb` | Push Decoder Range Reuse | Coalesced buffer reuse is bounded to current row group; cleared on transition and reader extraction | **`SAFE`** | [`runtime_stream.rs:343-363`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/arrow/reader/runtime_stream.rs#L343-L363), [`file_reader.rs:69-90`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/arrow/reader/file_reader.rs#L69-L90) |
| **F-08** | All 3 | Multi-feature Composition | False-positive in `file_always_matches` cascades to empty projection and late runtime filter, returning ghost rows | **`PROVEN-DEFECT`** | Composite interaction of F-01 with [`projection.rs:123`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/arrow/reader/projection.rs#L123) and [`runtime_stream.rs:289`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/arrow/reader/runtime_stream.rs#L289) |

---

## 2. Commit 288a0ee9e: "Skip planned row decoding when file statistics prove every row matches"

### 2.1 The NaN Bypass Vulnerability in `not_in` and `not_eq`
- **Verdict:** `PROVEN-DEFECT`
- **Location:** [`crates/iceberg/src/expr/visitors/strict_metrics_evaluator.rs:363-398`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/expr/visitors/strict_metrics_evaluator.rs#L363-L398), [`crates/iceberg/src/arrow/reader/pipeline.rs:975-996`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/arrow/reader/pipeline.rs#L975-L996)

#### Vulnerability Mechanics
Per the Iceberg Table Specification, `lower_bounds` and `upper_bounds` for float/double columns **exclude NaN values**. When a column contains both finite floats and `NaN`s, `nan_value_counts` records the NaN count.

In `StrictMetricsEvaluator`:
- `visit_inequality` (handling `<`, `<=`, `>`, `>=`), `eq`, and `in` all explicitly guard against NaNs:
  ```rust
  if self.may_contain_null(field_id) || self.may_contain_nan(reference) {
      return ROWS_MIGHT_NOT_MATCH;
  }
  ```
- **However, `not_in` and `not_eq` omit `self.may_contain_nan(reference)`.**
- Furthermore, `pipeline.rs::null_semantics_match` only verifies `null_value_counts == Some(&0)` for `NotEq` and `NotIn`. It completely ignores `nan_value_counts`.

#### Proof of Failure:
1. Consider a column `val: Float` in a Parquet file containing two rows: `[50.0, NaN]`.
2. Manifest statistics:
   - `null_value_counts = {val: 0}`
   - `nan_value_counts = {val: 1}`
   - `lower_bounds = {val: 50.0}`
   - `upper_bounds = {val: 50.0}`
3. User query: `WHERE val NOT IN (10.0, NaN)`.
4. In `pipeline.rs`:
   - `file_bounds_match_predicate` returns `true`.
   - `null_semantics_match` inspects `null_value_counts` (0) and returns `true`.
5. In `StrictMetricsEvaluator::not_in`:
   - `filtered_literals` starts as `{10.0, NaN}`.
   - Lower bound check (`50.0 <= val`):
     - `50.0 <= 10.0` is false -> `10.0` is removed.
     - `50.0 <= NaN`: In `iceberg_float_cmp_f32`, `a.total_cmp(&b)` orders `-NaN < -inf < 0 < +inf < +NaN`. Thus `50.0 <= +NaN` evaluates to `true`! `NaN` is retained.
   - Upper bound check (`val <= 50.0`):
     - `+NaN <= 50.0` is false in `total_cmp` -> `NaN` is removed!
   - `filtered_literals.is_empty()` is now **true**!
   - Line 389 returns `ROWS_MUST_MATCH`.
6. `file_always_matches` returns `true`.
7. `plan_predicate` sets `row_filter = false`.
8. Arrow row filtering is completely bypassed.
9. **Result Corruption:** The row containing `NaN` is emitted in the output, directly violating `WHERE val NOT IN (10.0, NaN)`.

---

### 2.2 Truncated Bounds for String and Binary
- **Verdict:** `SAFE`
- **Location:** [`crates/iceberg/src/expr/visitors/strict_metrics_evaluator.rs:180-210`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/expr/visitors/strict_metrics_evaluator.rs#L180-L210)
- **Analysis:**
  Iceberg writers truncate string and binary bounds to reduce metadata size:
  - Lower bounds are truncated down to a prefix: $L_{trunc} \le L_{actual} \le v_{min}$.
  - Upper bounds are truncated up (incrementing the lowest character or byte): $U_{trunc} > U_{actual} \ge v_{max}$.
  
  For strict evaluation:
  - `col < K` requires $U_{trunc} < K \implies v_{max} < U_{trunc} < K$. All rows strictly match.
  - `col > K` requires $L_{trunc} > K \implies v_{min} \ge L_{trunc} > K$. All rows strictly match.
  - `col == K` requires $L == K \land U == K$. Truncated upper bounds cannot equal truncated lower bounds unless bounds are exact and identical ($v_{min} == v_{max}$).
  - `col != K` requires $L_{trunc} > K$ or $U_{trunc} < K$. If $U_{trunc} < K$, then $v_{max} < K$, so all rows differ from $K$.
  
  Truncation only widens the metric interval $[L_{trunc}, U_{trunc}] \supseteq [v_{min}, v_{max}]$. It can cause safe false negatives (failing to prove all rows match), but cannot cause false positives.

---

### 2.3 SQL 3-Valued Logic (3VL), NOT, !=, NOT IN, IS NOT NULL
- **Verdict:** `SAFE`
- **Location:** [`crates/iceberg/src/arrow/reader/pipeline.rs:975-996`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/arrow/reader/pipeline.rs#L975-L996)
- **Analysis:**
  SQL 3VL requires that comparisons against `NULL` evaluate to `UNKNOWN` (treated as false in `WHERE`).
  - `null_semantics_match` specifically checks `null_value_counts.get(&field_id) == Some(&0)` for `NotEq` and `NotIn`. If any nulls exist, or if null counts are missing (`None`), `null_semantics_match` rejects the fast path.
  - `BoundPredicate::Not(_)` is unconditionally rejected (`BoundPredicate::Not(_) => false`), preventing negation tree discrepancies.
  - `col IS NOT NULL`: Handled by `StrictMetricsEvaluator::not_null`, which strictly requires `null_count == Some(0)`.
  - `col IS NULL`: Handled by `StrictMetricsEvaluator::is_null`, which requires `null_count == value_count`.

---

### 2.4 Missing Stats and Schema Evolution (Added Columns / Defaults)
- **Verdict:** `SAFE`
- **Location:** [`crates/iceberg/src/expr/visitors/strict_metrics_evaluator.rs:77-100`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/expr/visitors/strict_metrics_evaluator.rs#L77-L100)
- **Analysis:**
  When a column is added after data files were written, or when file metrics omit a column:
  - `self.lower_bound(id)` and `self.upper_bound(id)` return `None`.
  - `self.null_count(id)` returns `None`, causing `self.may_contain_null(id)` to return `true`.
  - `StrictMetricsEvaluator` returns `ROWS_MIGHT_NOT_MATCH` for all comparison, equality, set, and nullness checks.
  - The fast path is never triggered for columns with missing metrics. Execution falls back to row decoding where `RecordBatchTransformer` inserts the appropriate default or null values.

---

### 2.5 Type Promotion and Timestamp Timezones
- **Verdict:** `SAFE`
- **Location:** [`crates/iceberg/src/arrow/reader/pipeline.rs:927-944`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/arrow/reader/pipeline.rs#L927-L944)
- **Analysis:**
  `file_bounds_match_predicate` iterates over all referenced fields and asserts:
  ```rust
  field.field_type.as_primitive_type() == Some(bound.data_type())
  ```
  - For promoted types (`int` to `long`, `float` to `double`, `decimal(P1, S)` to `decimal(P2, S)`), the bound's primitive data type does not equal the promoted schema's primitive type.
  - The check returns `false`, preventing `file_always_matches` from comparing mismatched literal types.
  - Timestamps (`Timestamp` vs `Timestamptz`) have distinct `PrimitiveType` variants and are rejected if mixed. Within `Timestamptz`, values are microsecond UTC epochs, matching between bound metrics and Arrow expressions.

---

### 2.6 Whole-File Statistics vs Row-Group / Split Tasks
- **Verdict:** `UNSAFE-ASSUMPTION`
- **Location:** [`crates/iceberg/src/arrow/reader/pipeline.rs:1042-1045`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/arrow/reader/pipeline.rs#L1042-L1045)
- **Analysis:**
  `task.file_metrics()` represents the entire physical Parquet file. When an engine splits a file into multiple byte-range tasks:
  - If a predicate matches all rows in the entire file, it trivially matches all rows in every row group subset within that file.
  - **The unsafe assumption:** This assumes the manifest metrics accurately describe the physical file currently on disk. If external tools appended uncommitted row groups or modified the file without updating manifest metrics, split tasks could execute without the safety net of row verification.
  - Furthermore, `file_always_matches` checks whole-file stats, missing opportunities where individual row-groups could skip row decoding while others require it.

---

### 2.7 Equality and Position Deletes
- **Verdict:** `SAFE`
- **Location:** [`crates/iceberg/src/arrow/reader/pipeline.rs:525-540`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/arrow/reader/pipeline.rs#L525-L540), [`crates/iceberg/src/arrow/reader/pipeline.rs:724-745`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/arrow/reader/pipeline.rs#L724-L745)
- **Analysis:**
  - **Equality Deletes:** Lines 525-540 combine `task.predicate()` and `delete_predicate` via `filter_predicate.and(delete_predicate)`. `file_always_matches` evaluates the conjunction. If it returns `true`, it guarantees that no row in the file matches any equality delete condition.
  - **Position Deletes & Deletion Vectors:** Applied independently at lines 724-745 via `ArrowReader::build_deletes_row_selection`. The resulting `RowSelection` is passed to `record_batch_stream_builder.with_row_selection(row_selection)`. It operates completely outside `row_filter` and is never skipped.

---

## 3. Commit 949cc88ac: "Avoid physical column decoding for empty output projections"

### 3.1 Exact Row Count Preservation & Deletes
- **Verdict:** `SAFE`
- **Location:** [`crates/iceberg/src/arrow/reader/projection.rs:121-125`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/arrow/reader/projection.rs#L121-L125)
- **Analysis:**
  - When `field_ids.is_empty()`, `ProjectionMask::none(num_columns)` is returned to the Parquet reader.
  - In `parquet-rs`, a reader with `ProjectionMask::none` produces `RecordBatch` instances with 0 columns and `num_rows` matching the selected row count.
  - **Equality Deletes:** Handled by `RowFilter`. At [`row_filter.rs:44-88`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/arrow/reader/row_filter.rs#L44-L88), `get_arrow_predicate` creates its own leaf projection mask (`ProjectionMask::leaves(parquet_schema, column_indices)`). Parquet decodes the equality-delete key columns during the filter phase, masks out deleted rows, and emits 0-column batches with the post-delete row count.
  - **Position Deletes / Deletion Vectors:** Handled via `RowSelection`. Page and row skipping occurs prior to batch construction, guaranteeing exact counts.
  - **Pruning & Runtime Filters:** Row-group pruning, page index selections, and runtime filters all apply normally because their column requirements are governed by their respective filter masks rather than the root projection mask.

---

## 4. Commit bad79bacb: "Reuse coalesced Parquet ranges within runtime-pruned row groups"

### 4.1 Coalesced Range Reuse and Buffer Lifecycle
- **Verdict:** `SAFE`
- **Location:** [`crates/iceberg/src/arrow/reader/runtime_stream.rs:343-363`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/arrow/reader/runtime_stream.rs#L343-L363), [`crates/iceberg/src/arrow/reader/file_reader.rs:69-90`](file:///home/unik/Coding/rust/rp-work/iceberg/crates/iceberg/src/arrow/reader/file_reader.rs#L69-L90)
- **Analysis:**
  - `fetch_ranges` merges requested column chunk ranges within `range_coalesce_bytes()` and fetches them concurrently.
  - Pushed ranges are retained by `ParquetPushDecoder` for the duration of the current row group's decoding stages (filter phase through output phase).
  - **Buffer Release / No Memory Leaks:**
    - At line 357, upon reaching `DecodeResult::Data(reader)`, `decoder.clear_all_ranges()` is called immediately, releasing all overfetched gap bytes.
    - At line 348, if the push decoder internally skips a row group (`self.buffered_frontier != Some(frontier)`), `decoder.clear_all_ranges()` is invoked before fetching the next group.
    - At line 324, in `rebuild_decoder`, if the next row group is pruned, `decoder.clear_all_ranges()` is executed.
  - **Error Propagation & Cancellation:**
    - `fetch_ranges` uses `try_collect().await?`. Any network failure immediately aborts with `ParquetError::External`, failing `next_batch` cleanly.
    - Dropping the stream cancels pending futures and frees in-memory buffers without dangling references.

---

## 5. Multi-Feature Interaction Analysis

### Scenario: All-Match Fast Path + Empty Projection + Deletes + Late Runtime Filter
- **Verdict:** `PROVEN-DEFECT` (Cascading from F-01)
- **Execution Trace:**
  1. A query executes `SELECT COUNT(*)` on a table with a planned predicate and a float column containing `NaN`s.
  2. Due to **F-01**, `file_always_matches` returns `true`.
  3. `pipeline.rs` sets `plan.row_filter = false`, and `refresh.planned = None`.
  4. Root projection is `ProjectionMask::none` (Commit `949cc88ac`).
  5. Mid-scan, a late runtime filter arrives at a row-group boundary.
  6. `runtime_stream.rs:289` rebuilds the filter:
     ```rust
     let planned = self.refresh.planned.iter().chain([runtime]);
     ```
     Because `refresh.planned` was dropped during planning, the rebuilt row filter **omits the planned filter entirely**.
  7. Coalesced range reuse (`bad79bacb`) fetches and decodes the runtime filter columns.
  8. Output batches with 0 columns are produced, counting rows that passed the runtime filter but should have been dropped by the planned filter.

---

## 6. Upstreamability Concerns

1. **`bad79bacb` (Coalesced Range Reuse):**
   - *Concern:* High coupling to `parquet::arrow::push_decoder` internal buffering state.
   - *Architectural Risk:* Exposing `fetch_ranges` on `ArrowFileReader` and manually synchronizing `clear_all_ranges()` with `buffered_frontier` leaks decoder-specific coordination into the reader stream. If `parquet-rs` alters push decoder state transitions or internal caching, this could silently break.

2. **`288a0ee9e` (File Statistics Skip):**
   - *Concern:* Premature bypass across abstraction layers.
   - *Architectural Risk:* `ArrowReader` bypasses Parquet's `RowFilter` entirely based on Iceberg manifest metrics. Logic like `null_semantics_match` duplicates evaluation rules that properly belong inside `StrictMetricsEvaluator`. A cleaner upstream architecture would pass the evaluator result into the filter compiler rather than conditionally omitting the filter.

3. **`949cc88ac` (Empty Projections):**
   - *Concern:* None. Minimal, clean, idiomatic integration with `parquet-rs` `ProjectionMask::none`. Highly upstreamable.

---

## 7. Missing Regression Tests

The following test cases are absent and should be added to prevent regressions:

1. **`test_file_always_matches_not_in_with_nan`**
   - *Scenario:* Table with Float32 column containing `[50.0, NaN]`. File metrics have `nan_value_counts: 1`, `null_value_counts: 0`, `lower_bounds: 50.0`, `upper_bounds: 50.0`. Query: `WHERE col NOT IN (10.0, NaN)`.
   - *Expected Result:* `file_always_matches` returns `false`; row filter decodes column and drops the `NaN` row; returns exactly 1 row.
   - *Current Bug:* Fast path skips decoding and returns 2 rows.

2. **`test_file_always_matches_not_eq_with_nan`**
   - *Scenario:* Float64 column with `nan_value_counts > 0` queried with `WHERE col != 10.0` and `WHERE col != NaN`.
   - *Expected Result:* Fast path is rejected unless `nan_value_counts == 0`, preserving IEEE 754 / SQL NaN comparison semantics.

3. **`test_empty_projection_with_equality_and_position_deletes`**
   - *Scenario:* `COUNT(*)` query (`project_field_ids = []`) on a file with both equality delete files and positional delete files.
   - *Expected Result:* Parquet push decoder decodes equality delete columns via row filter, applies positional delete selections, and emits exact net row count.

4. **`test_rebuild_decoder_late_runtime_filter_with_empty_projection`**
   - *Scenario:* Empty projection query where a late runtime filter arrives after row group 0.
   - *Expected Result:* Push decoder compiles row filter for runtime predicate, fetches coalesced ranges, evaluates filter on 0-column output, and releases buffers on boundary.

5. **`test_file_always_matches_type_promoted_int_to_long`**
   - *Scenario:* Table where column evolved from `int` to `long`. Older file manifest has `int` lower/upper bounds.
   - *Expected Result:* `file_bounds_match_predicate` rejects mismatched bounds and falls back to row decoding.
