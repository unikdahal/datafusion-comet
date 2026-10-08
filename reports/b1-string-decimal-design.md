# Design: Extending Runtime Join-Key Pruning to STRING and DECIMAL Keys

## Overview

This technical design specifies the architecture, correctness rules, and implementation plan for extending runtime dynamic filter join-key pruning to `STRING` and `DECIMAL` keys across Apache Comet and `iceberg-rust`.

Currently, Comet and `iceberg-rust` support runtime dynamic filtering for signed integer (`Int8`, `Int16`, `Int32`, `Int64`), `Date32`, and microsecond `Timestamp` keys. When selective joins occur on string or decimal keys, the fact table reader falls back to unfiltered scans, decoding tens of millions of irrelevant rows and gigabytes of Parquet data. By lifting type gates safely, selective joins on string and decimal columns can achieve the same order-of-magnitude I/O and latency reductions observed on integer keys.

---

## 1. Current Flow and Touch Points

### 1.1 End-to-End Dynamic Filter Lifecycle

```
Build Side (Dimension)                 Probe Side (Iceberg Fact Table)
----------------------                 -------------------------------
HashJoinExec (Build Phase)
         │
         ▼
DynamicFilterPhysicalExpr ───────────► IcebergRuntimePredicateProvider
  (Accumulates min/max & IN-list)        │ (Polls physical expr generation)
                                         ▼
                                       extract_iceberg_predicate()
                                         │ (PhysicalExpr AST -> Iceberg Predicate)
                                         ▼
                                       iceberg::arrow::reader::RuntimePredicates
                                         │ (Binds predicate to Table Schema)
                                         ▼
                                       iceberg::arrow::reader::pipeline
                                         ├── InclusiveMetricsEvaluator (File pruning)
                                         ├── InclusiveMetricsEvaluator (Row-group pruning)
                                         └── StrictMetricsEvaluator (Row-filter bypass)
```

1. **Build-Side Collection**: When `DynamicFilterJoinExec` executes, the build side populates a `DynamicFilterPhysicalExpr` containing the join key bounds (min, max) and, when cardinality permits, an exact set of distinct build keys (`IN (...)` list).
2. **Snapshot Extraction**: On the probe side, `IcebergRuntimePredicateProvider` periodically polls `DynamicFilterPhysicalExpr::current()` via its generation counter. If updated, `extract_iceberg_predicate` translates DataFusion's physical expression tree into an `iceberg::expr::Predicate`.
3. **Table Schema Binding**: The predicate is passed into `RuntimePredicates::current()`, where `iceberg::expr::visitors::manifest_evaluator` and schema binding associate field IDs and validate types against the Iceberg table schema.
4. **Three-Tier Pruning in Reader Pipeline**:
   - **File Task Pruning**: `InclusiveMetricsEvaluator` evaluates partition/manifest file metrics (`lower_bounds`, `upper_bounds`, `null_value_counts`) to drop non-overlapping data files.
   - **Row Group Pruning**: Parquet file metadata row-group column chunks are inspected to skip non-overlapping row groups before disk I/O.
   - **Row Filtering & Skipping**: `StrictMetricsEvaluator` checks if all rows in a row group must match. If so, decoding filters are skipped (`file_always_matches`); otherwise, an Arrow `RowFilter` evaluates surviving rows.

### 1.2 Exact Touch Points and Type Gates

The system currently enforces type gates at two specific touch points in Comet:

| Component | File & Line | Current Gate Logic | Rejection Behavior |
| :--- | :--- | :--- | :--- |
| **Comet Dynamic Filter Join** | [`native/core/src/execution/operators/dynamic_filter/join.rs:348-357`](file:///home/unik/Coding/rust/rp-work/comet/native/core/src/execution/operators/dynamic_filter/join.rs#L348-L357) | `build_type != probe_type \|\| !matches!(build_type, DataType::Int8 \| DataType::Int16 \| DataType::Int32 \| DataType::Int64 \| DataType::Date32 \| DataType::Timestamp(TimeUnit::Microsecond, _))` | Returns `Some("requires matching integer, date or timestamp keys")`. Disables dynamic filter injection. |
| **Comet Predicate Extractor** | [`native/core/src/execution/operators/dynamic_filter/iceberg_reader/predicate.rs:204-222`](file:///home/unik/Coding/rust/rp-work/comet/native/core/src/execution/operators/dynamic_filter/iceberg_reader/predicate.rs#L204-L222) | `fn scalar_to_datum(value: &ScalarValue) -> Option<Datum>` handles only `Int8`, `Int16`, `Int32`, `Int64`, `Date32`, `TimestampMicrosecond`. | Returns `None` for `Utf8`, `LargeUtf8`, `Utf8View`, and `Decimal128`. Causes `extract_iceberg_predicate` to fail translation. |
| **Iceberg Reader Semantics** | [`crates/iceberg/src/arrow/reader/runtime_predicate.rs:286-302`](file:///home/unik/Coding/rust/rp-work/iceberg-nanfix/crates/iceberg/src/arrow/reader/runtime_predicate.rs#L286-L302) | `check_runtime_predicate_semantics` rejects `PrimitiveType::Float` and `PrimitiveType::Double`. | String and Decimal are **not** rejected by `check_runtime_predicate_semantics`, meaning `iceberg-rust` already permits them once bound. |
| **Spark Plan Serde Collation Guard** | [`spark/src/main/scala/org/apache/spark/sql/comet/operators.scala:2797-2800`](file:///home/unik/Coding/rust/rp-work/comet/spark/src/main/scala/org/apache/spark/sql/comet/operators.scala#L2797-L2800) | `if (joinKeys.exists(key => isStringCollationType(key.dataType)))` | Disables native join offload entirely if keys carry non-default string collations (`withFallbackReason(..., "unsupported non-default collated string join keys")`). |

### 1.3 Historical Context & Why Gates Were Placed

1. **Initial Scope Limitation**: As revealed by source history (commit `b3e8e796e6`), dynamic filters were initially implemented strictly for signed integer types. That commit subsequently extended eligibility to `Date32`, microsecond `Timestamp`, and semi-joins. Non-integer numeric types and strings were deferred because:
   - DataFusion's in-list and min/max pushdown kernels originally had variable coverage across nested and variable-width types.
   - Parquet native reader pushdown (`parquet_reader.rs:43-52`) was only built and tested for signed integers (`Int8` through `Int64`).
2. **Decimal Datum Construction**: In `iceberg-rust`, `Datum` only exposed constructors accepting `rust_decimal::Decimal` (which caps precision at 28 digits) or parsed string representations. Direct translation of 38-precision Arrow `Decimal128` mantissas lacked a public zero-allocation constructor in `iceberg-rust`.
3. **String Safety Concerns**: String bounds truncation in Iceberg metadata and collation differences in Spark introduced subtle correctness hazards that required dedicated design before widening.

---

## 2. Correctness Hazards and Safe Rules

### 2.1 Hazard 1: Iceberg String Bounds Truncation & Strict Evaluator Safety

* **Hazard**: Iceberg writers truncate string lower and upper bounds in manifest files to conserve storage (governed by table properties such as `write.summary.string-truncation-length`, default 16 bytes).
  - **Lower bound truncation**: Truncated downwards (e.g., `"apple"` truncated to `"app"`), ensuring `stored_lower <= actual_min`.
  - **Upper bound truncation**: Truncated upwards to the lowest string of equal length greater than the prefix (e.g., `"banana"` truncated to 3 bytes becomes `"ban"` incremented to `"bao"`), ensuring `stored_upper >= actual_max`.
  - *The Danger*: In `InclusiveMetricsEvaluator` ([`inclusive_metrics_evaluator.rs:161-192`](file:///home/unik/Coding/rust/rp-work/iceberg-nanfix/crates/iceberg/src/expr/visitors/inclusive_metrics_evaluator.rs#L161-L192)), conservative bounds are safe: if the literal falls outside `[stored_lower, stored_upper]`, no row can match. However, in `StrictMetricsEvaluator` ([`strict_metrics_evaluator.rs:240-275`](file:///home/unik/Coding/rust/rp-work/iceberg-nanfix/crates/iceberg/src/expr/visitors/strict_metrics_evaluator.rs#L240-L275)), which feeds `pipeline.rs:1022` (`file_always_matches`), proving that *all* rows match is dangerous:
    - If `stored_upper` is artificially incremented, testing `stored_upper <= literal` or `stored_lower == stored_upper == literal` can yield false positives or false negatives. If `file_always_matches` returns `true`, the reader skips the row filter entirely, emitting non-matching rows!
* **Safe Rule**:
  1. `InclusiveMetricsEvaluator` continues to be used for file and row-group pruning with string bounds; conservative bounds never cause false dismissals.
  2. In `StrictMetricsEvaluator` (or in `pipeline.rs:file_always_matches`), string predicates must **never** be used to prove `file_always_matches = true` unless bounds are proven untruncated. Specifically, for runtime string predicates, `file_always_matches` must conservatively return `false`, ensuring that survivor row groups always execute the decoded Arrow row-level filter.

### 2.2 Hazard 2: Byte-Wise vs. Spark Collation

* **Hazard**: Spark 4.0+ supports pluggable string collations (e.g., `UTF8_LCASE`, ICU locale collations). In non-binary collations, equality and order differ fundamentally from binary comparisons (e.g., `'a' == 'A'`, or accented character equivalence).
  - Iceberg metadata bounds and Parquet statistics are strictly byte-ordered (`UTF8_BINARY`).
  - DataFusion dynamic filters compare raw byte arrays.
  - If a join on a case-insensitive or locale-collated column pushed a binary dynamic filter to the reader, rows that match under the collation would be dropped during file or row-group pruning.
* **Safe Rule**:
  1. **Spark Serde Verification**: Rely on Comet's existing collation guard in [`operators.scala:2797`](file:///home/unik/Coding/rust/rp-work/comet/spark/src/main/scala/org/apache/spark/sql/comet/operators.scala#L2797):
     ```scala
     val joinKeys = join.leftKeys ++ join.rightKeys
     if (joinKeys.exists(key => isStringCollationType(key.dataType))) {
       withFallbackReason(join, "unsupported non-default collated string join keys")
       return None
     }
     ```
     Any join involving non-default collations is rejected before reaching native execution.
  2. **Native Defensive Gate**: In native `join.rs`, verify that string join keys have pure Arrow `DataType::Utf8`, `DataType::LargeUtf8`, or `DataType::Utf8View` with no collation extension metadata attached. If any collation metadata is present, reject with `"non-binary string collation is not supported"`.

### 2.3 Hazard 3: Utf8View, Dictionary Encoding, Char Padding, Empty Strings, Null Keys

* **Utf8View and LargeUtf8**:
  - DataFusion may represent string arrays as `Utf8`, `Utf8View`, or `LargeUtf8`.
  - In `scalar_to_datum`, map `ScalarValue::Utf8(Some(s))`, `ScalarValue::LargeUtf8(Some(s))`, and `ScalarValue::Utf8View(Some(s))` to `Datum::string(s)`.
  - When comparing probe key types in `ineligible_reason`, permit matching between `Utf8` and `Utf8View` only if physical expression comparison kernels support cross-view comparisons, or require exact matching `build_type == probe_type`.
* **Dictionary-Encoded Keys**:
  - In Iceberg scans, dictionary-encoded string columns (`DataType::Dictionary(Int32, Utf8)`) may be emitted.
  - DataFusion's dynamic filter evaluation handles dictionary arrays seamlessly via Arrow compute kernels.
  - In build-side extraction, dictionary values unpack into scalar `Utf8` literals when building the IN-list.
* **CHAR Padding**:
  - Spark's `CharType(N)` semantics pad strings with trailing whitespace (`'foo ' == 'foo'`).
  - Iceberg does not pad. If a `CHAR(N)` column is compared against `VARCHAR`/`STRING`, byte-wise pruning is unsafe without space trimming.
  - *Safe Rule*: Reject dynamic filters if the Spark logical key expression is `CharType` or if length-padded comparison rules apply.
* **Empty Strings**:
  - An empty string `""` has length 0 and is the minimum non-null value in UTF-8 binary collation.
  - `Datum::string("")` is valid in Iceberg. Slicing logic during manifest evaluation must guard against zero-length indexing.
* **Null Keys**:
  - Spark equi-joins have `NullEqualsNothing` semantics.
  - Build-side `NULL` values can never match any probe row. DataFusion's `DynamicFilterPhysicalExpr` strips nulls when creating min/max and IN-lists. If the build side contains only `NULL`s, the filter becomes empty (`false`), pruning all probe data files.

### 2.4 Hazard 4: Decimal Precision, Scale Mismatch, Casting, and Rounding

* **Hazard**:
  - Decimal values in Arrow are represented by `Decimal128(precision, scale)`. Two decimals with different scales represent different values even if their unscaled `i128` mantissas match (e.g., mantissa `100` with scale 2 is `1.00`, but with scale 1 is `10.0`).
  - If Spark casts a build or probe key (e.g., from `Decimal(10, 2)` to `Decimal(18, 2)` or `Decimal(10, 3)`), rounding or overflow can occur.
  - Iceberg's `PrimitiveType::Decimal { precision, scale }` requires that any predicate `Datum` match the field's exact scale.
* **Safe Rule**:
  1. **Strict Scale and Precision Equality**: In `ineligible_reason`, require `build_type == probe_type`. Both sides must be `DataType::Decimal128(p, s)` where `p_build == p_probe` and `s_build == s_probe`.
  2. **Reject Computed Keys & Casts**: If Spark inserted a `Cast` expression to widen one side, `build_key.is::<Column>()` or `probe_key.is::<Column>()` evaluates to `false`, naturally rejecting computed join keys with `"computed join keys are not supported"`.
  3. **No Rescaling or Rounding**: In `scalar_to_datum`, map `ScalarValue::Decimal128(Some(val), precision, scale)` directly using the unscaled `val: i128` along with the exact `precision` and `scale`. Never allow floating-point conversions or implicit rescaling.

### 2.5 Hazard 5: Join Type Semantics and Null Equality

* **Hazard**: Outer joins (LeftOuter, RightOuter, FullOuter) and Anti joins emit probe rows that have *no* matching build key. Pruning probe rows that lack build matches would silently eliminate output rows.
* **Safe Rule**:
  - Retain the strict join type validation in [`join.rs:324-329`](file:///home/unik/Coding/rust/rp-work/comet/native/core/src/execution/operators/dynamic_filter/join.rs#L324-L329):
    ```rust
    if !matches!(
        join.join_type(),
        JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
    ) || join.null_equality() != NullEquality::NullEqualsNothing {
        return Ok(Some("only inner and semi equijoins are supported"));
    }
    ```
  - The build side is always the left input, and the probe side is the right input. Probe pruning is safe if and only if unmatched probe rows are discarded by the join.

---

## 3. Minimal Implementation Plan

The extension is partitioned into two sequential, independently reviewable phases: **Decimal first** (lower risk, fixed-width, no collation or bounds-truncation hazards), followed by **String** (handling truncation and collation defenses).

```
Phase 1: Decimal Keys
├── Step 1.1: iceberg-rust: Expose Datum::decimal_from_i128 constructor
├── Step 1.2: Comet: Extend scalar_to_datum for Decimal128
├── Step 1.3: Comet: Widen ineligible_reason for Decimal128
└── Step 1.4: Tests: Unit tests & Spark SQL decimal join test suite

Phase 2: String Keys
├── Step 2.1: iceberg-rust: Guard StrictMetricsEvaluator against truncated string bounds
├── Step 2.2: Comet: Extend scalar_to_datum for Utf8, LargeUtf8, Utf8View
├── Step 2.3: Comet: Widen ineligible_reason for Utf8 types
└── Step 2.4: Tests: Truncation, UTF8_BINARY, empty string, and Unicode test suite
```

### 3.1 Phase 1: Decimal Support

#### Step 1.1: `iceberg-rust` Datum Constructor for Decimal128
* **Touch Point**: [`crates/iceberg/src/spec/values/datum.rs`](file:///home/unik/Coding/rust/rp-work/iceberg-nanfix/crates/iceberg/src/spec/values/datum.rs) and [`decimal_utils.rs`](file:///home/unik/Coding/rust/rp-work/iceberg-nanfix/crates/iceberg/src/spec/values/decimal_utils.rs).
* **Change**: Expose a public constructor on `Datum`:
  ```rust
  impl Datum {
      pub fn decimal_from_i128(mantissa: i128, precision: u32, scale: u32) -> Result<Self> {
          Self::decimal_from_mantissa(mantissa, precision, scale)
      }
  }
  ```
* **Tests**: Unit tests in `crates/iceberg/src/spec/values/tests.rs` constructing positive, negative, zero, and maximum 38-digit decimals from `i128`.

#### Step 1.2: Comet Predicate Extractor for Decimal128
* **Touch Point**: [`comet/native/core/src/execution/operators/dynamic_filter/iceberg_reader/predicate.rs:204-222`](file:///home/unik/Coding/rust/rp-work/comet/native/core/src/execution/operators/dynamic_filter/iceberg_reader/predicate.rs#L204-L222).
* **Change**: Add `ScalarValue::Decimal128` branch:
  ```rust
  ScalarValue::Decimal128(Some(val), precision, scale) => {
      let p = u32::from(*precision);
      let s = u32::try_from(*scale).ok()?;
      Datum::decimal_from_i128(*val, p, s).ok()
  }
  ```
* **Tests**: Unit tests in `predicate/tests.rs` verifying IN-list and range translation of `ScalarValue::Decimal128` to `iceberg::expr::Predicate`.

#### Step 1.3: Comet Join Eligibility Gate for Decimal128
* **Touch Point**: [`comet/native/core/src/execution/operators/dynamic_filter/join.rs:348-357`](file:///home/unik/Coding/rust/rp-work/comet/native/core/src/execution/operators/dynamic_filter/join.rs#L348-L357).
* **Change**:
  ```rust
  if build_type != probe_type
      || !matches!(
          build_type,
          DataType::Int8
              | DataType::Int16
              | DataType::Int32
              | DataType::Int64
              | DataType::Date32
              | DataType::Timestamp(TimeUnit::Microsecond, _)
              | DataType::Decimal128(_, _)
      )
  {
      return Ok(Some("requires matching integer, date, timestamp or decimal keys"));
  }
  ```
  Ensure the Iceberg reader requirement check at line 360 admits `Decimal128`.
* **Tests**: Integration tests in `join/tests.rs` verifying dynamic filter attachment when join key is `Decimal128(18, 2)`.

---

### 3.2 Phase 2: String Support

#### Step 2.1: `iceberg-rust` Strict Evaluator Truncated Bounds Guard
* **Touch Point**: [`crates/iceberg/src/expr/visitors/strict_metrics_evaluator.rs:240-275`](file:///home/unik/Coding/rust/rp-work/iceberg-nanfix/crates/iceberg/src/expr/visitors/strict_metrics_evaluator.rs#L240-L275) and [`crates/iceberg/src/arrow/reader/pipeline.rs:1020-1025`](file:///home/unik/Coding/rust/rp-work/iceberg-nanfix/crates/iceberg/src/arrow/reader/pipeline.rs#L1020-L1025).
* **Change**: In `StrictMetricsEvaluator`, add a safety check that returns `false` for `visit_inequality` and `visit_in` if the target field is `PrimitiveType::String` and the metadata bounds do not guarantee exact boundaries. In `pipeline.rs:file_always_matches`, ensure that dynamic string predicates do not trigger row-filter bypass.
* **Tests**: Unit tests in `strict_metrics_evaluator.rs` verifying that truncated string upper bounds do not return `true` for all-match.

#### Step 2.2: Comet Predicate Extractor for Strings
* **Touch Point**: [`comet/native/core/src/execution/operators/dynamic_filter/iceberg_reader/predicate.rs:204-222`](file:///home/unik/Coding/rust/rp-work/comet/native/core/src/execution/operators/dynamic_filter/iceberg_reader/predicate.rs#L204-L222).
* **Change**:
  ```rust
  ScalarValue::Utf8(Some(s))
  | ScalarValue::LargeUtf8(Some(s))
  | ScalarValue::Utf8View(Some(s)) => Some(Datum::string(s.as_str())),
  ```
* **Tests**: Unit tests for ASCII, Unicode, empty string `""`, and multi-byte UTF-8 literals.

#### Step 2.3: Comet Join Eligibility Gate for Strings
* **Touch Point**: [`comet/native/core/src/execution/operators/dynamic_filter/join.rs:348-357`](file:///home/unik/Coding/rust/rp-work/comet/native/core/src/execution/operators/dynamic_filter/join.rs#L348-L357).
* **Change**:
  ```rust
  if build_type != probe_type
      || !matches!(
          build_type,
          DataType::Int8
              | DataType::Int16
              | DataType::Int32
              | DataType::Int64
              | DataType::Date32
              | DataType::Timestamp(TimeUnit::Microsecond, _)
              | DataType::Decimal128(_, _)
              | DataType::Utf8
              | DataType::LargeUtf8
              | DataType::Utf8View
      )
  {
      return Ok(Some("requires matching integer, date, timestamp, decimal or string keys"));
  }
  ```
  And update line 360 to require a native Iceberg probe for strings:
  ```rust
  if !is_parquet_reader_key(probe_key, &join.right().schema())
      && !reaches_iceberg_reader(join.right())
  {
      return Ok(Some(
          "date, timestamp, decimal and string keys require a native Iceberg probe",
      ));
  }
  ```
* **Tests**: Integration tests in `join/tests.rs` checking filter attachment on `Utf8` keys and rejection on non-Iceberg probes.

---

### 3.3 Test Matrix

| Category | Test Case | Target Assertion |
| :--- | :--- | :--- |
| **Decimal** | Exact Scale Match (`Decimal(18, 2) == Decimal(18, 2)`) | Filter attached, row groups pruned, exact match results |
| **Decimal** | Scale Mismatch (`Decimal(18, 2)` vs `Decimal(18, 3)`) | Type gate rejects (`build_type != probe_type`), query falls back safely |
| **Decimal** | Precision Mismatch (`Decimal(10, 2)` vs `Decimal(18, 2)`) | Rejected without failure; no incorrect results |
| **Decimal** | Maximum Precision (38 digits, large mantissa) | Exact `Datum::decimal_from_i128` preserved without truncation |
| **Decimal** | Negative Decimals & Zero | Correct min/max range boundaries in manifest evaluator |
| **String** | Selective Strings (`dim_128` on `f_str`) | File task pruning cuts scanned splits from 26 to 2; exact row count |
| **String** | Truncated Bounds | Row filter retained; no false-match rows emitted |
| **String** | Empty String `""` | Correctly pruned or matched; no bounds panic |
| **String** | Unicode & Emoji (4-byte UTF-8) | Binary comparison matches Iceberg Datum and Parquet stats |
| **String** | Non-default Collation (`UTF8_LCASE`) | Serde gate in `operators.scala:2797` rejects offload; falls back to Spark |
| **String** | `Utf8View` probe with `Utf8` build | Rejected unless types match exactly; safe fallback |
| **General** | LeftOuter / RightOuter / LeftAnti Joins | `ineligible_reason` rejects join type; probe rows preserved |
| **General** | All-NULL Build Table | Filter becomes empty; probe scan pruned completely |

---

## 4. Expected Benefit Analysis

### 4.1 Affected Benchmark Queries

Evidence from benchmark analysis (`reports/rca/join.md` and `reports/rca/strjoin.md`) confirms that all queries joining on string or decimal fact tables are currently classified as regressions or under-deliveries strictly because type gates reject dynamic filter pushdown:

1. **Decimal Benchmark Queries** (`reports/rca/join-ledger.csv`):
   - `join_inner__f_dec__dim_128`: Currently scans **617.32 MB** (20 data files, 26 splits), emitting 16,000,000 fact rows to join.
   - `join_inner__f_dec__dim_10k`: Currently scans **617.37 MB**, emitting 16,010,000 fact rows.
   - `join_inner__f_dec__dim_128_spread`: Currently scans **617.32 MB**, emitting 16,000,000 fact rows.
2. **String Benchmark Queries** (`reports/rca/join-ledger.csv` & `strjoin_ledger.csv`):
   - `join_inner__f_str__dim_128`: Currently scans **588.73 MB** (16 data files, 22 splits), emitting 16,000,000 fact rows.
   - `join_inner__f_str__dim_10k`: Currently scans **588.75 MB**, emitting 16,010,000 fact rows.
   - `join_inner__f_str__dim_128_spread`: Currently scans **588.73 MB**, emitting 16,000,000 fact rows.
   - `join_inner__f_eq_deletes_str__dim_*`: Fact table with equality delete files scanning full data without pruning.
   - **42 queries** across the `strjoin_str_*` suite (covering sorted/unsorted keys, dictionary encoding, high/low selectivity, and prefix variations).

### 4.2 Projected I/O and Runtime Reductions

Based on the verified performance of identical integer queries (`join_inner__f_sorted__dim_128`, where dynamic filtering pruned 15 of 16 data files and 95%+ of row groups):

| Metric | Current Baseline (Gates Active) | Projected with Pruning (Gates Lifted) | Expected Reduction |
| :--- | :--- | :--- | :--- |
| **Selective Data Files Scanned** (`dim_128`) | 16–20 files | 1–2 files | **87% – 95% reduction** |
| **Fact Table Bytes Scanned** | 588 – 617 MB | 30 – 45 MB | **>93% I/O reduction** |
| **Rows Emitted to Hash Join** | 16,000,000 rows | ~100,000 – 1,000,000 rows | **93% – 99% reduction** |
| **Query Latency** (`dim_128`) | ~750 – 780 ms | ~80 – 150 ms | **5x – 10x speedup** |
| **Equality Delete Application** | Evaluated on all 16M rows | Evaluated only on surviving row groups | **Drastic compute saving** |

---

## 5. Upstreamability and Component Boundaries

To allow clean, independent PR reviews that align with upstream repository standards:

### 5.1 What Belongs in `iceberg-rust`
1. **Public `Datum::decimal_from_i128` Constructor**:
   - Upstream `iceberg-rust` already supports `PrimitiveType::Decimal` up to 38 digits via `fastnum::D128`.
   - Exposing `Datum::decimal_from_i128(mantissa: i128, precision: u32, scale: u32) -> Result<Self>` is a natural, zero-cost API addition with no Comet dependencies.
2. **Bounds Truncation Safety in Evaluators**:
   - `InclusiveMetricsEvaluator` already operates safely on truncated bounds.
   - Ensuring `StrictMetricsEvaluator` does not falsely prove strict matches when string bounds are truncated is a general bug fix and hardening improvement for `iceberg-rust`.

### 5.2 What Belongs in Apache Comet
1. **Physical Join Eligibility Expansion** (`join.rs`):
   - Widen `ineligible_reason` to accept `DataType::Decimal128` and `DataType::Utf8` / `DataType::LargeUtf8` / `DataType::Utf8View` when probe targets native Iceberg scans.
2. **Predicate Extraction** (`predicate.rs`):
   - Map `ScalarValue::Decimal128` and `ScalarValue::Utf8*` to `iceberg::spec::Datum`.
3. **Collation Defenses**:
   - Native verification that string columns do not carry collation extension metadata.
4. **No Internal Workarounds**:
   - With the public `Datum::decimal_from_i128` constructor in `iceberg-rust`, Comet requires no private struct access or string-formatting hacks.

---

## Conclusion

Extending runtime join-key pruning to string and decimal types resolves the final major coverage gap in Comet's native dynamic filtering architecture. With strict adherence to scale matching, collation guards, and bounds-truncation protections, this design ensures 100% result accuracy while unlocking up to 10x query acceleration on string and decimal join workloads.
