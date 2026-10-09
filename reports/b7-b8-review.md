# Adversarial Correctness Review: Branches B7 and B8

**Review Date**: October 9, 2026  
**Auditor**: Antigravity (Strict Read-Only Adversarial Audit)  
**Target Branches**:
- **Branch A (B7)**: `adaptive-ci/ws-b7-string-topk` (Head: `5608eab25`, Base: `007080ae0`)
- **Branch B (B8)**: `adaptive-ci/ws-b8-file-stats-dec-str` (Head: `bba05c754`, Stacked on `1ca825a4d`)

---

## Executive Summary & Verdicts

| Branch | Description | Verdict | Proven Defects | Unsafe Assumptions | Nits |
|---|---|---|:---:|:---:|:---:|
| **Branch A (B7)** | String Top-K and MIN/MAX dynamic filter pruning on native Iceberg scan | **APPROVE** | 0 | 0 | 1 |
| **Branch B (B8)** | Driver-side file-level statistics for Decimal and String join keys | **APPROVE** | 0 | 0 | 1 |

Both branches demonstrate meticulous attention to format specifications, edge cases, fail-open semantics, and subtle cross-engine ordering invariants. No proven defects or unsafe assumptions were identified.

---

## Detailed Audit: Branch A (B7 `ws-b7-string-topk`)

### 1. Byte-Wise Ordering Consistency
**Status: VERIFIED SAFE**
- **Arrow UTF-8**: Arrow compares string arrays byte-by-byte lexicographically as unsigned 8-bit integers (`arrow_ord::cmp`).
- **DataFusion Top-K**: DataFusion's `SortExec` and threshold tracking use standard Arrow row/array comparison kernels, evaluating UTF-8 bytes unsigned lexicographically.
- **Iceberg `Datum::string`**: In `crates/iceberg/src/spec/values/datum.rs:249-253`, string literals compare via Rust's `String::partial_cmp` / `&str::cmp`, which performs unsigned byte-by-byte lexicographic comparison (`[u8]::cmp`).
- **Parquet `BYTE_ARRAY` Statistics**: The Parquet format specification mandates unsigned lexicographic ordering for `BYTE_ARRAY` binary and string statistics (`ColumnOrder::TYPE_DEFINED_ORDER`).
- **Cross-Engine Invariant**: All four evaluation layers share identical byte-level ordering semantics for valid UTF-8 strings.
- **Verification**: `predicate.rs:365` (`arrow_and_iceberg_strings_share_unsigned_byte_order`) verifies that Arrow UTF-8/LargeUtf8/Utf8View/Dictionary comparisons precisely match Iceberg `Datum::string` ordering across ASCII, multi-byte Unicode, emojis, and shared prefixes.

### 2. Truncated Bounds & Inexact Parquet Statistics
**Status: VERIFIED SAFE**
- **Truncated Bounds Semantics**:
  - Iceberg manifest lower bounds are truncated prefixes ($\le \text{true\_min}$).
  - Iceberg manifest upper bounds are truncated prefixes incremented by 1 ($\ge \text{true\_max}$).
- **Inclusive-Only Pruning**:
  - In `crates/iceberg/src/expr/visitors/inclusive_metrics_evaluator.rs`, `row_group_metrics_evaluator.rs`, and `page_index_evaluator.rs`:
    - For `col < datum`: Evaluator checks `min_value < datum`. A row group/file is pruned only if `min_value >= datum`. Because `lower_bound <= true_min`, `lower_bound >= datum \implies true_min >= datum`. Zero false rejections.
    - For `col > datum`: Evaluator checks `max_value > datum`. A row group/file is pruned only if `max_value <= datum`. Because `upper_bound >= true_max`, `upper_bound <= datum \implies true_max <= datum`. Zero false rejections.
    - For `col = datum`: Pruned only if `lower_bound > datum` or `upper_bound < datum`. Sound.
- **Strict Shortcut Prohibition**:
  - In `crates/iceberg/src/arrow/reader/pipeline.rs:1225-1232`, `row_filter_semantics_match` explicitly returns `false` for `PrimitiveType::String`.
  - As a result, `file_always_matches` returns `false` for all string predicates; the strict shortcut (bypassing the row filter because file stats prove all rows match) is disabled. Surviving rows always execute decoded record-level filtering.
- **Parquet Inexact Max Statistics (`is_max_value_exact = false`)**:
  - Per Parquet specification, when max values are truncated, they are incremented to remain an upper bound ($\ge \text{true\_max}$), or omitted entirely.
  - In `RowGroupMetricsEvaluator` and `PageIndexEvaluator`, `max_value` is only used to prune queries where `max_value < datum` or `max_value <= datum`. An inexact max value ($\ge \text{true\_max}$) is strictly conservative (may retain more pages, but will never drop matching rows).

### 3. Null Ordering & MIN/MAX Aggregates
**Status: VERIFIED SAFE**
- **`NULLS FIRST` Strict Rejection**:
  - Spark Planner (`CometLocalTopKExec.scala:58`): Requires `order.nullOrdering == NullsLast` for string pruning types.
  - Native Engine (`topk.rs:81`): Explicitly rejects `sort.expr()[0].options.nulls_first` for string keys (`return Ok(None)`).
  - Both JVM and native layers fail closed. Tested in `CometIcebergNativeSuite.scala:2074`.
- **`NULLS LAST` Handling**:
  - Top-K ASC with `NULLS LAST`: Emits `key < threshold`. Iceberg translates to `Reference::less_than` (or `<=`). Because nulls sort last, they cannot appear in the top-K minimum elements; pruning null rows is correct.
  - Top-K DESC with `NULLS LAST`: Emits `key > threshold`. Iceberg translates to `Reference::greater_than` (or `>=`). Nulls cannot appear in top-K maximum elements; pruning null rows is correct.
- **MIN/MAX Over Strings with Nulls**:
  - SQL `MIN`/`MAX` ignore NULL values.
  - In DataFusion, `AggregateExec`'s accumulator ignores nulls.
  - A threshold is only emitted when at least one non-null string has been observed.
  - Pruning files/row-groups where all values are $\ge \text{current\_min}$ is sound because nulls could never produce a smaller minimum. If a partition contains only nulls, no threshold is emitted and no pruning occurs.

### 4. Collation & CHAR Fail-Closed
**Status: VERIFIED SAFE**
- **JVM Side**:
  - `QueryPlanSerde.isBinaryStringPruningType`: Gated on `!isStringCollationType(dataType)`. For Spark 4.0+, any non-default collation ID is rejected.
  - `hasCharPruningKey`: Recursively inspects Catalyst metadata for `__CHAR_VARCHAR_TYPE_STRING` starting with `char(`. Rejects CHAR types from TopK, MIN/MAX, and Join pruning.
- **Native Side**:
  - `is_direct_pruning_key` in `native/core/src/execution/operators/dynamic_filter/join.rs:418-430`:
    - Checks field metadata for `__COLLATIONS` and `ARROW:extension:name`.
    - Checks `__CHAR_VARCHAR_TYPE_STRING`: permits only `varchar(`, rejecting `char(` and any unknown annotations.
- **Negative Tests**: Verified via `annotated_string_columns_fail_closed` in `aggregate/tests.rs` and `topk/tests/iceberg.rs`, as well as e2e collation/CHAR queries in `CometIcebergNativeSuite.scala`.

### 5. Dictionary and Utf8View Paths
**Status: VERIFIED SAFE**
- `is_string_key_type`: Handles `Utf8`, `LargeUtf8`, `Utf8View`, and `Dictionary(_, Utf8 | LargeUtf8 | Utf8View)`.
- `scalar_to_datum`: Explicitly unboxes `ScalarValue::Dictionary` containing string variants into `Datum::string`.
- Unit tests (`aggregate/tests.rs`, `topk/tests/iceberg.rs`) iterate over all six Arrow encodings and assert proper filter attachment.

### 6. Test Suite Quality
**Status: VERIFIED SAFE**
- Comprehensive coverage in `CometIcebergNativeSuite.scala:1943-2100`.
- Validates split-level Iceberg file pruning with 16-character truncation against golden Spark results.
- Asserts metrics: `bytes_scanned` reduction and `iceberg_runtime_file_tasks_pruned > 0L`.
- Asserts zero pruning on negative controls (CHAR, Collation, NULLS FIRST).

---

## Detailed Audit: Branch B (B8 `ws-b8-file-stats-dec-str`)

### 1. Decimal Bound Decoding (`parse_iceberg_file_metrics`)
**Status: VERIFIED SAFE**
- **Format Compliance**: Iceberg serializes single-value decimal metrics as unscaled two's-complement big-endian bytes using minimal byte length.
- **Sign Extension**: `i128_from_be_bytes` in `iceberg::spec::decimal_utils` inspects the sign bit `bytes[0] & 0x80 != 0` and sign-extends slices of length $1 \dots 16$ to a 16-byte array before converting to `i128`. Lengths $> 16$ return `None`.
- **Defensive Type Validation**:
  - `scale > precision`: Detected at `planner.rs:4528` $\implies$ returns `None`.
  - Mantissa Overflow: At `planner.rs:4538`, verifies `val.unsigned_abs() < 10_u128.checked_pow(*precision)`. For precision 38, $10^{38} \le 2^{128} - 1$, preventing integer overflow. Exceeding values return `None`.
- **Fail-Open Invariant**: Errors in `Datum::try_from_bytes` or defensive guards return `None` via `.ok()?` within `.filter_map()`. Corrupted, mismatched, or out-of-spec bounds are silently dropped from file metrics without failing the plan or query.

### 2. String Bounds & Inclusiveness
**Status: VERIFIED SAFE**
- **Decoding**: Validated via `std::str::from_utf8(bytes)`. Non-UTF-8 bytes return `None` and fail open.
- **Inclusive Bounds**: Truncated Iceberg string bounds are treated exclusively as lower ($\le \min$) and upper ($\ge \max$) bounds.
- **Strict Evaluator Disabled**: Iceberg reader refuses string all-match shortcuts, guaranteeing row-level filter execution.

### 3. Scala-Side Gating & Key Type Alignment
**Status: VERIFIED SAFE**
- `RuntimePruningKeyTypes.scala`:
  - `isFileStatsKey`: Explicitly matches `_: DecimalType` and `st: StringType if !isStringCollationType(st)`.
  - `isFileStatsAttribute`: Excludes fixed `char(` attributes via `isCharType(attr.metadata)`.
  - `isSupportedTopKKey` & `isSupportedMinMaxKey`: Strictly restricted to `IntegerType | LongType | DateType | TimestampType | TimestampNTZType`. Decimal and String are deliberately **not** widened on this branch.
- `CometScanRule.scala`:
  - Joins use `directKeyAttribute` (`isFileStatsAttribute`) and check `!hasCharJoinKey` and `!isStringCollationType`.
  - TopK and MinMax use dedicated `directTopKKeyAttribute` and `directMinMaxKeyAttribute`.
  - Column names collected in `inputs` match `CometIcebergNativeScan.scala` (`RuntimePruningKeyTypes.isFileStatsAttribute(attr)`).

### 4. Pruning Safety (No False Pruning)
**Status: VERIFIED SAFE**
- When a bound is absent or dropped during parsing:
  - Manifest metrics contain no entry for that field ID.
  - `InclusiveMetricsEvaluator` treats missing bounds as `MIGHT_MATCH` (`true`), bypassing pruning.
  - Under no circumstances can a dropped or absent bound cause false row or file elimination.

### 5. Test Suite Quality & Adversarial Scenarios
**Status: VERIFIED SAFE**
- `CometIcebergNativeSuite.scala`:
  - `decimal(18,2) join prunes disjoint Iceberg files via runtime file stats`: Validates disjoint file elimination and golden query equivalence.
  - `string join prunes disjoint Iceberg files via runtime file stats`: Validates string disjoint file elimination.
  - `adversarial string join with long shared prefix beyond truncation produces correct results`: Configures `truncate(16)` with a 24-character common prefix (`abcdefghijklmnopqrstuvwx`), proving that overlapping truncated metadata does not compromise query correctness.
- Unit tests:
  - `planner.rs`: `test_iceberg_file_metrics_decimal_bound_decoding` tests negative decimals, precision 38 min/max, bad scale, 17-byte length, and mantissa overflow.
  - `planner.rs`: `test_iceberg_file_metrics_string_bounds` tests empty strings, Unicode, invalid UTF-8, and truncated prefixes.

---

## Ranked Findings Summary

### Branch A (B7: `adaptive-ci/ws-b7-string-topk`)
- **PROVEN-DEFECT**: None.
- **UNSAFE-ASSUMPTION**: None.
- **NIT**:
  - `spark/src/main/scala/org/apache/comet/rules/CometScanRule.scala:1285`: `limit.sortOrder.head.child` is added to `inputs` (requesting driver-side file stats collection) regardless of whether `order.nullOrdering` is `NullsFirst`. At execution time, `CometLocalTopKExec` and native `TopKReaderFilterExec` correctly refuse `NULLS FIRST` strings and skip runtime filter attachment. This is benign (fails open / unpruned), but collecting unused statistics could be avoided by checking `order.nullOrdering == NullsLast` in `CometScanRule`.

### Branch B (B8: `adaptive-ci/ws-b8-file-stats-dec-str`)
- **PROVEN-DEFECT**: None.
- **UNSAFE-ASSUMPTION**: None.
- **NIT**:
  - `spark/src/main/scala/org/apache/comet/rules/CometScanRule.scala:1298`: Redundant `!RuntimePruningKeyTypes.isStringCollationType(...)` checks on join keys; `directKeyAttribute` calls `isFileStatsAttribute`, which already delegates to `isFileStatsKey` and rejects collated strings.
