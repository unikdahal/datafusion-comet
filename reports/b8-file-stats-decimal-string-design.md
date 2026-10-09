# Technical Design: B8 — Driver-Side File-Level Statistics Pruning for Decimal and String Runtime Join Keys

**Status**: Proposed  
**Audience**: Comet Native and Spark Query Engine Engineers  
**Target Branches**: Integration branch `adaptive-ci/runtime-pruning-rewrite-20261006` (head `fcd020283`) on top of R1 refactor `adaptive-ci/ws-r1-key-types` (head `1ca825a4d`)  
**Design Classification**: Read-Only Architecture & Implementation Plan  

---

## Executive Summary & Quantified Opportunity

In Apache Comet's native Iceberg integration, runtime dynamic filter join keys (produced by hash join build sides) can prune data at two distinct granularities:
1. **Whole-file pruning** on the reader pipeline before opening the Parquet data file (via Iceberg manifest `lower_bounds`, `upper_bounds`, and `null_value_counts`).
2. **Row-group / page pruning** inside the native Parquet reader after opening the data file and parsing the Parquet footer and page index.

Prior to this design, the driver-side statistics collector (`RuntimePruningKeyTypes.isFileStatsKey`) strictly gated file-level statistics to integer (`Int8` through `Int64`), `Date32`, and microsecond `Timestamp` types. While native dynamic filtering was extended to `Decimal128` and `String` keys for row-group pruning, **file-level statistics were never collected or populated for Decimal and String keys**. Consequently, every split allocated for a file had to open the physical Parquet file, read and parse Parquet footers and page index dictionaries, and evaluate row groups individually—even when entire data files fell completely outside the join key domain.

### Empirical Quantification from Benchmark Run 37865998293

Artifacts from benchmark run `37865998293` (specifically shard logs from `results-join-shard-2.jsonl`, `results-strjoin-shard-0.jsonl`, and `matched-report/report.md`) provide empirical quantification of the remaining opportunity:

| Query | Key Type | Baseline Exec (ms) | Candidate Exec (ms) | Speedup (RG only) | Splits / Predicate Tasks | Files Pruned | RG Pruned / Total RG | Native MiB Read (Off -> On) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `join_inner__f_dec__dim_128` | Decimal(18, 2) | 621 ms | 90 ms | **6.83x** | 26 / 22 | **0** | **80 / 80** | 588.7 -> 12.2 MiB |
| `join_inner__f_str__dim_128` | String (dictionary/plain) | 1,054 ms | 147 ms | **7.35x** | 27 / 23 | **0** | **79 / 80** | 573.5 -> 12.7 MiB |
| `join_inner__f_str__dim_10k` | String | 684 ms | 112 ms | **6.06x** | 27 / 23 | **0** | **79 / 80** | 573.5 -> 13.2 MiB |
| `strjoin_str_broadcast_distinct_10000` | String | 238 ms | 210 ms | **1.22x** | 16 / 16 | **0** | **31 / 32** | 9.7 -> 8.0 MiB |

#### Key Insights from the Data:
1. **100% of Row Groups Pruned Without Whole-File Pruning**:
   In `join_inner__f_dec__dim_128`, 80 out of 80 row groups were pruned by the native row group evaluator. Yet `files pruned` was **0**. Exactly 22 predicate tasks were launched; every single task had to open a Parquet file, load its footer, and evaluate all row groups before returning an empty stream.
2. **Elimination of Parquet Footer and Page Index I/O**:
   For object storage (Amazon S3, Google Cloud Storage, Azure Data Lake), reading Parquet footers requires 1 to 2 synchronous HTTP range GET requests per file (typically 15–50 ms latency each). In `join_inner__f_str__dim_128`, 23 files were opened to prune 79 row groups. Whole-file pruning eliminates 23 physical file opens, saving 200–500 ms of remote I/O latency and task scheduling overhead.
3. **Task and Memory Deserialization Overhead**:
   When `files pruned > 0`, the reader pipeline detects `!file_might_match` at the task level (`pipeline.rs:186-193`) and immediately returns `Box::pin(futures::stream::empty())`. This bypasses Parquet metadata parsing, delete file resolution, Arrow schema construction, and page index buffer allocations.

---

## 1. End-to-End Architecture: File Stats Lifecycle

The end-to-end lifecycle spans 6 stages across the JVM and Rust layers:

```
[ Iceberg Manifest / Metadata ]
               │
               ▼ (Single-value serialization: ByteBuffer)
[ Iceberg DataFile.lowerBounds() / upperBounds() ]
               │
               ▼ (Scala Reflection in IcebergReflection.scala)
[ CometIcebergNativeScanMetadata.runtimeFileStatistics ]
               │
               ▼ (Protobuf Serialization in CometIcebergNativeScan.scala)
[ operator.proto: IcebergFileMetrics.lower_bounds / upper_bounds ]
               │
               ▼ (Native Deserialization in planner.rs: parse_iceberg_file_metrics)
[ iceberg-rust: FileScanTaskMetrics with Datum ]
               │
               ▼ (Bound against Table Schema in pipeline.rs)
[ InclusiveMetricsEvaluator::eval_metrics(predicate, metrics) ]
               │
       ┌───────┴───────┐
       ▼               ▼
[ ROWS_CANNOT_MATCH ]  [ ROWS_MIGHT_MATCH ]
  Reject Task           Open Parquet &
  (Skip file open)      Evaluate Row Groups
```

### Stage 1: Iceberg Manifest Single-Value Serialization
Iceberg stores column-level lower and upper bounds in manifest files using single-value binary serialization:
- **Decimal**: Serialized as big-endian two's-complement unscaled bytes representing an `i128` integer. The manifest bytes store only the unscaled value (mantissa); the column scale is NOT stored in the manifest entry and is defined strictly by the table schema.
- **String**: Serialized as standard UTF-8 encoded bytes. Iceberg manifest writers may truncate string bounds to reduce metadata size (e.g., `write.metadata.metrics.column.x=truncate(16)`). Lower bounds are truncated downwards ($\le \min$), and upper bounds are truncated upwards and incremented ($\ge \max$). If an upper bound cannot be incremented (e.g., ends in maximum UTF-8 code point `\uFFFF` or `0xFF` sequence), the upper bound is omitted (absent).

### Stage 2: Scala Reflection Extraction (`IcebergReflection.scala`)
During query compilation and scan planning:
1. `CometScanRule.runtimeFilterColumns` identifies dynamic filter join keys and validates whether they are eligible file statistics keys via `RuntimePruningKeyTypes.isFileStatsKey(attr.dataType)`.
2. `IcebergReflection.runtimeKeyColumns` inspects the Iceberg scan schema. It checks `RuntimePruningKeyTypes.isFileStatsIcebergType(typeStr)` to retain only columns with supported types.
3. `IcebergReflection.runtimeFileStatistics` calls Iceberg's Java API `TableScan.includeColumnStats(columns)`. Iceberg manifests are re-scanned (with LRU caching) to extract `DataFile` instances containing lower and upper bounds for the requested columns.

### Stage 3: Protobuf Encoding (`CometIcebergNativeScan.scala`)
In `CometIcebergNativeScan.scala`, `fileMetrics(contentFileClass, statistics, runtimeFieldIds)` reads:
- `lowerBounds: java.util.Map[Integer, ByteBuffer]`
- `upperBounds: java.util.Map[Integer, ByteBuffer]`
For every field ID in `runtimeFieldIds`, the byte buffer is copied into protobuf `IcebergFileMetrics` fields:
- `map<int32, bytes> lower_bounds = 5;`
- `map<int32, bytes> upper_bounds = 6;`
*Note*: `operator.proto` already has generic `map<int32, bytes>` for lower and upper bounds. No schema change or wire format change is required.

### Stage 4: Native Decode into Iceberg Datum (`planner.rs`)
In `native/core/src/execution/planner.rs`, `parse_iceberg_file_metrics` deserializes `IcebergFileMetrics`:
1. It looks up the field in the table schema by ID: `let field = schema.field_by_id(*id)?;`.
2. It obtains the primitive data type: `let data_type = field.field_type.as_primitive_type()?;`.
3. It reconstructs the bound using `iceberg::spec::Datum::try_from_bytes(bytes, data_type.clone())`.
   - **For Decimal**: Decodes the big-endian bytes into `i128` via `i128_from_be_bytes(bytes)` and associates it with `PrimitiveType::Decimal { precision, scale }` from the **table schema**. The table schema's scale is authoritative.
   - **For String**: Decodes UTF-8 bytes via `std::str::from_utf8(bytes)`. If UTF-8 decoding fails, the bound is dropped and the column fails open.

### Stage 5: Schema Binding and Semantic Verification
When a runtime filter arrives from the hash join build side:
1. `DynamicFilterPhysicalExpr` produces a predicate containing `Datum::Decimal` or `Datum::String`.
2. In `pipeline.rs`, `RuntimePredicates::current` binds the predicate to the task's table schema:
   - For `Decimal`: `bound_literal.to(&target_type)` verifies that the literal's scale exactly matches the column's table schema scale (`self_scale == target_scale`). If the scale mismatches, `Datum::to` returns `Err("Decimal scale conversion is not supported")`, which `runtime_predicate.rs` catches, logging a debug statement and safely disabling runtime pruning for that file (failing open).
   - In `pipeline.rs:file_bounds_match_predicate`, it verifies:
     `field.field_type.as_primitive_type() == Some(bound.data_type())`. If the bound's precision/scale or type differs from the predicate field type, `file_might_match` returns `true` (fails open).

### Stage 6: Inclusive Metrics Evaluation (`InclusiveMetricsEvaluator`)
In `pipeline.rs:file_might_match`:
`InclusiveMetricsEvaluator::eval_metrics(predicate, metrics.into(), false)` checks:
- `col = X`: If $X < \text{lower\_bound}$ or $X > \text{upper\_bound}$, returns `ROWS_CANNOT_MATCH`.
- `col IN (X1, X2, ...)`: If all $X_i$ lie outside $[\text{lower\_bound}, \text{upper\_bound}]$, returns `ROWS_CANNOT_MATCH`.
- `col < X`, `col > X`, `col <= X`, `col >= X`: Evaluated via boundary comparisons.
- If `ROWS_CANNOT_MATCH`, `pipeline.rs` records:
  ```rust
  self.scan_metrics.record_runtime_predicate_task();
  self.scan_metrics.record_runtime_file_task_pruned();
  return Ok(Box::pin(futures::stream::empty()));
  ```
  The physical Parquet file is **never opened**.

---

## 2. Type Semantics: Decimal and String Correctness

### 2.1 Decimal Semantics
1. **Unscaled Byte Representation**:
   - Iceberg manifests store unscaled decimal values as signed big-endian two's-complement byte sequences of minimal required length (1 to 16 bytes).
   - Negative values are sign-extended (e.g., `-1234` is stored as `[0xFB, 0x2E]`).
   - `i128_from_be_bytes` in `iceberg-rust` properly reconstructs signed `i128` values from any slice length $\le 16$ bytes.
2. **Authoritative Column Scale**:
   - Manifest bounds do not record the scale. The native side must reconstruct `Datum` using the table schema's declared scale (`PrimitiveType::Decimal { precision, scale }`).
   - Comparison in `InclusiveMetricsEvaluator` uses `PartialOrd for Datum`, which compares decimals via `decimal_from_i128_with_scale(*val, *scale)`:
     ```rust
     let val = decimal_from_i128_with_scale(*val, *scale);
     let other_val = decimal_from_i128_with_scale(*other_val, *other_scale);
     val.partial_cmp(&other_val)
     ```
3. **Scale Mismatch Rejection**:
   - If a join predicate literal has a scale different from the table column (e.g., runtime dynamic filter generated from an unaligned expression or cast), `iceberg-rust`'s `predicate.bind(schema)` explicitly fails:
     `Decimal scale conversion is not supported: source scale S1, target scale S2`.
   - Furthermore, `file_bounds_match_predicate` in `pipeline.rs` validates `field.field_type.as_primitive_type() == Some(bound.data_type())`. Any mismatch fails open (`file_might_match` returns `true`).
4. **Precision Overflow Defense**:
   - To guard against malformed manifest entries where the unscaled integer exceeds the column's declared precision ($|val| \ge 10^{\text{precision}}$ for precision $< 38$), `parse_iceberg_file_metrics` must reject the bound and drop it from the metrics map.

### 2.2 String Semantics
1. **Truncated Bounds Soundness**:
   - Iceberg allows strings in manifests to be truncated:
     - Lower bound: Truncated to a prefix $\le \text{min\_value}$ (e.g., `"abcdef"` truncated to `"abcd"`).
     - Upper bound: Truncated to a prefix and incremented $\ge \text{max\_value}$ (e.g., `"abcdef"` truncated and incremented to `"abce"`).
   - If the upper bound ends in `\uFFFF` or cannot be incremented, the upper bound is omitted.
2. **Inclusive Evaluation Only**:
   - Because bounds are outward-bounded, they are mathematically sound for **inclusive evaluation** (`ROWS_MIGHT_MATCH` / `ROWS_CANNOT_MATCH`). If `literal < lower_bound`, then `literal < true_min`, so no row can match. If `literal > upper_bound`, then `literal > true_max`, so no row can match.
   - If the upper bound is absent, `InclusiveMetricsEvaluator` assumes the upper bound is infinite and conservatively returns `ROWS_MIGHT_MATCH`.
3. **Strict Shortcut Disabled for Strings**:
   - In `StrictMetricsEvaluator`, strict equality shortcut (`file_always_matches`) requires `lower == literal == upper`. For truncated strings, `lower == upper` can only occur if the true min and max are identical and not truncated. Furthermore, `pipeline.rs:file_always_matches` requires exact semantic matching. Truncated strings will never trigger false strict row-filter bypasses.
4. **UTF-8 and Multi-byte Characters**:
   - Manifest bounds store valid UTF-8 byte sequences. Non-ASCII characters (e.g., CJK characters, accented Latin, emoji) must be validated via `std::str::from_utf8`. Malformed UTF-8 must fail open.

---

## 3. Detailed Changes by File and Line

The implementation builds on top of the R1 refactor branch (`adaptive-ci/ws-r1-key-types`, commit `1ca825a4d`), which centralized key type predicates in `RuntimePruningKeyTypes.scala`.

### 3.1 `spark/.../sql/comet/RuntimePruningKeyTypes.scala`
**Location**: `spark/src/main/scala/org/apache/spark/sql/comet/RuntimePruningKeyTypes.scala` (Lines 40–80)  
**Changes**:
1. Update `SUPPORTED_FILE_STATS_TYPES` documentation to reflect Decimal and String support.
2. Update `SUPPORTED_ICEBERG_FILE_STATS_TYPES` to include `"string"`.
3. Update `isFileStatsKey` pattern match to include `_: DecimalType` and `StringType`.
4. Update `isFileStatsIcebergType` to match `"string"` and any decimal definition matching prefix `"decimal("`.

```scala
  // Line 41:
  val SUPPORTED_FILE_STATS_TYPES: Seq[DataType] =
    Seq(IntegerType, LongType, DateType, TimestampType, TimestampNTZType, DecimalType.SYSTEM_DEFAULT, StringType)

  // Line 48:
  val SUPPORTED_ICEBERG_FILE_STATS_TYPES: Set[String] =
    Set("int", "long", "date", "timestamp", "timestamptz", "string")

  // Line 63:
  def isFileStatsKey(dataType: DataType): Boolean = dataType match {
    case IntegerType | LongType | DateType | TimestampType | TimestampNTZType | _: DecimalType | StringType => true
    case _ => false
  }

  // Line 75:
  def isFileStatsIcebergType(typeStr: String): Boolean =
    SUPPORTED_ICEBERG_FILE_STATS_TYPES.contains(typeStr) ||
      typeStr == "string" ||
      typeStr.startsWith("decimal(")
```

### 3.2 `spark/.../comet/iceberg/IcebergReflection.scala`
**Location**: `spark/src/main/scala/org/apache/comet/iceberg/IcebergReflection.scala` (Line 487 and Line 2574)  
**Verification & Refinement**:
- Line 487: `runtimeKeyColumns(schema: Any)` extracts `typeStr = getMethod(column.getClass, "type").invoke(column).toString`.
  In Iceberg Java, `Types.DecimalType.of(p, s).toString` produces `"decimal(p, s)"` (e.g., `"decimal(18, 2)"`), and `Types.StringType.get().toString` produces `"string"`.
  Delegating to `RuntimePruningKeyTypes.isFileStatsIcebergType(typeStr)` directly enables both Decimal and String.
- Line 2574:
  `val eligibleRuntimeStatisticsColumns = runtimeStatisticsColumns.intersect(IcebergReflection.runtimeKeyColumns(scanSchema).toSet)`
  Now automatically retains Decimal and String column names when they appear in join keys.
- Lines 550–575 (`runtimeFileStatistics`):
  Calls `scan.includeColumnStats(columns)`. Iceberg's Java manifest planner populates `lowerBounds` and `upperBounds` maps of `ByteBuffer` for the selected Decimal and String columns.

### 3.3 `spark/.../comet/rules/CometScanRule.scala`
**Location**: `spark/src/main/scala/org/apache/comet/rules/CometScanRule.scala` (Lines 1240–1248)  
**Verification**:
```scala
    def directKeyAttribute(expression: Expression): Option[Attribute] = expression match {
      case attr: Attribute if RuntimePruningKeyTypes.isFileStatsKey(attr.dataType) =>
        Some(attr)
      case _ => None
    }
```
With `RuntimePruningKeyTypes.isFileStatsKey` updated, `runtimeFilterColumns` now collects Decimal and String join key attributes from hash join conditions.

### 3.4 `spark/.../comet/serde/operator/CometIcebergNativeScan.scala`
**Location**: `spark/src/main/scala/org/apache/comet/serde/operator/CometIcebergNativeScan.scala` (Lines 1025–1035 and 55–87)  
**Verification**:
- Lines 1025–1035:
  ```scala
  val runtimeFieldIds = output
    .zip(projectFieldIds)
    .collect {
      case (attr, id)
          if runtimeFieldNames.contains(attr.name) &&
            RuntimePruningKeyTypes.isFileStatsKey(attr.dataType) =>
        id
    }
    .toSet
  ```
  `runtimeFieldIds` is now non-empty for Decimal and String columns.
- Lines 55–87 (`fileMetrics`):
  Serializes `ByteBuffer` maps via `ByteString.copyFrom(buf.duplicate())` into `lower_bounds` and `upper_bounds`. This already operates generically on raw byte buffers, requiring no changes.

### 3.5 `native/core/src/execution/planner.rs`
**Location**: `native/core/src/execution/planner.rs` (Lines 4515–4540)  
**Enhancement in `parse_iceberg_file_metrics`**:
Add precision validation for decimals to reject corrupt or overflow bounds:
```rust
fn parse_iceberg_file_metrics(
    metrics: &spark_operator::IcebergFileMetrics,
    schema: &iceberg::spec::Schema,
) -> iceberg::scan::FileScanTaskMetrics {
    let bounds = |values: &std::collections::HashMap<i32, Vec<u8>>| {
        values
            .iter()
            .filter_map(|(id, bytes)| {
                let field = schema.field_by_id(*id)?;
                let data_type = field.field_type.as_primitive_type()?;
                // Decode bytes into datum using table column's data type
                let datum = iceberg::spec::Datum::try_from_bytes(bytes, data_type.clone()).ok()?;
                
                // Defensive validation: if Decimal, ensure unscaled mantissa does not overflow column precision
                if let iceberg::spec::PrimitiveType::Decimal { precision, .. } = data_type {
                    if let iceberg::spec::PrimitiveLiteral::Int128(val) = datum.literal() {
                        if *precision < 38 {
                            let limit = 10_u128.pow(*precision);
                            if val.unsigned_abs() >= limit {
                                return None; // Reject bound on precision overflow
                            }
                        }
                    }
                }
                Some((*id, datum))
            })
            .collect()
    };
    iceberg::scan::FileScanTaskMetrics::new(
        metrics.record_count,
        metrics.value_counts.clone(),
        metrics.null_value_counts.clone(),
        metrics.nan_value_counts.clone(),
        bounds(&metrics.lower_bounds),
        bounds(&metrics.upper_bounds),
    )
}
```

---

## 4. Comprehensive Test Plan

### 4.1 Rust Native Unit Tests (`native/core/src/execution/planner.rs`)
Add dedicated test cases to `mod tests` in `planner.rs`:

1. **`test_iceberg_file_metrics_decimal_bound_decoding`**:
   - Field: `Decimal { precision: 18, scale: 2 }`.
   - Positive Decimal: `123.45` (unscaled `12345`). Big-endian bytes `12345_i128.to_be_bytes()`. Assert decodes to `Datum::decimal(12345, 2)`.
   - Negative Decimal: `-123.45` (unscaled `-12345`). Big-endian two's complement `(-12345_i128).to_be_bytes()`. Assert decodes to `Datum::decimal(-12345, 2)`.
2. **`test_iceberg_file_metrics_decimal_max_precision_38`**:
   - Field: `Decimal { precision: 38, scale: 10 }`.
   - Bound: `i128::MAX` and `i128::MIN`. Validate proper sign preservation and bound construction without integer overflow.
3. **`test_iceberg_file_metrics_decimal_precision_overflow_rejection`**:
   - Field: `Decimal { precision: 4, scale: 2 }` (max unscaled value 9999).
   - Bound with unscaled value `10000` (exceeds precision 4).
   - Assert bound is discarded (`None`) and not added to metrics map.
4. **`test_iceberg_file_metrics_string_utf8_and_non_ascii`**:
   - Field: `PrimitiveType::String`.
   - Bounds: Multi-byte UTF-8 strings (`"房东整租霍营小区二层两居室"`, `"日本語"`, `"café"`).
   - Assert decodes cleanly into `Datum::string(...)`.
   - Invalid UTF-8 bytes: `vec![0xFF, 0xFE]`. Assert decoding returns `Err` and bound is ignored.
5. **`test_iceberg_file_metrics_truncated_strings_with_shared_prefixes`**:
   - Column: `key STRING`.
   - Lower bound: `"prefix_a"`, Upper bound: `"prefix_m"`.
   - Inclusive evaluator checks:
     - `key = "prefix_c"`: returns `ROWS_MIGHT_MATCH` (within bounds).
     - `key = "prefix_z"`: returns `ROWS_CANNOT_MATCH` (exceeds upper bound).
     - `key = "alpha"`: returns `ROWS_CANNOT_MATCH` (below lower bound).
     - Absent upper bound (cannot be incremented): returns `ROWS_MIGHT_MATCH`.

### 4.2 End-to-End Spark Integration Tests (`CometIcebergNativeSuite.scala`)
Add the following tests to `spark/src/test/scala/org/apache/comet/CometIcebergNativeSuite.scala`:

1. **`test("runtime file statistics pruning for Decimal join keys")`**:
   - Create Iceberg table `t_dec (id INT, price DECIMAL(10, 2), data STRING) USING iceberg`.
   - Insert data across 4 separate commits to create 4 data files:
     - File 1: `price` in `[10.00, 20.00]`
     - File 2: `price` in `[30.00, 40.00]`
     - File 3: `price` in `[50.00, 60.00]`
     - File 4: `price` in `[70.00, 80.00]`
   - Create dimension table `dim (price DECIMAL(10, 2))` containing only `[35.00]`.
   - Execute join query:
     ```sql
     SELECT /*+ BROADCAST(dim) */ count(*), sum(t.id)
     FROM t_dec t JOIN dim ON t.price = dim.price
     ```
   - Assert with `checkSparkAnswer` against vanilla Spark (`spark.comet.enabled=false`).
   - Collect Iceberg native scans:
     ```scala
     val scans = collectIcebergNativeScans(df.queryExecution.executedPlan)
     val filesPruned = scans.map(_.metrics("iceberg_runtime_file_tasks_pruned").value).sum
     assert(filesPruned >= 3, s"Expected at least 3 files pruned, got $filesPruned")
     ```
2. **`test("runtime file statistics pruning for String join keys with shared prefix")`**:
   - Create Iceberg table `t_str (code STRING, val INT) USING iceberg`.
   - Insert 4 files with clustered keys:
     - File 1: `code` in `["item_001", "item_050"]`
     - File 2: `code` in `["item_100", "item_150"]`
     - File 3: `code` in `["item_200", "item_250"]`
     - File 4: `code` in `["item_300", "item_350"]`
   - Execute broadcast join filtering for `code = "item_120"`.
   - Assert `checkSparkAnswer` matches.
   - Assert `iceberg_runtime_file_tasks_pruned >= 3`.
3. **`test("runtime file statistics decimal scale mismatch fails open safely")`**:
   - Query joining `t_dec.price` (`DECIMAL(10, 2)`) with a dimension column of `DECIMAL(10, 3)`.
   - Assert answer correctness matches Spark.
   - Verify that execution does not crash and safely fails open (`files_pruned == 0`, rows filtered correctly).

---

## 5. Benchmark Query Target List

The following queries from benchmark campaign `37865998293` directly target this functionality and should be tracked for latency and file pruning verification:

1. **`join_inner__f_dec__dim_128`**:
   - Fact: 16M rows, 26 splits, 80 row groups. Decimal(18, 2) join key.
   - Target metric: `files pruned` increases from **0 to 22**; Parquet footer reads reduced to 4; execution latency drops from ~90 ms towards ~35 ms.
2. **`join_inner__f_str__dim_128`**:
   - Fact: 16M rows, 27 splits, 80 row groups. String join key with 128 distinct dimension keys.
   - Target metric: `files pruned` increases from **0 to 23**; Parquet footer reads reduced to 4.
3. **`join_inner__f_str__dim_10k`**:
   - Fact: 16M rows, 27 splits. String join key with 10,000 distinct dimension keys.
   - Target metric: `files pruned` increases from **0 to 23**.
4. **`join_inner__f_str__dim_128_spread`**:
   - Fact: 16M rows, spread key distribution across files.
   - Target metric: Tests partial file pruning efficacy when bounds overlap on some files.
5. **`strjoin_str_broadcast_distinct_10000`**:
   - Fact: 4M rows, 16 splits, 32 row groups. Broadcast string join.
   - Target metric: `files pruned` increases from **0 to ~15**.

---

## 6. Risks, Rollback Strategy & Failure Modes

| Risk / Failure Mode | Impact | Mitigation / Built-in Defense |
| :--- | :--- | :--- |
| **Decimal Scale Mismatch** | Incorrect pruning if unscaled mantissa compared with wrong scale. | **Fail-open by construction**: `iceberg-rust`'s `bound_literal.to(&target_type)` checks `self_scale == target_scale`. Scale mismatch returns error, caught by `runtime_predicate.rs` to safely drop runtime filter. |
| **Truncated String False Positive/Negative** | Pruning files that actually contain matching rows. | **Mathematically sound**: Lower bound truncated down ($\le \min$), upper bound truncated up ($\ge \max$). Missing upper bound treated as $\infty$. Strict shortcut is explicitly disabled for strings. |
| **Corrupt / Non-UTF8 Strings in Manifest** | Panic or crash during manifest decoding. | `std::str::from_utf8` returns `Result`. `planner.rs` uses `.ok()?`, discarding corrupt bounds and failing open to row-group/batch scanning. |
| **Driver Overhead from Manifest Re-planning** | Slower query compilation on the driver. | Protected by `IcebergReflection.runtimeStatsCache` (LRU cache keyed by table metadata location, snapshot ID, and sorted column IDs). Configured via `spark.comet.iceberg.runtimeStatsCache.enabled` (default `true`) and `spark.comet.iceberg.runtimeStatsCache.maxEntries` (default `64`). |
| **Runtime Predicate Generation Lag** | Tasks start before join build side finishes. | Expected dynamic filtering behavior. If build side finishes late, tasks that haven't opened yet prune files; in-flight tasks prune surviving row groups. |

### Rollback Strategy
1. **Runtime Configuration Kill-Switch**:
   - To disable runtime join dynamic filtering entirely:
     `SET spark.comet.exec.join.dynamicFilter.enabled = false;` (defaults to standard unfiltered Iceberg scan).
   - To disable driver-side Iceberg file statistics collection while retaining native row-group pruning:
     `SET spark.comet.iceberg.runtimeStatsCache.enabled = false;` or revert `isFileStatsKey` in `RuntimePruningKeyTypes.scala`.
2. **Code Rollback**:
   - Reverting the single-line changes in `RuntimePruningKeyTypes.scala` (`isFileStatsKey` and `isFileStatsIcebergType`) instantly restores previous behavior without affecting native execution or other data types.
