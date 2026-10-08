# Diagnosis of Failing Integration Jobs in Run 37755479696

**Target Run:** [Run 37755479696](https://github.com/unikdahal/datafusion-comet/actions/runs/37755479696)  
**Workflow:** Fork Adaptive Iceberg Pruning Validation  
**Head SHA:** `92bba61b68ec44f305e0519f507cd02ae4546ee0` (`adaptive-ci/runtime-pruning-rewrite-20261006`)  
**Last Green Run:** Run 37703958482 at commit `058991ede`  
**Commits Since Green Run:**
- `793506a54`: Merge remote-tracking branch 'upstream/main' (at `f80042f78`)
- `4226d7e3c`: Pin row-group range reuse and proven static-filter fast paths
- `9cccd0676`: Avoid redundant dynamic filtering for materialized shuffle probes
- `92bba61b6`: Use row-count-preserving empty Iceberg projections

---

## 1. Job Details and Failure Summary

| Job Name | Job ID | Conclusion | Failing Step |
| :--- | :--- | :--- | :--- |
| `integration (3.5)` | `113244480094` | `failure` | `Native Iceberg, aggregate, planner and transport suites` |
| `integration (4.1)` | `113244480106` | `failure` | `Native Iceberg, aggregate, planner and transport suites` |

Other jobs in run 37755479696:
- `native`: `success`
- `rust`: `success`
- `spark_sql / Build Native + JVM Test Classes`: `success`

### Test Suite Execution Summary
- **Spark 3.5 (`integration (3.5)`):** `Tests: succeeded 598, failed 2, canceled 11, ignored 2, pending 0`
- **Spark 4.1 (`integration (4.1)`):** `Tests: succeeded 608, failed 2, canceled 1, ignored 2, pending 0`

Exactly **2 tests** failed in both Spark 3.5 and Spark 4.1. No other tests failed.

---

## 2. Failing Tests and Error Excerpts

Both failing tests belong to [`org.apache.comet.exec.CometJoinSuite`](file:///home/unik/Coding/rust/rp-work/comet/spark/src/test/scala/org/apache/comet/exec/CometJoinSuite.scala):

### Test 1: `join dynamic filter prunes probe rows: SHUFFLE_HASH, buildLeft=false, AQE=true`
- **Fails in:** Both Spark 3.5 and Spark 4.1
- **Failure Location:** [`CometJoinSuite.scala:417`](file:///home/unik/Coding/rust/rp-work/comet/spark/src/test/scala/org/apache/comet/exec/CometJoinSuite.scala#L417)
- **Error Excerpt:**
```text
org.scalatest.exceptions.TestFailedException: 0 was not greater than 0
  at org.scalatest.Assertions.newAssertionFailedException(Assertions.scala:472)
  at org.scalatest.Assertions.newAssertionFailedException$(Assertions.scala:471)
  at org.scalatest.Assertions$.newAssertionFailedException(Assertions.scala:1231)
  at org.scalatest.Assertions$AssertionsHelper.macroAssert(Assertions.scala:1295)
  at org.apache.comet.exec.CometJoinSuite.$anonfun$new$61(CometJoinSuite.scala:417)
  at org.apache.spark.sql.catalyst.plans.SQLHelper.withSQLConf(SQLHelper.scala:54)
  at org.apache.spark.sql.catalyst.plans.SQLHelper.withSQLConf$(SQLHelper.scala:38)
  at org.apache.spark.sql.CometTestBase.withSQLConf(CometTestBase.scala:60)
  at org.apache.comet.exec.CometJoinSuite.$anonfun$new$60(CometJoinSuite.scala:394)
  at scala.collection.immutable.List.foreach(List.scala:431)
```

### Test 2: `join dynamic filter prunes probe rows: SHUFFLE_HASH, buildLeft=true, AQE=true`
- **Fails in:** Both Spark 3.5 and Spark 4.1
- **Failure Location:** [`CometJoinSuite.scala:417`](file:///home/unik/Coding/rust/rp-work/comet/spark/src/test/scala/org/apache/comet/exec/CometJoinSuite.scala#L417)
- **Error Excerpt:**
```text
org.scalatest.exceptions.TestFailedException: 0 was not greater than 0
  at org.scalatest.Assertions.newAssertionFailedException(Assertions.scala:472)
  at org.scalatest.Assertions.newAssertionFailedException$(Assertions.scala:471)
  at org.scalatest.Assertions$.newAssertionFailedException(Assertions.scala:1231)
  at org.scalatest.Assertions$AssertionsHelper.macroAssert(Assertions.scala:1295)
  at org.apache.comet.exec.CometJoinSuite.$anonfun$new$61(CometJoinSuite.scala:417)
  at org.apache.spark.sql.catalyst.plans.SQLHelper.withSQLConf(SQLHelper.scala:54)
  at org.apache.spark.sql.catalyst.plans.SQLHelper.withSQLConf$(SQLHelper.scala:38)
  at org.apache.spark.sql.CometTestBase.withSQLConf(CometTestBase.scala:60)
  at org.apache.comet.exec.CometJoinSuite.$anonfun$new$60(CometJoinSuite.scala:394)
  at scala.collection.immutable.List.foreach(List.scala:431)
```

---

## 3. Root Cause Analysis

### Suspected Commit
Commit **`9cccd06764fdab52ec112fa1c906975cfe65b16d`**: *"Avoid redundant dynamic filtering for materialized shuffle probes"*.

### Code Location
File: [`native/core/src/execution/operators/dynamic_filter/join.rs:304-328`](file:///home/unik/Coding/rust/rp-work/comet/native/core/src/execution/operators/dynamic_filter/join.rs#L304-L328):
```rust
fn is_materialized_shuffle_probe(input: &Arc<dyn ExecutionPlan>) -> bool {
    if input.is::<ShuffleScanExec>() {
        true
    } else if let Some(projection) = input.downcast_ref::<ProjectionExec>() {
        is_materialized_shuffle_probe(projection.input())
    } else if let Some(filter) = input.downcast_ref::<CometFilterExec>() {
        is_materialized_shuffle_probe(filter.input())
    } else {
        false
    }
}

fn ineligible_reason(join: &HashJoinExec, config: &ConfigOptions) -> Result<Option<&'static str>> {
    ...
    if is_materialized_shuffle_probe(join.right()) {
        return Ok(Some("probe is already materialized by a shuffle"));
    }
    ...
}
```

### Failure Mechanism
1. In Spark, when Adaptive Query Execution (AQE) is enabled (`SQLConf.ADAPTIVE_EXECUTION_ENABLED = true`), shuffle stages above exchange boundaries become `ShuffleQueryStageExec`.
2. When Comet serializes physical plans containing query stages where direct shuffle read is enabled ([`CometSink.scala:108`](file:///home/unik/Coding/rust/rp-work/comet/spark/src/main/scala/org/apache/comet/serde/operator/CometSink.scala#L108)), it converts the operator into a `ShuffleScan` protobuf message, which the native query planner converts to [`ShuffleScanExec`](file:///home/unik/Coding/rust/rp-work/comet/native/core/src/execution/operators/shuffle_scan.rs#L55).
3. Commit `9cccd0676` added `is_materialized_shuffle_probe` to mark any hash join whose probe side is a `ShuffleScanExec` (or wrapped in projection/filter) as ineligible for dynamic filter pushdown (`ineligible_reason` returning `Some("probe is already materialized by a shuffle")`).
4. When `ineligible_reason` fires, [`DynamicFilterJoinExec::try_new`](file:///home/unik/Coding/rust/rp-work/comet/native/core/src/execution/operators/dynamic_filter/join.rs#L290) returns `Ok(None)`, and the native planner leaves the join as a plain `HashJoinExec` without the `DynamicFilterJoinExec` wrapper.
5. In [`CometJoinSuite.scala:414-418`](file:///home/unik/Coding/rust/rp-work/comet/spark/src/test/scala/org/apache/comet/exec/CometJoinSuite.scala#L414-L418), the test asserts:
   ```scala
   val evaluated = join.metrics("dynamic_filter_join_rows_evaluated").value
   val pruned = join.metrics("dynamic_filter_join_rows_pruned").value
   val bypassed = join.metrics("dynamic_filter_join_rows_bypassed").value
   assert(evaluated > 0L && pruned > 0L)
   ```
   Because `DynamicFilterJoinExec` was never attached to the native plan, the dynamic filter never ran. Both `evaluated` and `pruned` remained `0`, triggering `0 was not greater than 0`.
6. Why other combinations passed:
   - `BROADCAST` joins (with or without AQE): probe side is not a `ShuffleScanExec`, so dynamic filtering remains attached and passes.
   - `SHUFFLE_HASH` with `AQE=false`: `CometSink.shouldUseShuffleScan` does not fire (query stages are not present), so input is read via normal `ScanExec`/FFI batch stream rather than `ShuffleScanExec`. Hence `is_materialized_shuffle_probe` evaluates to `false`, dynamic filtering is attached, and the test passes.

---

## 4. Upstream Apache Main Status

- The test matrix in [`CometJoinSuite.scala`](file:///home/unik/Coding/rust/rp-work/comet/spark/src/test/scala/org/apache/comet/exec/CometJoinSuite.scala) exists on Apache main (`origin/main` at `3ae29aefd`, and merged commit `f80042f78`).
- Recent CI runs on Apache main (e.g., Run `37707916699`, Run `37423888890`) completed successfully with green checks across Spark SQL test suites including `CometJoinSuite` in the `[exec]` suite profile.
- Apache main does not contain commit `9cccd0676` or the `is_materialized_shuffle_probe` exclusion.

---

## 5. Classification

**Genuine regression from fork commit**:
Commit `9cccd0676` introduced a deliberate optimization to bypass dynamic filter evaluation on `ShuffleScanExec` probe inputs, but did not update the existing Scala test suite in `CometJoinSuite.scala` which specifically tests and asserts active dynamic filter row pruning under `SHUFFLE_HASH` with `AQE=true`.

---

## 6. Proposed Fix

Depending on the intended system semantics:

### Option A: If bypassing dynamic filtering for materialized shuffle probes is intended
Update the test assertions in [`CometJoinSuite.scala`](file:///home/unik/Coding/rust/rp-work/comet/spark/src/test/scala/org/apache/comet/exec/CometJoinSuite.scala#L412-L425) to account for the shuffle probe bypass under AQE:
- For `strategy == "SHUFFLE_HASH" && adaptive`, expect `evaluated == 0L` and `pruned == 0L` when dynamic filter pushdown is bypassed by design for materialized shuffle probe inputs.
- Alternatively, provide a configuration key to govern the shuffle probe bypass behavior if users still require dynamic filter pushdown on shuffle inputs.

### Option B: If dynamic filtering should remain active on shuffle probes
Revert the probe-side shuffle check in [`native/core/src/execution/operators/dynamic_filter/join.rs`](file:///home/unik/Coding/rust/rp-work/comet/native/core/src/execution/operators/dynamic_filter/join.rs#L326-L328):
- Remove `if is_materialized_shuffle_probe(join.right())` from `ineligible_reason`.
- Update `docs/runtime-pruning-design.md` and remove or adjust the unit test in `join/tests.rs` (`shuffled_probes_keep_exact_join_without_redundant_filter`).
