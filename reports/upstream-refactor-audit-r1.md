# Upstream refactor audit (round 1) — runtime pruning, Comet + Iceberg Rust

**Mode:** read-only audit. No builds, tests, or edits were run; line anchors come from
`git show`/`git grep` on the fork commits and from the saved scoped diffs in `/tmp/opencode/audit/`.

**Revisions audited**

| repo | fork | merge-base |
|---|---|---|
| `datafusion-comet` | `bef150221ce10a332d7257b4d885288aa0d49016` | `f80042f78715e5a009352f1582f6c246a5f96131` |
| `iceberg-rust` | `157cf6475ef46f272366d881580715e7edacf4c4` | `ecba0b537a1f78ec8125c1a26f6dfab3f95b509e` |

**Scope:** Comet `native/core/src/execution/operators/dynamic_filter/`, iceberg-related native
code, `spark/src/main/scala/org/apache/comet/**`, `spark/src/main/scala/org/apache/spark/sql/comet/**`;
Iceberg `crates/iceberg/src/{arrow,expr,scan,runtime}/**`.

**Verdict.** The code is in better shape than most fork-sized changes: no fork-context comments,
no added `#[allow(dead_code)]`, no dead functions, and every new `pub` item has real doc
comments (including doc examples). The upstreamability problems are (a) nine behavior-affecting
changes buried in the refactor stream that must be split out, (b) a public API surface that is
slightly wider than its consumers need, (c) test instrumentation living inside production
structs/impls, and (d) six places that re-derive the same "which key types are supported"
decision, which *will* drift.

Effort: S ≤ half a day, M ≤ 2 days, L > 2 days. Risk = risk to behavior/review, not effort.

---

## Tier 1 — Behavior-affecting changes: split out, one PR unit each (items 1–9)

These are not refactors. Each changes observable behavior and must not ride along with
mechanical cleanup, or the refactor review becomes a semantics review.

**1. [behavior] `IN`/set literal must lie within *both* bounds at once**
- **Where:** `crates/iceberg/src/expr/visitors/inclusive_metrics_evaluator.rs:505`,
  `crates/iceberg/src/expr/visitors/row_group_metrics_evaluator.rs:500`
- **Problem:** this is a pruning-correctness fix (over/under-pruning depending on direction),
  currently sitting next to unrelated refactors in the same files.
- **Change:** land as its own unit with the fix + tests in both evaluators (and the matching
  page-index test at `page_index_evaluator.rs`), no mechanical edits mixed in.
- **Effort** S · **Risk** M (pruning semantics) · **Verify:** evaluator suites in both files,
  plus a bounds table test (lower > upper, set straddling each bound).

**2. [behavior] Strict evaluator now reports NaN presence**
- **Where:** `crates/iceberg/src/expr/visitors/strict_metrics_evaluator.rs` (`may_contain_nan`,
  used by `may_contain_null(...) || may_contain_nan(...)`).
- **Problem:** changes which files the strict path may reject; correctness-adjacent and silent
  if bundled.
- **Change:** own unit with tests covering all-NaN, no-NaN, and unknown-NaN columns.
- **Effort** S · **Risk** M · **Verify:** strict evaluator tests + a "never rejects a file
  containing NaN" assertion.

**3. [behavior] Zero-column scans build `ProjectionMask::none`**
- **Where:** `crates/iceberg/src/arrow/reader/projection.rs` (`return Ok(ProjectionMask::none(...))`
  with the COUNT(*)-style rationale comment).
- **Problem:** changes projection for zero-output-field scans; previously a different path ran.
- **Change:** own unit; keep the comment, add the regression test that motivated it.
- **Effort** S · **Risk** M · **Verify:** projection tests + a COUNT(*) scan test.

**4. [behavior] `Decimal32/64/256` + `Time64(Second?/Microsecond)` schema and scalar support**
- **Where:** `crates/iceberg/src/arrow/schema.rs` (decimal arms in the Arrow→Iceberg type
  conversion, `Time64MicrosecondArray::new_scalar`).
- **Problem:** types that previously errored or took another path now convert.
- **Change:** own unit with per-type round-trip tests (schema + scalar).
- **Effort** S · **Risk** M · **Verify:** schema conversion tests for each new decimal width.

**5. [behavior] Page-index evaluation widened (UINT32 + compatibility handling)**
- **Where:** `crates/iceberg/src/expr/visitors/page_index_evaluator.rs`.
- **Problem:** more files/pages are now pruned than before; any regression is a wrong-results bug.
- **Change:** own unit with before/after tests for each newly-supported type/encoding.
- **Effort** M · **Risk** M · **Verify:** page-index tests + one end-to-end prune test.

**6. [behavior] Metadata-table detection switched to class-hierarchy walk**
- **Where:** `spark/.../rules/CometScanRule.scala:133` (`metadataTableSuffix`), `:152-172`
  (superclass walk for `org.apache.iceberg.BaseMetadataTable`, name fallback).
- **Problem:** changes *which* scans are treated as Iceberg metadata tables, i.e. whether
  runtime statistics are attached; the name fallback keeps old behavior only when the class
  is unreachable.
- **Change:** own unit including the false-positive regression test named in the comment
  (`orders_history`, `user_files`, `snapshots`).
- **Effort** S · **Risk** M · **Verify:** new unit test + existing Iceberg scan suites.

**7. [behavior] IPC batch chunk size 1 MiB → 64 KiB**
- **Where:** `spark/.../comet/util/Utils.scala:53-56` (`BatchIpcChunkSize`), used at `:263`.
- **Problem:** perf/memory behavior change for every broadcast/chunked IPC write; the rationale
  comment is good but has no number attached.
- **Change:** own unit with a microbenchmark or at least measured allocation/broadcast evidence
  in the description; keep comment, add the measurement.
- **Effort** S · **Risk** M (regression on large broadcasts) · **Verify:** broadcast/exec suite
  + before/after benchmark on a large build side.

**8. [behavior] Arrow writer lifecycle + `normalizeBatchOffsets`**
- **Where:** `spark/.../comet/util/Utils.scala:274-286` (writer close moved into `finally`,
  `normalized.close()`), helper at `:463`.
- **Problem:** fixes writer/offset handling — a real fix, but it changes what bytes leave the
  JVM for multi-batch roots.
- **Change:** own unit with a regression test that exercises >1 batch.
- **Effort** S · **Risk** M · **Verify:** new test + shuffle/broadcast suites.

**9. [behavior] `iceberg_partition_value.rs` test-expectation change**
- **Where:** `crates/iceberg/src/.../iceberg_partition_value.rs` (changed assertions).
- **Problem:** an expectation change bundled into the feature stream; it encodes a dependency's
  behavior change, not a refactor.
- **Change:** attach it to the dependency-bump/behavior PR that caused it, or land it first as
  a standalone "test expectation follows dependency" unit with the reason in the commit body.
- **Effort** S · **Risk** L · **Verify:** that one test file.

---

## Tier 2 — Public API surface (items 10–14)

**10. [api] `TableScanBuilder::include_column_stats{,_for}` has no production caller**
- **Where:** `crates/iceberg/src/scan/mod.rs:182`, `:189` (docs); only callers are tests at
  `scan/mod.rs:~1115-1150`.
- **Problem:** new `pub` API whose only in-repo consumer is its own test; Comet drives the Java
  API through `IcebergReflection.runtimeFileStatistics` instead. Widest-than-needed surface.
- **Change:** demote to `pub(crate)` (tests still compile) or state the external consumer before
  landing; regenerate `crates/iceberg/public-api.txt`.
- **Effort** S · **Risk** M (removing `pub` after release is breaking) · **Verify:**
  `cargo public-api` diff, `cargo test -p iceberg`.

**11. [api] `impl From<&DataFile> for FileScanTaskMetrics` cannot be crate-private**
- **Where:** `crates/iceberg/src/scan/task.rs:56-60` (plus inherent `from_data_file` used by it).
- **Problem:** trait impls have no visibility, so this `From` is permanently public; only the
  inherent constructor needs in-crate visibility.
- **Change:** delete the `From` impl, keep an inherent `pub(crate) fn from_data_file`; consider
  `#[non_exhaustive]` on the struct (crate precedent: `error.rs`, `expr/mod.rs`,
  `encryption/{io,manager}.rs`).
- **Effort** S · **Risk** L · **Verify:** `public-api.txt` shrinks, full test build.

**12. [api] Six `ScanMetrics::runtime_*` getters are justified but listed twice**
- **Where:** `crates/iceberg/src/arrow/scan_metrics.rs:113-` (getters); used cross-crate by Comet
  `iceberg_scan.rs:392-413`; both copies appear in the `public-api.txt` diff (lines 10–15 and 46–51).
- **Problem:** the getters themselves are correct (`pub` needed for a cross-crate consumer), but
  the baseline file appears to carry duplicate entries, which will make every upstream API review
  noisier.
- **Change:** keep the getters `pub`; regenerate `public-api.txt` and confirm each line appears once.
- **Effort** S · **Risk** L · **Verify:** regenerate, `diff` is minimal.

**13. [api] `RuntimePredicateProvider` / `RuntimePredicateSnapshot` / `with_runtime_predicate_provider` — decide the shape now**
- **Where:** `crates/iceberg/src/arrow/reader/runtime_predicate.rs:31-35` (snapshot), `:131`
  (trait); `crates/iceberg/src/arrow/reader/mod.rs:138` (builder method); the doc at
  `reader/mod.rs:135-137` admits `TableScan::to_arrow` cannot accept a provider and calls it "a follow-up".
- **Problem:** this is the most expensive surface to change later; leaving the gap undocumented-as-a-decision
  invites an upstream request to wire it through `TableScanBuilder` in the same series.
- **Change:** either add the `TableScanBuilder` plumbing in the same unit or write the
  follow-up explicitly as future work in the doc; land this unit **last** in the stack so the
  shape can still move.
- **Effort** S (document) / L (plumb) · **Risk** M · **Verify:** `cargo test --doc`, `cargo doc --no-deps`.

**14. [api] `FileScanTaskMetrics::new` takes six positional `HashMap`s**
- **Where:** `crates/iceberg/src/scan/task.rs:70-`; called from Comet `planner.rs:4521-4536`.
- **Problem:** positional 6-arg constructor is brittle to field addition and unreadable at the
  call site; all fields are already `pub(crate)`.
- **Change:** keep `pub` but prefer a named/builder constructor (or `Default` + setters) and
  migrate the Comet call site in the same series.
- **Effort** S · **Risk** L · **Verify:** both call sites, `public-api.txt`.

---

## Tier 3 — Test-only code in production (items 15–17)

**15. [tests-in-prod] `#[cfg(test)]` semaphore inside `DeleteFilter`**
- **Where:** `crates/iceberg/src/arrow/delete_filter.rs:66-67` (field), `:122-123` (ctor),
  `:128-134` (`wait_for_load_waiters`), `:204` and second `add_permits` site in the loading path;
  test consumer `caching_delete_file_loader.rs:~1281`.
- **Problem:** test instrumentation threaded through production control flow; the struct has a
  different shape under test than in release builds, so tests exercise code production never runs.
- **Change:** move the barrier out — e.g. poll the cache's observable state in the test, or inject
  an observer hook that exists only in test constructors — and delete the field plus both
  `add_permits` sites.
- **Effort** M · **Risk** M (concurrency tests) · **Verify:** delete/caching tests repeated
  (`cargo nextest run -p iceberg -- <names> --repeat 20`) and `cargo build --release` shows no field.

**16. [tests-in-prod] `#[cfg(test)]` method on the production `IcebergScanExec` impl**
- **Where:** Comet `native/core/src/execution/operators/iceberg_scan.rs:118-122`
  (`runtime_predicate_field_name`), used only by tests at `:1159-1333`.
- **Problem:** test helper lives in the production `impl` block (it is compiled out in release,
  but it advertises a test API on the type).
- **Change:** move into `mod tests` (free function taking `&IcebergScanExec`) and delete the method.
- **Effort** S · **Risk** L · **Verify:** `cargo test -p comet` (names unchanged elsewhere).

**17. [tests-in-prod] 2,726-line single test module**
- **Where:** `crates/iceberg/src/arrow/reader/runtime_predicate_tests.rs` (104 KB), included at
  `reader/mod.rs:48-49` under `#[cfg(test)]`.
- **Problem:** one file mixes semantics, concurrency, page-index, metrics and serde coverage;
  reviewers cannot tell which area a change touches.
- **Change:** split by concern (`runtime_predicate_semantics_tests.rs`,
  `..._concurrency_tests.rs`, `..._page_index_tests.rs`, `..._metrics_tests.rs`) — pure file move.
- **Effort** S · **Risk** L · **Verify:** `git diff --color-moved` shows no content change; test count unchanged.

---

## Tier 4 — Structure: functions over the size budget (items 18–19)

**18. [structure] `FileReadPipeline::process` is 681 lines**
- **Where:** `crates/iceberg/src/arrow/reader/pipeline.rs:167-847` (merge-base ≈ 500; grew ~180).
- **Problem:** far past the ~150-line budget; it now interleaves plan-time predicate application,
  delete-file loading, runtime-predicate adoption, decoder refresh, and streaming.
- **Change:** extract named phases as private methods (`plan_and_filter`, `load_deletes`,
  `attach_runtime_predicate`, `stream_with_refresh`) with no logic changes; keep `file_always_matches`
  (`:970`) and `plan_predicate` (`:1019`) where they are.
- **Effort** M · **Risk** M (hottest path in the crate) · **Verify:** full `cargo test -p iceberg`,
  round-trip integration tests, `cargo clippy`.

**19. [structure] `apply_predicate_to_column_index` is 195 lines**
- **Where:** `crates/iceberg/src/expr/visitors/page_index_evaluator.rs:261-452`.
- **Problem:** over budget; dispatch over column types, null pages, ranges and blooms in one body.
- **Change:** split into per-concern helpers (`null_page_range`, `column_index_ranges`,
  `bloom_may_contain`) called from one dispatcher.
- **Effort** M · **Risk** M · **Verify:** page-index tests unchanged.

*(Everything else non-test stayed under budget: nearest are
`caching_delete_file_loader.rs:load_file_for_task` 122 lines, `pipeline.rs:filter_row_groups_by_bloom_filter`
114, Comet `iceberg_scan.rs:execute_with_tasks` 138, `CometScanRule.runtimeFilterColumns` (`:1223`) ~108.
`check_runtime_predicate_semantics` (`runtime_predicate.rs:278-303`) is only 25 lines — earlier
"177 lines" notes about it were a measurement error.)*

---

## Tier 5 — Duplication (items 20–29)

**20. [dup] The supported key-type allowlist exists in six-plus places and already disagrees**
- **Where:** `spark/.../rules/CometScanRule.scala:1242-1248` (`directKeyAttribute`),
  `spark/.../serde/operator/CometIcebergNativeScan.scala:1248-1249` (same list inline),
  `spark/.../sql/comet/CometLocalTopKExec.scala:48` (`firstKeyEligible`),
  `spark/.../comet/iceberg/IcebergReflection.scala:472-479` (stringly `"int","long","date","timestamp","timestamptz"`),
  Comet `native/.../dynamic_filter/parquet_reader.rs:57` (`is_parquet_reader_key`),
  `planner.rs:3153` / `:3181` (Min/Max arms), Iceberg `runtime_predicate.rs` + `predicate_visitor.rs`.
- **Problem:** drift is already visible: Scala accepts `TimestampNTZType`/any `TimestampType`
  while `runtimeKeyColumns` accepts only microsecond Iceberg types, so the driver can retain
  statistics for columns the native reader will never use (wasted work at best, confusing
  eligibility at worst).
- **Change:** one documented spec per layer: a single Scala helper used by both Scala call sites
  (drop the inline copy in `CometIcebergNativeScan.scala`), plus a cross-reference comment table
  tying the Scala set to the Rust sets, plus a test asserting the Scala sets agree with each other.
- **Effort** M · **Risk** M · **Verify:** unit test comparing the two Scala predicates; existing
  native eligibility tests.

**21. [dup] Bounds/prefix logic triplicated across evaluators**
- **Where:** prefix logic at `inclusive_metrics_evaluator.rs:472`, `row_group_metrics_evaluator.rs:461`,
  `page_index_evaluator.rs:833`; the "both bounds at once" logic at `inclusive_metrics_evaluator.rs:505`
  and `row_group_metrics_evaluator.rs:500`.
- **Problem:** three copies of the same decision tree; a future fix must be applied three times
  (exactly how item 1 could regress in one of them).
- **Change:** shared helper module (e.g. `expr/visitors/bounds.rs`) taking `(lower, upper, literal,
  prefix_len)` and returning the three-way decision; call it from all three.
- **Effort** M · **Risk** M (pruning correctness) · **Verify:** land **after** item 1, keep every
  evaluator test green, add one table-driven test for the helper.

**22. [dup] `InSet` carries three identical string variants**
- **Where:** `crates/iceberg/src/arrow/reader/predicate_visitor.rs:668-681`
  (`Utf8`/`LargeUtf8`/`Utf8View` all hold `FnvHashSet<String>`), build arms `~721-746` (two
  `unreachable!()`s), mask arms `~809-834`.
- **Problem:** three variants that differ only in how the column is read, so every match has
  triplicate arms; also the string build arm **silently skips** null literals while the `collect!`
  macro path returns `Ok(None)` for a narrowing-cast null with a comment explaining why skipping
  would change semantics — the two paths disagree unless a comment justifies it.
- **Change:** collapse to one `Strings(FnvHashSet<String>)` variant that carries the column
  `DataType` (the timestamp variants already do this); make null-literal handling identical
  between macro and string arms, or document why strings can never null-cast.
- **Effort** M · **Risk** M · **Verify:** predicate-visitor tests + a new test with a
  null-producing literal per string type.

**23. [dup] `Option<Option<InSet>>` + `.as_ref().unwrap()`**
- **Where:** `crates/iceberg/src/arrow/reader/predicate_visitor.rs:254` (declaration),
  `:264-268` (`set.as_ref().unwrap().as_ref().and_then(...)`).
- **Problem:** outer = "built yet?", inner = "supported type?" — the intent is invisible and
  requires an `unwrap()` on a path that runs per batch.
- **Change:** a three-state enum (`Unbuilt` / `Unsupported` / `Ready(InSet)`) or a `OnceLock`;
  no `unwrap`.
- **Effort** S · **Risk** L · **Verify:** predicate-visitor tests, `cargo clippy`.

**24. [dup] Threshold `8` appears twice under two names**
- **Where:** `predicate_visitor.rs:665` (`IN_SET_THRESHOLD`) vs
  `caching_delete_file_loader.rs:803` (`SingleColumnEqualityDeletes::COMPARISON_LIMIT`).
- **Problem:** same magic value, different vocabulary, no shared rationale — a reviewer cannot
  tell whether they are meant to move together.
- **Change:** align the naming (both about "when a linear comparison pass stops being cheap")
  and add a one-line rationale to each pointing at the other, or hoist one shared constant if
  the crates allow.
- **Effort** S · **Risk** L · **Verify:** comment-only diff.

**25. [dup] Error-wrapping pattern duplicated**
- **Where:** `crates/iceberg/src/arrow/delete_filter.rs:226` (`cached_load_error`) vs the
  stat-error wrapper in `delete_file_loader.rs:~85-96` (kind + retryable + context + source).
- **Problem:** two hand-rolled "wrap preserving kind/retryability" helpers; easy to fix one and
  not the other.
- **Change:** one shared helper next to `Error::with_retryable` (or a `context_retryable` method)
  used by both.
- **Effort** S · **Risk** L · **Verify:** error-shape tests (kind/retryable preserved).

**26. [dup] Three type-promotion decision points**
- **Where:** `predicate_visitor.rs:833` (`promote_column_for_literal`),
  `record_batch_transformer.rs:138` (`column_needs_type_promotion`), plus the transformer's cast path.
- **Problem:** "does this column need widening for this literal/output" is decided three times
  with subtly different rules; divergence = wrong comparisons or wrong casts.
- **Change:** single module exposing `needs_promotion(source, target) -> Option<DataType>`; have
  the predicate and transformer both call it (behavior-preserving: assert identical outcomes in a
  table test before switching).
- **Effort** M · **Risk** M · **Verify:** table test covering every promotion pair both before
  and after.

**27. [dup] `create_agg_expr` repeats the Min/Max `direct_minmax` block**
- **Where:** Comet `native/core/src/execution/planner.rs:3128` (param), `:3153-3166` (Min arm),
  `:3181-3190` (Max arm); call site `:1594`/`:1605`.
- **Problem:** two near-identical condition/expression construction blocks in one function.
- **Change:** extract `direct_minmax_expr(child, name, schema)` and call it from both arms.
- **Effort** S · **Risk** L · **Verify:** aggregate dynamic-filter tests, plan-assertion tests.

**28. [dup] Metric names are string-duplicated across Rust and Scala**
- **Where:** native `iceberg_scan.rs:451-461` (six counters) ↔
  `spark/.../sql/comet/CometIcebergNativeScanExec.scala:224-234` (name map), and
  `CometMetricNode.scala:518-530` for the join family.
- **Problem:** a rename on one side silently blanks the Spark-side metric; nothing enforces the pairing.
- **Change:** document the pairing at both ends and add a test that asserts every native counter
  name is present in the Scala map (parse the Rust constants or keep a shared list).
- **Effort** S · **Risk** L · **Verify:** new test fails if a name is removed on either side.

**29. [dup] Two metric namespaces for one feature**
- **Where:** new counters use `iceberg_runtime_*` (`iceberg_scan.rs:451-461`) while the rest of
  the feature uses `dynamic_filter_{join,topk,minmax}_*` (new: `dynamic_filter_join_bypass_switches`,
  `dynamic_filter_minmax_filters_attached`; join/topk families largely pre-exist at merge-base).
- **Problem:** one feature, two prefixes, no stated rule; renaming later breaks dashboards.
- **Change:** write the naming rule (operator-scoped prefix) in `CometMetricNode.scala` and either
  conform the new counters or explicitly justify the split; any rename is its own unit **before**
  release.
- **Effort** S · **Risk** M (observability) · **Verify:** metric assertions in
  `planner.rs:6259-6266` and `CometIcebergNativeScanExec` tests.

---

## Tier 6 — Nits: unwrap, constants, naming, comments (items 30–40)

**30. [nit] `unwrap()` immediately after insert**
- **Where:** Comet `native/core/src/execution/operators/iceberg_scan.rs:593`
  (`&self.cached.as_ref().unwrap().projection_exprs`).
- **Change:** bind the inserted value and reuse it; no `unwrap`. **Effort** S · **Risk** L ·
**Verify:** `cargo clippy`, scan tests.

**31. [nit] Three `.expect("decoder exists while streaming")` in production**
- **Where:** `crates/iceberg/src/arrow/reader/runtime_stream.rs:129`, `:317`, `:343`.
- **Problem:** state-machine invariant expressed as panics; fine if documented, brittle if the
  refresh path ever learns to run without a decoder.
- **Change:** keep the `expect`s but state the invariant once on the struct field, or return an
  internal error at `:343`. **Effort** S · **Risk** L · **Verify:** streaming tests.

**32. [nit] Anonymous bounded retry**
- **Where:** Comet `native/.../dynamic_filter/iceberg_reader.rs:79` (`for _ in 0..3`, comment
  explains fail-open).
- **Change:** `const SNAPSHOT_READ_RETRIES: usize = 3;` and mention the failure mode (fail open,
  no pruning) in the doc. **Effort** S · **Risk** L · **Verify:** comment/const-only diff.

**33. [nit] Magic constants: rationale uneven**
- **Where:** `iceberg_reader/predicate.rs:77` (`MAX_IN_LIST_LITERALS = 1024`), `join.rs:299-300`
  (`IN_LIST_PUSHDOWN_MAX_BYTES = 16 * 1024`, `IN_LIST_PUSHDOWN_MAX_DISTINCT = 1024`),
  `batch_filter.rs:44/49/54` (65_536 / 0.10 / 16 — these three *do* have good rationale comments),
  `delete_file_loader.rs:78` (size-0 sentinel) and `:98` (< 8 footer minimum — justified by comment).
- **Problem:** the two unrelated `1024`s and the `16 KiB` have no stated origin or unit.
- **Change:** one-line rationale each (or a single `limits` module with the three constants
  cross-referenced). **Effort** S · **Risk** L · **Verify:** comment-only diff.

**34. [nit] Metric counter incremented before the work it names**
- **Where:** `crates/iceberg/src/arrow/scan_metrics.rs:91-100` (`record_runtime_refresh`
  increments `runtime_decoder_rebuilds`), called at `runtime_stream.rs:191` immediately before
  `rebuild_decoder` at `:192`.
- **Problem:** `rebuild_decoder` has no other caller, so `rebuilds == refreshes` holds by
  construction — but on a failing refresh the counter still counts a rebuild that did not happen,
  and the doc comment ("Each refresh costs exactly one") describes intent, not the error path.
- **Change:** move the increment after the successful call, or reword the doc to say the counter
  tracks refreshes. **Effort** S · **Risk** L · **Verify:** metrics unit test.

**35. [nit] Vocabulary drift: filter vs predicate vs statistics**
- **Where:** `CometScanRule.scala:1223` (`runtimeFilterColumns`), `:652`
  (`runtimeStatisticsColumns`), `IcebergReflection.scala:472` (`runtimeKeyColumns`), `:501`
  (`runtimeFileStatistics`), native `runtime_predicate_*`, metrics `iceberg_runtime_*`.
- **Change:** pick "runtime predicate" (native) + "runtime statistics columns" (driver) and
  rename locals/params to match; no serialized names involved. **Effort** S · **Risk** L ·
**Verify:** `git grep` shows one vocabulary per concept.

**36. [nit] New conf doc style**
- **Where:** `spark/.../comet/CometConf.scala:576-583`
  (`spark.comet.exec.aggregate.dynamicFilter.enabled`).
- **Problem:** naming matches the existing `join.dynamicFilter`/`topK.dynamicFilter` pattern
  (good), but the doc omits the `$TUNING_GUIDE` reference neighbors use and hardcodes
  "Iceberg"-only support in prose.
- **Change:** align doc structure with adjacent entries. **Effort** S · **Risk** L ·
**Verify:** conf docs render.

**37. [nit] Comment referencing branch context**
- **Where:** `native/proto/src/lib.rs:6175` ("Main already assigns…").
- **Problem:** the only comment in either diff that reasons about a branch rather than the code.
- **Change:** restate as the invariant ("the driver assigns `dynamic_filter_id`; native must not
  reassign it"). **Effort** S · **Risk** L · **Verify:** comment-only diff.

**38. [nit] Reflection-driven re-plan has no direct test**
- **Where:** `spark/.../comet/iceberg/IcebergReflection.scala:501-` (`runtimeFileStatistics`,
  re-plans the scan to retain statistics; fail-open documented), wired at `:2446`, `:2515-2519`;
  no reference found under `spark/src/test`.
- **Problem:** the most fragile driver-side code (string/method reflection) is covered only
  indirectly.
- **Change:** extract the reflection calls behind a small interface and add one focused test
  (happy path + missing-method fail-open). **Effort** M · **Risk** L · **Verify:** new test.

**39. [nit] `dynamic_filter/tests.rs` deletion is a pure move**
- **Where:** Comet `native/core/src/execution/operators/dynamic_filter/tests.rs` (deleted, 125
  lines) → `batch_filter/tests.rs`.
- **Problem:** deletions in a mixed diff invite "what behavior changed here?" review cycles.
- **Change:** land as its own mechanical commit verified with `git diff --color-moved`.
- **Effort** S · **Risk** L · **Verify:** `--color-moved` shows green/moved-only.

**40. [nit] Keep the `Self::cached` pattern consistent**
- **Where:** Comet `iceberg_scan.rs` (cache fill then read-back; the read-back unwrap is item 30).
- **Problem:** two idioms in one function (`Self::cached()` helper vs field read) make the
  no-unwrap fix look optional.
- **Change:** one idiom throughout the file while fixing item 30. **Effort** S · **Risk** L ·
**Verify:** `cargo clippy`.

---

## Checked — no findings

- **Dead code / unused params:** none found. Every function whose name appears only once in the
  diffs is a test function; no non-test private fn lacks a second caller.
- **Added `#[allow(dead_code)]`:** none in either diff (pre-existing allows on the strict
  evaluator were mostly *removed*).
- **Fork-context comments / TODO / FIXME / HACK / "workaround":** none, except item 37.
- **Missing docs on new `pub` items:** none — `RuntimePredicateProvider`, `RuntimePredicateSnapshot`,
  `FileScanTaskMetrics`, `file_metrics()`, `include_column_stats{,_for}`,
  `with_runtime_predicate_provider` and all six `ScanMetrics::runtime_*` getters carry doc
  comments, and the provider trait carries a runnable example.
- **Conf naming:** `aggregate.dynamicFilter.enabled` matches the existing `join`/`topK` convention.
- **`runtime_predicate_tests` module:** correctly `#[cfg(test)]`-gated (`reader/mod.rs:48-49`).
- **Functions >150 lines:** only items 18 and 19.
- **New `pub` justified:** `FileScanTaskMetrics::new` (`planner.rs:4536`), `file_metrics()`, the
  six `ScanMetrics` getters (Comet `iceberg_scan.rs:392-413`), and the provider trait are used
  cross-crate and must stay `pub`.

---

## Ordered split into independently reviewable upstream units

Each unit is meant to be reviewable alone (builds, tests green, no dependency on an unlanded
unit except where stated). Order below is the recommended landing order; `[iceberg]`/`[comet]`
marks the repo.

**Phase 0 — mechanical prep (no behavior)**

- **U0a `[comet]` Move `dynamic_filter/tests.rs` → `batch_filter/tests.rs`** (item 39).
  *Deps:* none.
- **U0b `[iceberg]` Split `runtime_predicate_tests.rs` by concern** (item 17).
  *Deps:* none.

**Phase 1 — behavior fixes, isolated (each its own unit)**

- **U1 `[iceberg]` Both-bounds `IN` fix** (item 1) — *Deps:* none.
- **U2 `[iceberg]` Strict `may_contain_nan`** (item 2) — *Deps:* none.
- **U3 `[iceberg]` `ProjectionMask::none` for zero-column scans** (item 3) — *Deps:* none.
- **U4 `[iceberg]` Decimal32/64/256 + Time64 schema/scalar** (item 4) — *Deps:* none.
- **U5 `[iceberg]` Page-index UINT32/compatibility widening** (item 5) — *Deps:* none.
- **U6 `[iceberg]` `iceberg_partition_value` expectation change** (item 9) — *Deps:* none;
  land with (or before) whatever dependency bump it tracks.
- **U7 `[comet]` Metadata-table class-vs-name detection** (item 6) — *Deps:* none.
- **U8 `[comet]` `Utils.BatchIpcChunkSize` + benchmark** (item 7) — *Deps:* none.
- **U9 `[comet]` `Utils` writer lifecycle / `normalizeBatchOffsets`** (item 8) — *Deps:* none;
  U8 and U9 touch the same file, so land them sequentially to avoid conflicts.

**Phase 2 — API surface freeze (before any cross-crate consumer PR is reviewed)**

- **U10 `[iceberg]` Shrink the new API: demote `include_column_stats{,_for}`, drop
  `From<&DataFile>`, dedupe `public-api.txt`, adjust `FileScanTaskMetrics::new`** (items 10, 11,
  12, 14). *Deps:* none, but **must precede U11** so Comet reviews against the final surface.
- **U11 `[iceberg]` Provider trait + snapshot + `with_runtime_predicate_provider` with the
  `TableScan` gap decided** (item 13). *Deps:* U10 (surface stability); land near the end of the
  stack so the shape can still change.

**Phase 3 — structural refactors (after phase 1 has strengthened tests)**

- **U12 `[iceberg]` Split `pipeline.rs::process` into phases** (item 18). *Deps:* U1, U5 (same
  read path — split after the fixes so the phase tests exist).
- **U13 `[iceberg]` Split `apply_predicate_to_column_index`** (item 19). *Deps:* U5 (same file).
- **U14 `[iceberg]` Extract shared bounds/prefix helper** (item 21). *Deps:* U1 (the fix must
  land first so all three copies are already correct).

**Phase 4 — duplication cleanup**

- **U15 `[iceberg]` `InSet` collapse + set-cache enum** (items 22, 23). *Deps:* U1 if the same
  test file conflicts; otherwise independent.
- **U16 `[iceberg]` Shared promotion decision point** (item 26). *Deps:* U4 (decimal types must
  exist first).
- **U17 `[iceberg]` Error-wrapping helper + threshold-8 naming/rationale** (items 24, 25).
  *Deps:* U12 if `delete_filter.rs` is touched by the split; otherwise independent.
- **U18 `[comet]` `create_agg_expr` Min/Max helper** (item 27) — *Deps:* none.
- **U19 `[comet]` Key-type allowlist: single Scala helper, drop the inline copy, agreement test,
  cross-repo reference table** (item 20). *Deps:* U7 (same file as `directKeyAttribute`), U18
  (touches the Rust arms in `planner.rs`).
- **U20 `[comet]` Metric naming: pairing test + namespace rule** (items 28, 29). *Deps:* none;
  **must precede any release** because renames get harder afterward.

**Phase 5 — test-only code removal**

- **U21 `[iceberg]` Remove the `cfg(test)` semaphore from `DeleteFilter`** (item 15).
  *Deps:* U12 (pipeline split first, so the loader tests move once).
- **U22 `[comet]` Move `runtime_predicate_field_name` into tests + `cached` unwrap/idiom fixes**
  (items 16, 30, 40). *Deps:* U18 (same file as the planner cleanup).

**Phase 6 — nits (can be batched, but keep each mechanically pure)**

- **U23 `[iceberg]` Doc/const nits: decoder expects, retry const (comet), counter wording,
  threshold comments, proto comment** (items 31, 32, 33, 34, 37). *Deps:* U12, U15.
- **U24 `[comet]` Vocabulary + conf-doc alignment** (items 35, 36). *Deps:* U19 (rename while
  touching those names anyway).
- **U25 `[comet]` `IcebergReflection.runtimeFileStatistics` seam + test** (item 38).
  *Deps:* U19 (shares `runtimeKeyColumns` names).

**Cross-repo dependency (the only hard one):** every `[comet]` unit that compiles against the
iceberg crate (`U10`→consumers, U11, and the existing `FileScanTaskMetrics`/`ScanMetrics`/
provider call sites in `planner.rs` and `iceberg_scan.rs`) must be reviewed *after* its
`[iceberg]` counterpart lands upstream. Everything else is repo-local.
