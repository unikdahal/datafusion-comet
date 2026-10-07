<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Native Iceberg runtime pruning

Comet connects completed join domains and tightening TopK/MIN/MAX bounds to
Iceberg's Arrow reader. Iceberg Java continues to plan `FileScanTask`s; Comet
keeps Spark schema adaptation and operator execution; iceberg-rust owns
file access, physical decoding, schema evolution and deletes. The original
join, sort or aggregate still determines the exact result.

```text
Spark / Iceberg Java tasks
  -> Comet execution-local producer + RuntimePredicateProvider
  -> iceberg-rust ArrowReader: file -> row-group -> page -> row pruning
  -> Spark batch adaptation -> original join / sort / aggregate
```

## Safety contract

- A published predicate may discard only rows that can never affect the result.
  Publications tighten monotonically; generations increase after publication.
  `None` stops new adoption and cannot restore work already skipped. Comet takes
  bounded, generation-bracketed snapshots; readers cache both successful and
  failed bindings by generation, schema and case policy.
- Snapshot, binding, statistics, planning and page-pruning failures are advisory:
  ignore the runtime predicate and retain the planned filters and deletes.
  Actual decoding and row-evaluation errors propagate. Advisory failure never
  converts a planned-filter error into success.
- Moving pruning below an expression requires that expression to be row-local
  and infallible. Determinism alone is insufficient. Casts, arbitrary functions,
  overflow-prone arithmetic and variable division remain boundaries. Secondary
  TopK sort keys obey the same rule, so a later file's division-by-zero cannot
  disappear behind a first-key bound. Fetch limits are boundaries too.
- Nulls remain possible when counts are unknown. TopK preserves null-first
  arms and first-key ties for multiple sort keys. Whole-file proofs may remove
  a redundant row filter only when Arrow's null semantics agree; negative
  equality/membership predicates require explicit zero null counts for that
  optimization. Floating comparisons and sets fail open because statistics and
  Arrow total-order comparisons disagree on NaNs and signed zeros. The reader's
  floating null/NaN tests remain supported.
- Binding follows field IDs, task schemas and case policy. Comet requires a
  consistent top-level field identity and matching type across tasks. Missing
  physical fields and table type promotions disable runtime physical filtering;
  Iceberg still applies defaults and output evolution. Row comparisons perform
  supported lossless physical numeric widening before comparison. Page bounds
  must use the same value domain: narrowing a wide literal or interpreting an
  `INT32` bound as a differently encoded value is unsafe. Regression coverage
  includes out-of-range literals, narrow physical integers and page pruning.
- Position deletes retain physical row positions before filtering. Page and
  positional selections remain local to each row group through refreshes and
  reordered reads. Equality deletes, deletion vectors, encryption and byte-range
  splits continue through the existing Iceberg reader path.

The attachment and expression rules are in
[iceberg_reader.rs](../native/core/src/execution/operators/dynamic_filter/iceberg_reader.rs).
The reader contracts and schema checks are in
[runtime_predicate.rs](https://github.com/unikdahal/iceberg-rust/blob/adaptive-ci/runtime-pruning-rewrite-20261006/crates/iceberg/src/arrow/reader/runtime_predicate.rs),
[pipeline.rs](https://github.com/unikdahal/iceberg-rust/blob/adaptive-ci/runtime-pruning-rewrite-20261006/crates/iceberg/src/arrow/reader/pipeline.rs)
and [page_index_evaluator.rs](https://github.com/unikdahal/iceberg-rust/blob/adaptive-ci/runtime-pruning-rewrite-20261006/crates/iceberg/src/expr/visitors/page_index_evaluator.rs).

## Attachment and lifecycle

Join pruning supports a single direct, matching signed-integer key; dates and
microsecond timestamps additionally reach native Iceberg readers. Eligible
inner and semi joins use null-unequal semantics and one native partition per
input. Outer/anti joins, computed keys and unsupported types retain their
ordinary execution. Join residuals still run on exact key matches.

TopK requires a direct supported first key and positive fetch. MIN/MAX requires
one ungrouped partial aggregate with a direct supported argument, without
distinct, aggregate filters or aggregate ordering. Safe projections and filters
can be traversed while preserving the key's column mapping. Parquet attachment
keeps its separate schema-adapter and statistics safety checks.

Permanent wrappers retain unexecuted templates. Each execution creates a fresh
producer and connected reader consumer; completed build domains, hash tables
and TopK thresholds do not survive into the next execution. EOF, error and
cancellation release stream-owned state. Reset detaches a scan's old provider
and ordering. Native Iceberg scans reject nonzero partition indices.

See [join.rs](../native/core/src/execution/operators/dynamic_filter/join.rs),
[topk.rs](../native/core/src/execution/operators/dynamic_filter/topk.rs),
[aggregate.rs](../native/core/src/execution/operators/dynamic_filter/aggregate.rs)
and [iceberg_scan.rs](../native/core/src/execution/operators/iceberg_scan.rs).

## Optimization rationale

| Change | Work avoided |
| --- | --- |
| File rejection before opening data or loading deletes | Footer, data and delete-file I/O for provably irrelevant tasks |
| Shared read lock for an unchanged bound predicate | Serializing parallel tasks merely to reuse a binding |
| Generation checks at row-group boundaries | Rebinding and rebuilding on every batch; a changed publication makes one pass over remaining groups and one decoder rebuild |
| Combined planned/runtime Arrow predicate | Decoding shared predicate columns twice |
| Whole-file proof that every row matches | Re-evaluating a redundant runtime row filter |
| Shared tasks, lazy task cloning and in-place ordering permutation | Eager copies of all tasks during ordinary execution and additional complete task copies during sorting |
| Best-first files and descending row groups for MAX/descending TopK | Reading worse candidates before a useful bound exists; unknown bounds retain conservative handling |
| Original join verifies surviving Iceberg rows directly | A second decoded-batch membership lookup and payload-array filtering |
| Compact single-column equality-delete sets above eight entries | Building one expression per delete row; supported larger sets compile membership once and use hash lookup per data row |

These changes reduce specific work; they do not establish a wall-clock speedup.
See [runtime_stream.rs](https://github.com/unikdahal/iceberg-rust/blob/adaptive-ci/runtime-pruning-rewrite-20261006/crates/iceberg/src/arrow/reader/runtime_stream.rs),
[predicate_visitor.rs](https://github.com/unikdahal/iceberg-rust/blob/adaptive-ci/runtime-pruning-rewrite-20261006/crates/iceberg/src/arrow/reader/predicate_visitor.rs)
and [caching_delete_file_loader.rs](https://github.com/unikdahal/iceberg-rust/blob/adaptive-ci/runtime-pruning-rewrite-20261006/crates/iceberg/src/arrow/caching_delete_file_loader.rs).

## Metrics and comparison

`bytes_scanned` counts ranged reader I/O, including metadata and deletes; it is
not a measure of physical disk traffic after operating-system caching. Spark
also receives `iceberg_runtime_predicate_tasks`, `iceberg_runtime_file_tasks_pruned`,
`iceberg_runtime_row_groups_pruned`, `iceberg_runtime_predicate_refreshes`,
`iceberg_runtime_row_groups_pruned_live` and `iceberg_runtime_decoder_rebuilds`.
Row-group counters attribute only additional runtime pruning, excluding groups
already removed by splits, planned filters or deletes. The live count is a
subset of the total. Scan compute time measures active polling/adaptation;
producer metrics measure their own operators. Poll, error and drop paths report
available metric deltas without retaining producer state.

The comparison against the existing runtime implementation is pending CI.
No speedup or regression result is claimed here. The
[benchmark harness](https://github.com/unikdahal/datafusion-comet/blob/adaptive-bench/rewrite-20261006/benchmarks/iceberg-runtime-pruning-detailed/README.md) provides
shared generated fixtures, a plain Spark correctness oracle, four balanced rounds
in fresh JVMs, paired timing intervals, separate planning/execution timings and
reader counters. Six suites cover sorted selective queries, unsorted/control
queries, position/equality deletes, schema evolution, TPC-H and deterministic fuzz
cases, with pinned build revisions and identical settings. Wall-clock distributions,
reader bytes and decoder rebuilds must be considered together; local CI timings
do not establish object-store latency improvements.

## Limits and regression coverage

Comet's attachment is task-local; it does not share domains across Spark
exchanges. Floating keys, general computed keys and fallible expressions receive
no reader attachment. Iceberg membership translation accepts at most 1,024
plain literals; larger unsupported shapes fail open. Existing rows, pages and
groups are never recovered after pruning. Refresh depends on row-group filtering
being enabled. `TableScan::to_arrow()` does not accept a runtime provider; Comet
supplies one directly to `ArrowReaderBuilder`. The live decoder currently uses
the pinned Parquet 59 backport.

Regression suites cover nulls, NaNs, signed zeros, schema evolution, dictionary
columns, missing field IDs, splits, deletes, page selections, concurrent
publications, failing expressions, resets and stream cancellation. Tests live in
[Comet's dynamic-filter suite](../native/core/src/execution/operators/dynamic_filter/)
and [Iceberg's reader suite](https://github.com/unikdahal/iceberg-rust/blob/adaptive-ci/runtime-pruning-rewrite-20261006/crates/iceberg/src/arrow/reader/runtime_predicate_tests.rs).

Local validation of this revision passed 1,909 Iceberg library tests and 689
Comet library tests with default features and the locked, published Iceberg
dependency. Five existing Comet tests were ignored: four require an HDFS cluster
and one is a manual buffer benchmark. The no-default-features run passed 692
tests with one ignored. Comet workspace Clippy checked all targets with warnings
denied; Iceberg library/test Clippy used its configured nightly toolchain with
warnings denied. Rust formatting, Scala formatting and whitespace checks passed.
Spark 3.5/4.1 validation and the matched benchmark are separate CI verdicts.
