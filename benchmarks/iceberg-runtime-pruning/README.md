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

# Native Iceberg runtime pruning benchmark

`.github/workflows/iceberg_adaptive_benchmark.yml` compares three variants on one
GitHub Actions runner against the same plain-Spark-generated tables:

| Variant | Comet build              | Runtime pruning |
| ------- | ------------------------ | --------------- |
| `main`  | Resolved baseline commit | Enabled         |
| `off`   | Workflow checkout commit | Disabled        |
| `on`    | Workflow checkout commit | Enabled         |

Top-K fusion remains enabled for every native variant. Only join, Top-K and
aggregate runtime-filter flags change between `off` and `on`. Baseline versus
candidate includes all implementation and locked-dependency differences; it does
not isolate a dependency change. Plans and reader counters establish adoption.
Plain Spark supplies an independent exact result and schema oracle for each query.

## GitHub Actions only

Run all builds, tests, formatting and benchmarks through Actions. The separate
`iceberg_runtime_pruning_harness.yml` workflow runs fast synthetic Python unit
tests and compiles scripts without importing Spark. It rejects missing variants,
queries, rounds, repetitions, duplicate identities, altered SQL, mismatched
checksums/counts/schemas, malformed JSON and invalid timings before any speedup
is printed. In particular, identical `main` and `on` checksums with missing `off`
fail. The manifest is declared from the selected catalog before measurement;
coverage is never inferred from available samples.

The benchmark runs on `adaptive-bench/**` pushes and supports manual dispatch.
Dispatch defaults to one million rows, eight writer partitions, three rounds,
two repetitions and the nine-query `smoke` profile. Pushes retain the original
16-million-row, 16-partition, six-round, three-repetition protocol. Both use a
single benchmark runner after two release builds. The benchmark timeout is four
hours; this is a cost ceiling, not a measured expected duration. Use the fast
synthetic workflow first, then a smoke dispatch before increasing coverage.

Set `baseline_repository=unikdahal/datafusion-comet` and `baseline_ref` to a full
before-fix commit SHA for a baseline/final comparison. For example, use
`4515c036c35286c98b37da690fbf84bba5a209b6` to compare this hardening against its
starting implementation. The candidate is the dispatched workflow ref. A
separate dispatch with `apache/datafusion-comet` and `refs/heads/main` compares
against Apache main, resolved once per run. Retain each resolved manifest.
Both sides build with identical release commands and their own locked dependencies;
neither build substitutes a dependency or changes native feature flags.

## Data and workload profiles

Generation uses Spark 3.5.9 and Iceberg Java 1.8.1 without Comet. Every native
variant and the Spark oracle read those same data and metadata bytes.
`BENCH_ROWS`, `BENCH_FILES`, `BENCH_ROW_GROUP_BYTES` and `BENCH_SPLIT_BYTES`
control row count, requested writer partitions, row-group target and Iceberg split
target. Actual counts and file sizes are recorded; writer partition count is not
a promise of exactly that many physical files or scan splits. The unsorted
ordering seed is 7; range partitioning can sample when generating layouts, so
retain the snapshot hashes rather than assuming regenerated files are identical.

Both profiles generate:

- `fact_sorted`: integer IDs, bigint values and SHA-256 string payloads, range
  partitioned and sorted by ID.
- `fact_pos_deletes`: that layout with merge-on-read position deletes for every
  101st ID.
- `fact_unsorted`: the same rows in seeded random order. Broad bounds provide an
  overhead control, though exact membership/row pruning can still remove rows.
- `dim`: 128 even keys in a narrow range starting at 60% of the key space.

`smoke` retains the original nine workloads: selective broadcast joins and
ascending Top-K on sorted, unsorted and position-deleted tables; MIN on sorted
and unsorted tables; and an ordinary scan control with no runtime producer.

`extended` adds nine workloads: MAX on sorted/unsorted tables; descending Top-K;
join, Top-K, MIN and scan control on an INT-to-LONG evolved table; and full-domain
non-selective joins on sorted/unsorted data. The full-domain dimension uses a
shuffled hash join to avoid broadcasting millions of keys. Exchanges can block
reader attachment, so inspect plans and metrics before attributing reader savings.
The evolved table retains old INT files, promotes its ID field to LONG and appends
new LONG files. Missing physical compatibility must retain ordinary execution.

For task/file investigations, dispatch controlled layouts with one writer partition
and a large split target, one partition and a 1 MiB split target, then 16/128
partitions. For concurrency, compare the same protocol at reader concurrency 1,
4 and 8 in separate matched runs. Repeated generation can change file layout;
use within-run comparisons as the primary evidence and inspect hashes/counts
before attributing differences across runs.

Equality deletes, delete-heavy mixes, multiple historical schemas, strings,
decimals, TPC-H and deterministic fuzz remain covered by the
[historical detailed campaign](https://github.com/unikdahal/datafusion-comet/blob/a591c3700e2fa4094500eee53ecc8f40ad3e8cce/benchmarks/iceberg-runtime-pruning-detailed/README.md).
This smaller harness does not replace that corpus or validate its totals. Retain
its exact harness revision, query catalog, seeds, data artifacts, resolved builds,
oracle and raw timing/warmup records when rerunning a targeted selection.

## Measurement and retained evidence

Each round starts a separate JVM for each variant in rotated order. Each JVM
warms every query once, then rotates query order for timed repetitions. Multiples
of three rounds balance variant positions. `plan_ms` includes SQL construction,
analysis and physical planning; `exec_ms` measures collection; `total_ms` is their
sum. The planning timer does not isolate manifest replanning, cache hit/miss
latency or executor task serialization. Cold JVMs do not imply cold storage caches.

`experiment.json` declares the SQL and SHA-256 digest for every expected query,
variant, round and repetition. The summary requires the full Cartesian matrix,
exact result counts/checksums/schemas, matching Spark oracle and finite consistent
timings. Warmups abort on errors but are not retained correctness samples.
Correctness uses exact ordered results for these aggregates and unique-ID Top-K
queries; it is not a generic floating-point/multiset oracle for arbitrary SQL.

The summary retains pooled medians, IQR, CV and the original independent bootstrap
95% intervals. It adds paired JVM-round median bootstrap intervals as exploratory
diagnostics, keeping repetitions clustered. This evaluates sensitivity to shared
round noise without changing the published point estimator. Few rounds, fixed
warmup count and multiple comparisons limit inference; no significance or causal
claim follows from an interval alone.

Artifacts retain raw JSONL, oracle records, SQL catalog, harness source/checksums,
resolved revisions, compiler/build flags, Cargo manifest/lockfile, jar/native
library digests, CPU/memory details, effective Spark settings, physical plans and
logs. Data/metadata hashes are checked after measurement, including added/deleted
files; a changed snapshot suppresses summary generation. The warehouse itself is
not uploaded by this inexpensive harness. Hashes prove within-run identity but do
not preserve a complete historical snapshot for later byte-identical replay.

Reader counters cover native Iceberg scans only. Missing metrics are `n/a`, never
zero. Reader-byte ratios require matching nonempty native metadata-location
multisets for every sample; still inspect saved plans for projection/fallback
scope. Counters include bytes, splits, predicate tasks, file pruning, row-group
pruning, live pruning, refreshes and decoder rebuilds. `bytes_scanned` counts
requested ranged I/O including metadata/deletes, not physical disk/network traffic.
Live pruning and byte totals can vary with scheduling. File-task counts include
splits and are not distinct physical-file counts.

Driver statistics collection time/cache hit rate, peak native/driver memory,
time to first bound, files in flight at publication and binding failure/cache
counts are not isolated by this harness. Do not substitute query planning time,
requested bytes or decoder rebuilds for those measurements.

These are warm local-disk measurements on shared hosted runners. They do not
establish cold S3/GCS/Azure latency, request charges, retry/cancellation behavior or
production throughput. No object-store credentials are provisioned here. Such
validation needs a separately configured Actions environment, isolated fixtures,
an explicit request/byte budget and matched storage conditions.
