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

# Matched runtime pruning benchmark

The workflow `.github/workflows/iceberg_adaptive_benchmark.yml` runs on
`adaptive-bench/rewrite-*` branches. The candidate Comet and Iceberg commits are fixed in
`revisions.json`; benchmark code stays on its own branch. Every workflow first fetches the
latest `apache/datafusion-comet` main commit in a single resolver job, then publishes its
immutable SHA, resolution time and locked Iceberg dependency in `resolved-revisions`.
Both builds and every suite consume that manifest. Main is built without source changes
or dependency substitutions. A rerun through a new workflow dispatch resolves main again;
retrying a failed job inside an existing run retains that run's resolved commit.

Both implementations use Rust 1.99.0, the same release build commands, Spark 3.5.9,
Iceberg Java 1.8.1 and Java 17. Build artifacts record the exact dependency sources,
Cargo lock checksum, compiler, flags and jar checksum. The candidate must already reference
the fixed Iceberg revision; the build does not alter either implementation.

## Comparisons

| Variant       | Implementation | Available join, Top-K and MIN/MAX runtime filters |
| ------------- | -------------- | --------------------------------------- |
| baseline_off  | Latest main    | Disabled                                |
| baseline_on   | Latest main    | Enabled                                 |
| candidate_off | Candidate      | Disabled                                |
| candidate_on  | Candidate      | Enabled                                 |

Top-K fusion stays enabled in all four variants so switching pruning off preserves the
execution algorithm. Main-on versus candidate-on is the headline comparison; each
implementation's off/on comparison measures pruning benefit and overhead. Off/off helps
identify costs unrelated to runtime filtering. Plain Spark supplies the result oracle. Build metadata declares which pruning settings each
revision recognizes; unsupported settings do not imply reader adoption. Actual physical
plans and counters determine whether a query used native Iceberg or fell back.

## Workloads

Each suite generates its tables once without Comet, then every variant reads those same
files on the same runner. Every data and metadata file is hashed before measurement.
Each query shard uses a separate runner, so compare implementations within the matched
shard rather than comparing absolute times between runners.

- `join`: empty, narrow, scattered and full-range build keys; selectivities through 100%;
  broadcast, shuffled hash, sort merge, semi, anti, grouped and star joins; integer, long,
  string, date, decimal, nullable and NaN keys; sorted, unsorted and position-deleted data.
- `topk_minmax`: K from 1 through 100,000; ascending/descending order; null placement,
  filters, offsets, projections and multiple sort keys; MIN, MAX and combined bounds;
  overlapping and reversed file layouts exercise bounds that tighten during scanning.
- Both of those suites include genuine integer and string equality deletes: one Parquet
  delete file containing 4,096 spread keys. The fixture uses Iceberg's Java equality-delete
  writer and commits a v2 row delta, then verifies the remaining row count with plain Spark.
  It samples roughly one million fact rows across the original key range to bound the cost
  of the original expression tree. Full scans, selective joins and Top-K/MIN/MAX exercise
  equality-delete evaluation with and without runtime pruning.
- `layouts`: small row groups, 256 files, wide payloads, skew, partitions, updates,
  snapshots, schema and partition evolution, plus four times the normal row count.
- `fuzz`: 500 deterministic queries per seed across 17 layouts and key types; eight seeds
  produce 4,000 distinct correctness checks for each implementation/configuration.
- `tpch`: all 22 queries at scale factor 3, on natural and date-clustered Iceberg layouts.
- `strjoin`: integer, long and string joins against up to three million build keys, with
  broadcast, shuffled hash and sort merge and with/without DISTINCT.

Unsorted data, full-range builds, expressions without usable bounds, combined MIN/MAX,
grouped aggregates and ordinary scans provide overhead/control cases. Live pruning counters
and decoder rebuilds expose whether a tightening bound arrived in time to save reader work.

## Measurement and correctness

Eight balanced rounds (two complete four-round blocks) run every native variant in a fresh JVM. Each variant occupies each
execution position twice; every ordered adjacent pair appears twice. Each JVM warms every
query four times before two timed repetitions (one for TPC-H/string joins). Fuzz queries run once per
seed for correctness and are excluded from speedup headlines. Query order is deterministically shuffled between
passes, using the same permutation for every variant. SQL construction/analysis, physical planning and collection are timed separately;
headline total time includes all three.

Every run checks its full result and output schema against plain Spark. Integer, decimal,
string, date and nested values remain exact; NaNs and signed zeros use SQL value semantics.
Only TPC-H floating-point values permit relative error `1e-9` or absolute error `1e-8`.
The artifact reports every accepted non-identical digest. No suite is exempt from correctness;
query errors, missing variants, missing records, duplicate records and mismatched SQL fail.
Compact warmup validation records retain every query/pass identity, result digest and failure
without mixing warmup timing into retained samples. A failed or incomplete warmup invalidates
that query in all comparisons. Invalid build provenance suppresses all performance ratios.

The report shows per-query medians and execution IQRs, plus main/candidate and off/on
ratios with paired bootstrap 95% intervals. It resamples independent JVM-round medians,
preserving repetitions as a cluster. Eight CI rounds provide a first comparison; the raw
records support further analysis or repeated runs. Measurements use warm local-disk caches
on shared CI runners. Exploratory bootstrap intervals do not adjust for multiple
comparisons and do not establish a root cause. Driver GC count/time and JIT compilation
time are sampled outside the timed region and retained as diagnostics; these driver-wide
counters include overlapping background work and must not be subtracted from query time.
The extra fixed warmup passes reduce compilation during measurement; compilation diagnostics
still need inspection, since a fixed warmup count cannot guarantee that JIT work has finished.

Reader bytes, file tasks, rows, file pruning, row-group pruning, live pruning, predicate
refreshes and decoder rebuilds are reported per variant. Missing counters are `n/a`, never
zero. Join batch-filter evaluation, rows evaluated and rows pruned are reported separately: an
exchange blocks reader attachment but can still permit filtering of decoded batches. All
exposed SQL metrics are captured by physical stage and physical plans are saved.
Reader byte totals quantify requested ranged I/O; scheduling-dependent pruning is shown
with a range. Byte and output-row counters cover native Iceberg scans only. Main can fall
back to Spark for a fact scan while retaining a counted native dimension scan; its smaller
native byte total then measures a different scope. The report compares metadata locations
and repeated-table counts in saved plans, labels different or unknown native scan coverage,
and lists partial-coverage changes. Such byte totals cannot establish an I/O gain or increase;
correct timings still compare the complete query. Queries with a physical fallback are listed so timing changes can be assessed
against the plan that actually ran.

The `matched-report` artifact contains the full report; `matched-<suite>-shard-<id>` artifacts contain
raw JSONL records, manifests, physical plans, logs, data hashes and runner/build provenance.
The workflow continues collecting diagnostics after a variant fails, then fails its verdict.
If one data producer fails, other suites still run; a shard with no valid snapshot fails,
and complete report coverage remains mandatory.

## Query sharding

The resolver publishes a dynamic matrix and the complete query/SQL-digest catalog before
any benchmark job starts. The default campaign uses 18 shards: `join` 3, `topk_minmax` 5,
`layouts` 2, `fuzz` 2, `tpch` 3 and `strjoin` 3. Maximum benchmark concurrency is 18;
the two builds and six data producers finish before benchmark shards start.

Timed queries are assigned greedily by descending measured workload cost from campaign
37703972798, with query-name and shard-id tie breaks. Costs include estimated warmup work;
new queries use the suite's median cost. Fuzz uses sorted round-robin independently within
each original round/seed. Estimated run-step times from that campaign are approximately
40, 42, 28, 44, 39 and 42 minutes per shard respectively, leaving headroom toward the
75-minute target. Runner hardware and candidate behavior can change these estimates.

Every shard runs all eight balanced rounds, the same four variant ORDERS, the same four
warmup passes and original timed repetitions for its assigned queries. Spark validates its
own subset; fuzz retains seeds 1 through 8 and the per-seed oracle. TPC-H setup/select/cleanup
sequences stay together, including q15's temporary view. Sharding changes which queries
share a JVM/cache, so historical campaign timings are scheduling estimates rather than
direct measurements of the new campaign.

Result, warmup-validation, manifest, log, data-digest and environment filenames include the
shard id; physical plans live under `plans/<suite>/shard-<id>`. The report downloads shards
into separate directories, preserving every original artifact and raw timing record. It
enumerates the full query catalog independently, checks each shard's exact assignment,
then explicitly requires the shard union to equal the full expected query set for every
variant and round. Missing/duplicate queries, missing shards, changed SQL, incomplete
warmups or altered protocol fail the report and suppress invalid comparisons. Plans from
all shards are retained; differing native coverage for identical SQL is reported as unknown.

## Build and data caches

The exact build-cache key is
`comet-release-v1-<resolved-comet-sha>-<Cargo.lock-iceberg-sha>-rust-1.99.0-<build-steps-hash>`.
The final hash covers the benchmark workflow and builder/Maven-bootstrap action files,
including build commands, flags and environment setup. There are no prefix restore keys.
The cache stores `dist/comet.jar`, `native/target/release/libcomet.so` and original build
provenance. A hit skips Cargo/Maven compilation, verifies source/dependency/compiler/flag
provenance and both binary digests, then uploads the same `pruning-<side>` artifact with
fresh `build-info.json`. Metadata records the hit, key, original producer run/hardware and
current build-runner hardware; each campaign retains its own resolved-revision manifest.

One data producer per suite uses
`iceberg-data-v1-<generator-scripts-hash>-<suite>-<rows>-<generation-params-hash>`.
The script hash covers `generate_data.py`, `tpch_generate.py`, `tpch_iceberg.py` and `lib.py`.
Generation parameters include file count,
fuzz rows, equality-delete cardinality, TPC-H scale, Spark/Iceberg/DuckDB/Java versions,
Spark generation settings and the fixed warehouse path. Keys omit the implementation,
query selection, shard id/count, warmups and measurement repetitions.

Data generation is plain Spark, without either Comet jar. The cache contains the complete
warehouse and its SHA-256 manifest, including hidden checksum files. A cache hit rehashes
every file. The producer always publishes `shared-data-<suite>` as an exact snapshot
artifact, with seven-day retention; shards use it if GitHub evicts the suite cache. This
fallback matters because the default cache quota can be smaller than the campaign's
combined warehouses. Shards restore at the same absolute path used in Iceberg metadata,
verify every file before and after all variants, and the report checks that every shard of
a suite used identical data/metadata bytes. Baseline and candidate read the same immutable
snapshot. Spark oracle files are regenerated per shard and are never cached.

Every job records CPU model and `/proc/cpuinfo`, available core count from `nproc`, and
memory from `free -m`. The report includes CPU/core/memory and cache hits per query shard.
Resolver, build, data-producer and report hardware metadata are retained in their artifacts.

## Targeted campaigns

Omitting selection fields in `revisions.json` runs the full campaign. To select suites,
add `"suites": ["join", "topk_minmax"]`. To select exact query names, optionally add:

```json
"queries": {
  "topk_minmax": ["min__f_sorted", "max__f_sorted"]
}
```

Query keys must name selected suites; each value is a nonempty list of unique canonical
query names, as listed in the resolved catalog or prior report. Unknown suites/names fail
resolution. If only `queries` is present, all suites remain selected and unspecified suites
run in full. Fuzz names include the original seed; selecting a seeded name runs it only in
that original round, with the other rounds retaining their empty-subset JVM launches.
Small selections reduce the shard count but never increase it beyond the default.

The report prominently labels a partial campaign and prints its suite/query selections.
Coverage must equal that explicit selection, while all variant, oracle, warmup, repetition,
round and provenance requirements remain in force. The data key remains identical to a
full campaign of the same suite and generation parameters.

## Harness checks

The resolver runs these checks in GitHub Actions before builds or data generation:

```bash
python3 benchmarks/iceberg-runtime-pruning-detailed/check_harness.py
```

This checks exact result handling, the oracle's schema/value gate, balanced execution order,
missing/duplicate/error coverage, independent-round statistics and unique workload names.
It also checks deterministic full-catalog sharding, all fuzz seeds, targeted selections,
shard-independent cache keys, TPC-H view sequences and report rejection of omitted queries
or missing shards. Workload enumeration does not start Spark.
