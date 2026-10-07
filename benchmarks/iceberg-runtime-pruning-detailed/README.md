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
The suites use separate runners, so compare implementations within a suite rather than
comparing absolute times across suites.

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
- `fuzz`: 500 deterministic queries per seed across 17 layouts and key types; four seeds
  produce 2,000 distinct correctness checks for each implementation/configuration.
- `tpch`: all 22 queries at scale factor 3, on natural and date-clustered Iceberg layouts.
- `strjoin`: integer, long and string joins against up to three million build keys, with
  broadcast, shuffled hash and sort merge and with/without DISTINCT.

Unsorted data, full-range builds, expressions without usable bounds, combined MIN/MAX,
grouped aggregates and ordinary scans provide overhead/control cases. Live pruning counters
and decoder rebuilds expose whether a tightening bound arrived in time to save reader work.

## Measurement and correctness

Eight balanced rounds (two complete four-round blocks) run every native variant in a fresh JVM. Each variant occupies each
execution position once; every ordered adjacent pair appears once. Each JVM warms every
query twice before two timed repetitions (one for TPC-H/string joins). Fuzz queries run once per
seed for correctness and are excluded from speedup headlines. Query order is deterministically shuffled between
passes, using the same permutation for every variant. SQL construction/analysis, physical planning and collection are timed separately;
headline total time includes all three.

Every run checks its full result and output schema against plain Spark. Integer, decimal,
string, date and nested values remain exact; NaNs and signed zeros use SQL value semantics.
Only TPC-H floating-point values permit relative error `1e-9` or absolute error `1e-8`.
The artifact reports every accepted non-identical digest. No suite is exempt from correctness;
query errors, missing variants, missing records, duplicate records and mismatched SQL fail.

The report shows per-query medians and execution IQRs, plus main/candidate and off/on
ratios with paired bootstrap 95% intervals. It resamples independent JVM-round medians,
preserving repetitions as a cluster. Eight CI rounds provide a first comparison; the raw
records support further analysis or repeated runs. Measurements use warm local-disk caches
on shared CI runners. Exploratory bootstrap intervals do not adjust for multiple
comparisons and do not establish a root cause. Driver GC count/time and JIT compilation
time are sampled outside the timed region and retained as diagnostics; these driver-wide
counters include overlapping background work and must not be subtracted from query time.

Reader bytes, file tasks, rows, file pruning, row-group pruning, live pruning, predicate
refreshes and decoder rebuilds are reported per variant. Missing counters are `n/a`, never
zero. All exposed SQL metrics are captured by physical stage and physical plans are saved.
Reader byte totals quantify requested ranged I/O; scheduling-dependent pruning is shown
with a range. Queries with a physical fallback are listed so timing changes can be assessed
against the plan that actually ran.

The `matched-report` artifact contains the full report; `matched-<suite>` artifacts contain
raw JSONL records, manifests, physical plans, logs, data hashes and runner/build provenance.
The workflow continues collecting diagnostics after a variant fails, then fails its verdict.

## Harness checks

After installing PySpark, run:

```bash
python3 benchmarks/iceberg-runtime-pruning-detailed/check_harness.py
```

This checks exact result handling, the oracle's schema/value gate, balanced execution order,
missing/duplicate/error coverage, independent-round statistics and unique workload names.
