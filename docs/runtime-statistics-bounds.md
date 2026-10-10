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

# Driver runtime-statistics resource bounds

File statistics are advisory. If collection exceeds a limit, cannot run concurrently,
or fails, native Iceberg keeps row-group, page and row runtime pruning. Spark's tasks,
splits, residuals and delete associations remain authoritative.

The driver owns one stable Guava cache with a **64 MiB estimated weight ceiling**.
Its keys contain immutable metadata location, pinned snapshot ID, sorted field IDs,
and the requesting configuration's entry and collection limits. They retain no
session or SQLConf objects. Equal policies may share immutable statistics; changing
a session's policy never replaces another policy's cache. Policies compete for the
same process budget, so their useful entries can still be evicted.

`spark.comet.scan.icebergNative.runtimeStatsCache.maxEntries` remains an upper bound
on entries for each policy. Each entry is charged at least the process budget divided
by that limit, rounded up. Actual estimated weight takes precedence. This conservative
floor enforces entry count without a second cache or eviction registry; small entries
may use less actual heap than their charged weight.

Collection has two additional session limits, applied with caching enabled or disabled:

- `spark.comet.scan.icebergNative.runtimeStats.maxFiles`: 4096 distinct selected files
  by default. Multiple splits of one file count once. An oversized selection stops
  before manifest replanning, so repeated calls do not repeat that planning work.
- `spark.comet.scan.icebergNative.runtimeStats.maxBytes`: 8 MiB estimated bytes by
  default, configurable up to the shared 64 MiB ceiling. Collection stops at the
  first file that would exceed the budget, closes the plan, and returns no partial
  statistics. A bounded rejection entry avoids replanning that snapshot/field-ID/policy
  combination until eviction. This deliberately also declines smaller selections of
  that combination; changing collection limits uses an independent policy.

These are conservative resource defaults, not performance thresholds derived from
benchmarks: 64 MiB allows eight entries at the default 8 MiB limit; 4096 files at
8 MiB allows roughly 2 KiB estimated metadata per file. The estimator charges file
and map headers, map nodes and boxed IDs/counts, paths, bound-buffer capacities,
key metadata, split offsets and variable-sized partition values. JVM layouts,
shared schemas and the Iceberg planner's internal manifest buffers are not measured
by it, so this is **not a hard JVM heap or RSS limit**. A current file can already
have been allocated by Iceberg before Comet rejects it.

Only one manifest-statistics collection is admitted per driver at a time. Another
miss declines advisory file statistics without waiting. Cache hits do not need the
collection permit. Selected-path sets and returned query metadata are bounded per
call; live query metadata and independent Iceberg planning allocations are owned by
the queries, not included in cache retention. Disabling caching bypasses positive
and rejection entries, but keeps the collection limits and admission rule.

Cache hits check coverage and build the result using only selected-path lookups.
Incomplete cached coverage triggers replanning; an incomplete new plan is never
cached. Snapshot and field-ID isolation, the zero/one-task gate, and exact selected
path sets are preserved.

Driver collection crosses only a conservative set of known infallible Catalyst
filters and projections. Determinism alone does not admit casts, division, arbitrary
functions or arithmetic. Constant integer modulo by a matching nonzero divisor
other than -1 is supported in both ANSI modes. Native attachment remains the final
policy and can support shapes beyond this driver collection subset. Top-K secondary
expressions must pass the collection gate; string NULLS FIRST does not collect file
statistics while the native producer declines it.

File metrics pooling includes the selected field-ID set as well as the path.
Identical splits share metrics, while tasks with different schema-derived subsets
receive distinct pool indices. Decimal schema strings must have precision 1 through
38 and scale 0 through precision; valid Iceberg formatting with a space after the
comma remains accepted.

`IcebergReflectionSuite` covers selected-file gates, byte rejection, cleanup,
retained-weight accounting, concurrent misses, alternating policies, eviction and
direct subset lookup. Its many-file test prints observed planning time and charged
cache retention in GitHub Actions. These observations are diagnostics, not heap
profiling or an end-to-end speedup benchmark. The fork adaptive validation workflow
also runs the serde, runtime-key type and scan-rule regressions on Spark 3.5 and 4.1.
