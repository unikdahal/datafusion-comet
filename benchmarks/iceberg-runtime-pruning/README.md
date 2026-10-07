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

`.github/workflows/iceberg_adaptive_benchmark.yml` compares three variants on one runner:

| variant | Comet build                             | runtime filter settings                    |
| ------- | --------------------------------------- | ------------------------------------------ |
| main    | the main commit this branch is based on | enabled (they apply only to Parquet there) |
| off     | this branch                             | disabled                                   |
| on      | this branch                             | enabled                                    |

Main versus off isolates dependency changes (the iceberg-rust revision);
off versus on isolates the runtime pruning itself. Both sides are built with the same commands
(`cargo build --release` and `mvnw install -Prelease -Pspark-3.5`) and no CPU-specific flags.

## Data

`generate_data.py` runs once with plain Spark 3.5.9 and Iceberg 1.8.1, so every variant reads
byte-identical files. Each fact table has 16 million rows `(id INT, value BIGINT, payload
STRING)` in 16 data files with an 8 MiB row-group target:

- `fact_sorted`: range-partitioned and sorted by `id`, so files and row groups cover key ranges.
- `fact_pos_deletes`: the same layout after a merge-on-read `DELETE` of every 101st key, which
  writes position delete files.
- `fact_unsorted`: the same rows in random order. Statistics cannot prune it, so it measures
  overhead.
- `dim`: 128 even keys in a narrow range at 60% of the key space.

## Queries

- `join_*`: broadcast inner join of a fact table with `dim` (completed build-side bounds).
- `topk_*`: `ORDER BY id LIMIT 10` (a threshold that tightens while scanning).
- `min_*`: ungrouped `min(id)` (a bound that tightens while scanning).
- `scan_control`: a filtered aggregate with no runtime producer.

## Method

Each round runs the three variants as separate `spark-submit` JVMs in rotated order. Inside a
JVM every query runs once untimed, then three times in a rotated query order, giving 18 timed
runs per query and variant. Planning (`executedPlan`) and execution (`collect`) are timed
separately; planning includes the driver-side statistics re-plan. Every result is
checksummed and the summary fails if variants disagree.

Timings are warm-cache local-disk timings on a shared CI runner. The summary reports medians,
interquartile ranges, coefficients of variation and bootstrap 95% intervals for speedups.
`bytes_scanned` is the total of ranged reads issued by the native reader, including footers and
page indexes. It does not depend on cache state and is the best proxy for object-store cost.

The baseline resolves the latest Apache DataFusion Comet main once at workflow start.
Its full commit and resolution time are recorded in the `resolved-revisions` artifact.
Build matrix jobs consume that immutable commit; the baseline retains its own locked
dependencies. New runs resolve main again rather than selecting a fork merge base.
