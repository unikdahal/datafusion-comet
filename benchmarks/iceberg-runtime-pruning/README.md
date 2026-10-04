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

The fork-only `iceberg_adaptive_pruning.yml` workflow builds the native release library and
runs the full `CometIcebergNativeSuite` on Spark 3.5 and 4.1, covering joins, MIN/MAX,
TopK, page selections, and all delete modes. Test logs are uploaded for each Spark version.
The historical three-version benchmark is available by manual dispatch with `benchmark=true`;
its original and intermediate native libraries remain pinned to their measured revisions.
Those historical comparisons include dependency differences after rebasing onto current main
and must not be presented as a matched-dependency optimization-only comparison. A new matched
current-main baseline is required before publishing refreshed performance claims.

The fixture is one sorted Iceberg v2 Parquet file containing 120,000 integer keys and
64-character SHA-256 payloads, written without compression with a 128 KiB row-group target.
The four primary scenarios set an explicit 128 MiB split-size read option and assert that
the file is executed as exactly one data-file task. Adaptive split sizing is disabled and
open-file cost is set to 1 byte: Iceberg first splits at Parquet offsets, then packs those
pieces using a per-piece open-file weight. The default 4 MiB weight fragments this small
file into more tasks as delete files are added.
The test inspects the actual footer and records its row-group count. The build table contains
32 even keys from 50,000 through 50,062, so min/max pruning must retain gaps for the exact
membership consumer to remove.

Both variants use native Iceberg scans and native execution. OFF sets
`spark.comet.exec.join.dynamicFilter.enabled=false`; ON enables it. Both answers must match
plain Spark. Every ON measurement must show an accepted Iceberg runtime predicate, additional
row groups pruned, and less than one quarter of OFF's reader bytes. The test repeats this for
no deletes, positional deletes, equality deletes on the payload column, and both delete types.
A fifth scenario restores the default 4 MiB open-file cost, enables adaptive split sizing,
and requires at least 20% fewer reader bytes. Each resulting task independently prefetches 512 KiB of metadata; this case
records how repeated metadata reads limit savings for a small file.

Each scenario warms both paths, then executes five pairs in alternating order. The summary
reports median reader bytes and median elapsed milliseconds. Timings cover `collect()`,
including Spark scheduling, with warm OS caches on local disk. `bytes_scanned` counts ranged
reader I/O requests, including metadata; it is neither a decoded-row count nor an S3 wire-byte
measurement. These results establish reader pruning on this fixture; remote object-store
latency and wider or unsorted build domains need separate workloads.

For local reproduction after a release native build:

```sh
export SPARK_HOME="$PWD"
export COMET_ICEBERG_BENCHMARK_REPEATS=5
export COMET_ICEBERG_BENCHMARK_OUTPUT="$PWD/runtime-benchmark.jsonl"
./mvnw -B -Prelease -Pspark-4.1 test -Dtest=none \
  '-Dsuites=org.apache.comet.CometIcebergNativeSuite join runtime filter prunes native Iceberg row groups and bytes'
python3 dev/ci/summarize-iceberg-runtime-benchmark.py runtime-benchmark.jsonl
```
