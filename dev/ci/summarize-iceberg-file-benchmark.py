#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Report original, adaptive reader, and pre-footer file-pruning measurements."""

import json
import statistics
import sys
from pathlib import Path

rows = [json.loads(line) for line in Path(sys.argv[1]).read_text().splitlines() if line.strip()]
queries = ("join", "join_no_pruning", "min", "topk", "join_many_files")
variants = ("before", "snapshot", "after")
assert len(rows) == 75, "Expected five alternating triplets for five queries"
assert {row["query"] for row in rows} == set(queries)
print("# Adaptive native Iceberg: original, row-group/page pruning, and file pruning")
print("\n| Query | Version | Reader bytes | Median query ms | File tasks pruned |")
print("|---|---|---:|---:|---:|")
for query in queries:
    group = [row for row in rows if row["query"] == query]
    assert len({row["file_sha256"] for row in group}) == 1
    assert len({tuple(row["values"]) for row in group}) == 1
    for iteration in range(5):
        triplet = [row for row in group if row["iteration"] == iteration]
        assert len(triplet) == 3 and {row["variant"] for row in triplet} == set(variants)
    for variant in variants:
        measurements = [row for row in group if row["variant"] == variant]
        for row in measurements:
            if variant == "before":
                assert row["runtime_predicate_tasks"] == row["runtime_row_groups_pruned"] == row["file_tasks_pruned"] == 0
            else:
                assert row["runtime_predicate_tasks"] > 0
                if query == "join_many_files":
                    if variant == "after":
                        assert row["file_tasks_pruned"] >= row["file_count"] - 2
                    else:
                        assert row["file_tasks_pruned"] == 0 and row["initial_pruned"] > 0
                elif query == "join_no_pruning":
                    assert row["runtime_row_groups_pruned"] == 0
                else:
                    assert row["runtime_row_groups_pruned"] > 0
        b = statistics.median(row["bytes_scanned"] for row in measurements)
        t = statistics.median(row["wall_ms"] for row in measurements)
        p = statistics.median(row["file_tasks_pruned"] for row in measurements)
        print(f"| {query} | {variant} | {b:,.0f} | {t:.2f} | {p:,.0f} |")
print("\n| Query | Comparison | Byte reduction | Query time reduction |")
print("|---|---|---:|---:|")
for query in queries:
    group = [row for row in rows if row["query"] == query]
    medians = {variant: (
        statistics.median(row["bytes_scanned"] for row in group if row["variant"] == variant),
        statistics.median(row["wall_ms"] for row in group if row["variant"] == variant),
    ) for variant in variants}
    for reference in ("before", "snapshot"):
        b0, t0 = medians[reference]
        b1, t1 = medians["after"]
        assert b0 > 0 and b1 > 0
        print(f"| {query} | {reference} → after | {(1-b1/b0)*100:.2f}% | {(1-t1/t0)*100:.2f}% |")
    if query == "join_many_files":
        assert medians["after"][0] < medians["snapshot"][0] < medians["before"][0]
for variant in variants:
    revisions = {row["revision"] for row in rows if row["variant"] == variant}
    assert len(revisions) == 1
    print(f"\n{variant} native source revision: `{next(iter(revisions))}`")
for query in ("join", "join_many_files"):
    row = next(row for row in rows if row["query"] == query)
    print(f"\n{query} fixture: {row['rows']:,} rows, {row['file_count']} files, "
          f"{row['file_bytes']:,} physical bytes, {row['row_groups']} row groups, SHA-256 `{row['file_sha256']}`.")
print("\nFive alternating triplets in separate JVMs, identical persisted data and Spark 4.1 harness, identical producer flags, two native warmups per query. Every answer checked against Spark. Reader bytes count requested storage ranges, including metadata/deletes. Elapsed time includes Spark scheduling and excludes JVM/fixture creation. Warm local OS caches; these are not cold or object-storage measurements. Negative reductions are reported as regressions. The snapshot variant is the prior adaptive row-group/page reader before manifest file pruning, rather than task-start-only join pruning. The wide TopK includes payload columns; the nonselective join measures overhead.")
