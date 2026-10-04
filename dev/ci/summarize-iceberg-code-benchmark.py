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

"""Summarize paired native Iceberg scan measurements emitted by the Spark test."""

import json
import statistics
import sys
from pathlib import Path

rows = [json.loads(line) for line in Path(sys.argv[1]).read_text().splitlines() if line.strip()]
assert len(rows) == 10, "Expected five before/after pairs"
assert len({row["file_sha256"] for row in rows}) == 1, "Both versions must read the same file"
assert {row["value"] for row in rows} == {2048}, "Results must match Spark"
for iteration in range(5):
    pair = [row for row in rows if row["iteration"] == iteration]
    assert len(pair) == 2 and {row["variant"] for row in pair} == {"before", "after"}
variants = [[row for row in rows if row["variant"] == variant] for variant in ("before", "after")]
for row in variants[0]:
    assert row["runtime_predicate_tasks"] == row["runtime_row_groups_pruned"] == 0
for row in variants[1]:
    assert row["runtime_predicate_tasks"] > 0 and row["runtime_row_groups_pruned"] > 0
b0, b1 = (statistics.median(row["bytes_scanned"] for row in group) for group in variants)
t0, t1 = (statistics.median(row["wall_ms"] for row in group) for group in variants)
assert b0 > 0 and b1 < b0
print("# Native Iceberg: true code before/after")
print("\n| Measurement | Before code | After code | Reduction |")
print("|---|---:|---:|---:|")
print(f"| Reader bytes | {b0:,.0f} | {b1:,.0f} | {(1-b1/b0)*100:.2f}% |")
print(f"| Query elapsed (ms) | {t0:.2f} | {t1:.2f} | {(1-t1/t0)*100:.2f}% |")
print("\nDynamic join filtering is enabled in both versions. Five alternating pairs, separate JVMs, two native warmups per measurement, same persisted sorted file and Spark 4.1 harness. Medians shown. Reader bytes measure requested I/O; query elapsed includes Spark scheduling and excludes fixture/JVM startup. Warm local OS cache; no object-storage or cold-cache claim.")
for name, group in zip(("Before", "After"), variants):
    revisions = {row["revision"] for row in group}
    assert len(revisions) == 1
    print(f"\n{name} revision: `{next(iter(revisions))}`")
print(f"\nData-file SHA-256: `{rows[0]['file_sha256']}`")
