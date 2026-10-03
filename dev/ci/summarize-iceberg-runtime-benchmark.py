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
assert rows, "No benchmark measurements: inspect the Spark test log"
print("| Delete mode | OFF bytes | ON bytes | I/O reduction | OFF median ms | ON median ms | Time ratio |")
print("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
for mode in dict.fromkeys(row["mode"] for row in rows):
    groups = {flag: [row for row in rows if row["mode"] == mode and row["enabled"] == flag] for flag in (False, True)}
    off, on = groups[False], groups[True]
    assert len(off) == len(on) and len(off) >= 5, "Five paired measurements required"
    assert {r["iteration"] for r in off} == {r["iteration"] for r in on}
    assert len({r["value"] for r in off + on}) == 1, "Results differ"
    assert all(r["runtime_predicate_tasks"] > 0 and r["runtime_row_groups_pruned"] > 0 for r in on)
    b0, b1 = (statistics.median(r["bytes_scanned"] for r in group) for group in (off, on))
    t0, t1 = (statistics.median(r["wall_ms"] for r in group) for group in (off, on))
    print(f"| {mode} | {b0:,.0f} | {b1:,.0f} | {100 * (1 - b1 / b0):.2f}% | {t0:.2f} | {t1:.2f} | {t0 / t1:.2f}x |")
print("\nFive alternating OFF/ON pairs after warmup, same sorted local Parquet file, native release library. Both paths checked against Spark. bytes_scanned measures reader I/O requests; timings include Spark scheduling and use warm OS caches.")
