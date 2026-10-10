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

"""Summarize runtime pruning benchmark JSON lines as Markdown.

Exits non-zero when variants disagree on any query result.
"""

import json
import random
import statistics
import sys
from collections import defaultdict

VARIANTS = ["main", "off", "on"]
LABELS = {
    "main": "main",
    "off": "branch, runtime filters off",
    "on": "branch, runtime filters on",
}


def quantile(values, q):
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def ratio_ci(baseline, candidate, rng, samples=2000):
    """Bootstrap 95% interval for median(baseline) / median(candidate)."""
    ratios = []
    for _ in range(samples):
        b = statistics.median(rng.choices(baseline, k=len(baseline)))
        c = statistics.median(rng.choices(candidate, k=len(candidate)))
        ratios.append(b / c)
    return quantile(ratios, 0.025), quantile(ratios, 0.975)


def mib(value):
    return f"{value / (1024 * 1024):,.1f}"


def main():
    records = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
    by_key = defaultdict(list)
    for record in records:
        by_key[(record["query"], record["variant"])].append(record)
    queries = list(dict.fromkeys(record["query"] for record in records))
    rng = random.Random(1)
    failures = []

    print("## Correctness\n")
    print("| query | identical results across variants | rows |")
    print("|---|---|---|")
    for query in queries:
        checksums = {r["checksum"] for v in VARIANTS for r in by_key[(query, v)]}
        rows = {r["rows"] for v in VARIANTS for r in by_key[(query, v)]}
        ok = len(checksums) == 1
        if not ok:
            failures.append(query)
        print(f"| {query} | {'yes' if ok else 'NO: ' + ', '.join(sorted(checksums))} | {sorted(rows)} |")

    print("\n## Execution time (ms, warm)\n")
    print("Median of all timed runs; IQR is p25-p75; CV is stdev/mean.\n")
    print("| query | variant | n | median | IQR | min | CV | median planning |")
    print("|---|---|---|---|---|---|---|---|")
    for query in queries:
        for variant in VARIANTS:
            runs = by_key[(query, variant)]
            if not runs:
                continue
            ms = [r["exec_ms"] for r in runs]
            plan = statistics.median(r["plan_ms"] for r in runs)
            cv = statistics.pstdev(ms) / statistics.mean(ms)
            flag = " (noisy)" if cv > 0.15 else ""
            print(
                f"| {query} | {LABELS[variant]} | {len(ms)} | {statistics.median(ms):,.0f} | "
                f"{quantile(ms, 0.25):,.0f}-{quantile(ms, 0.75):,.0f} | {min(ms):,.0f} | "
                f"{cv:.1%}{flag} | {plan:,.0f} |"
            )

    print("\n## Speedup of branch with runtime filters on\n")
    print("Ratio of median execution times with a bootstrap 95% interval; >1 is faster.\n")
    print("| query | vs main | 95% CI | vs branch off | 95% CI |")
    print("|---|---|---|---|---|")
    for query in queries:
        on = [r["exec_ms"] for r in by_key[(query, "on")]]
        cells = []
        for baseline in ["main", "off"]:
            base = [r["exec_ms"] for r in by_key[(query, baseline)]]
            if not on or not base:
                cells += ["-", "-"]
                continue
            low, high = ratio_ci(base, on, rng)
            cells += [f"{statistics.median(base) / statistics.median(on):.2f}x", f"{low:.2f}-{high:.2f}"]
        print(f"| {query} | " + " | ".join(cells) + " |")

    print("\n## Reader I/O and pruning (median per query run)\n")
    print("`bytes_scanned` is the byte total of ranged reads issued by the native Iceberg reader,")
    print("including footers and page indexes. It is deterministic for a given plan.\n")
    print(
        "| query | variant | MiB read | vs main | file tasks | tasks with runtime predicate | "
        "files pruned before open | row groups pruned (live) | refreshes |"
    )
    print("|---|---|---|---|---|---|---|---|---|")
    for query in queries:
        main_bytes = statistics.median(r["bytes_scanned"] for r in by_key[(query, "main")] or [{"bytes_scanned": 0}])
        for variant in VARIANTS:
            runs = by_key[(query, variant)]
            if not runs:
                continue

            def med(name):
                return statistics.median(r.get(name, 0) for r in runs)

            read = med("bytes_scanned")
            share = f"{read / main_bytes:.1%}" if main_bytes else "-"
            print(
                f"| {query} | {LABELS[variant]} | {mib(read)} | {share} | {med('num_splits'):,.0f} | "
                f"{med('iceberg_runtime_predicate_tasks'):,.0f} | "
                f"{med('iceberg_runtime_file_tasks_pruned'):,.0f} | "
                f"{med('iceberg_runtime_row_groups_pruned'):,.0f} "
                f"({med('iceberg_runtime_row_groups_pruned_live'):,.0f}) | "
                f"{med('iceberg_runtime_predicate_refreshes'):,.0f} |"
            )

    if failures:
        print(f"\n**Result mismatch:** {', '.join(failures)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
