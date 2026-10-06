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

"""Summarize the detailed runtime pruning benchmark as Markdown.

Usage: summarize.py results-*.jsonl

Variants: spark (no Comet), main (the main commit this branch is based on), off (branch,
runtime filters off), on (branch, runtime filters on). Exits non-zero when variants disagree
on a query result: any two Comet variants for every suite, and plain Spark too for every suite
except tpch (floating point rounding) and strjoin (a known Comet string-join difference).
"""

import json
import math
import random
import statistics
import sys
from collections import defaultdict

VARIANTS = ["spark", "main", "prev", "off", "on"]
LABELS = {
    "spark": "Spark",
    "main": "main",
    "prev": "previous branch",
    "off": "branch off",
    "on": "branch on",
}
COMET = ["main", "prev", "off", "on"]


def quantile(values, q):
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def ratio_ci(baseline, candidate, rng, samples=1000):
    ratios = []
    for _ in range(samples):
        b = statistics.median(rng.choices(baseline, k=len(baseline)))
        c = statistics.median(rng.choices(candidate, k=len(candidate)))
        ratios.append(b / c if c else float("inf"))
    return quantile(ratios, 0.025), quantile(ratios, 0.975)


def mib(value):
    return f"{value / (1024 * 1024):,.1f}"


def load(paths):
    records = []
    for path in paths:
        for line in open(path, encoding="utf-8"):
            if line.strip():
                record = json.loads(line)
                # Table names repeat across suites with different row counts: key them apart.
                if record["suite"] in ("join", "topk_minmax", "layouts"):
                    record["query"] = f"{record['suite']}/{record['query']}"
                records.append(record)
    return records


def main():
    records = load(sys.argv[1:])
    by_key = defaultdict(list)
    suite_of = {}
    for r in records:
        by_key[(r["query"], r["variant"])].append(r)
        suite_of[r["query"]] = r["suite"]
    queries = list(dict.fromkeys(r["query"] for r in records))
    rng = random.Random(1)
    failures, warnings, errors = [], [], []

    for q in queries:
        for v in VARIANTS:
            for r in by_key[(q, v)]:
                if "error" in r:
                    errors.append((q, v, r["error"]))
    errored = {(q, v) for q, v, _ in errors}

    for q in queries:
        ok = lambda vs: {r["checksum"] for v in vs for r in by_key[(q, v)] if "checksum" in r}  # noqa: E731
        if len(ok(COMET)) > 1:
            failures.append((q, "Comet variants disagree"))
        elif len(ok(VARIANTS)) > 1:
            (warnings if suite_of[q] in ("tpch", "strjoin") else failures).append((q, "Spark and Comet disagree"))
        # An error must be shared by every variant that ran the query.
        failed = {v for v in VARIANTS if (q, v) in errored}
        ran = {v for v in VARIANTS if by_key[(q, v)]}
        if failed and failed != ran:
            (warnings if suite_of[q] == "strjoin" else failures).append((q, f"only {sorted(failed)} failed"))

    suites = list(dict.fromkeys(suite_of.values()))
    print("## Verdict\n")
    print(f"- queries: {len(queries)} across suites {', '.join(suites)}; records: {len(records)}")
    print(f"- result mismatches: {len(failures)}; shared query errors: {len({q for q, _, _ in errors})}")
    print()

    # --- headline per suite --------------------------------------------------------------
    print("## Branch with runtime filters on, versus main and versus filters off\n")
    print("Geometric mean of median-time ratios (above 1.0 is faster); wins and losses count queries")
    print("whose bootstrap 95% interval excludes 1.0.\n")
    print("| suite | queries | geomean vs main | wins | losses | geomean vs previous | wins | losses | MiB read main -> prev -> on |")
    print("|---|---|---|---|---|---|---|---|---|")
    detail = {}
    for suite in suites:
        cells = {}
        for base in ("main", "prev"):
            ratios, wins, losses = [], 0, 0
            for q in [x for x in queries if suite_of[x] == suite]:
                on = [r["exec_ms"] for r in by_key[(q, "on")] if "exec_ms" in r]
                bs = [r["exec_ms"] for r in by_key[(q, base)] if "exec_ms" in r]
                if not on or not bs:
                    continue
                med = statistics.median(bs) / statistics.median(on)
                low, high = ratio_ci(bs, on, rng) if len(on) > 1 and len(bs) > 1 else (med, med)
                detail[(q, base)] = (med, low, high)
                ratios.append(med)
                wins += low > 1.0
                losses += high < 1.0
            geomean = math.exp(statistics.mean(math.log(x) for x in ratios)) if ratios else float("nan")
            cells[base] = (geomean, wins, losses, len(ratios))
        qs = [x for x in queries if suite_of[x] == suite]
        read = lambda v: sum(statistics.median(r["bytes_scanned"] for r in by_key[(x, v)]) for x in qs if by_key[(x, v)] and "bytes_scanned" in by_key[(x, v)][0])  # noqa: E731
        print(
            f"| {suite} | {cells['main'][3]} | {cells['main'][0]:.2f}x | {cells['main'][1]} | {cells['main'][2]} | "
            f"{cells['prev'][0]:.2f}x | {cells['prev'][1]} | {cells['prev'][2]} | {mib(read('main'))} -> {mib(read('prev'))} -> {mib(read('on'))} |"
        )

    # --- per query -----------------------------------------------------------------------
    for suite in suites:
        if suite == "fuzz":
            continue
        print(f"\n## {suite}: per query\n")
        print("Median execution time in ms (warm). Ratios are baseline / branch-on with a 95% interval.\n")
        print("| query | Spark | main | prev | off | on | on vs main | 95% CI | on vs prev | MiB prev | MiB on | files pruned | row groups pruned (live) |")
        print("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
        for q in [x for x in queries if suite_of[x] == suite]:
            def med(v, key="exec_ms"):
                vals = [r[key] for r in by_key[(q, v)] if key in r]
                return statistics.median(vals) if vals else None

            def fmt(x):
                return "-" if x is None else f"{x:,.0f}"

            m = detail.get((q, "main"))
            o = detail.get((q, "prev"))
            on_runs = [r for r in by_key[(q, "on")] if "exec_ms" in r]

            def pm(name):
                return statistics.median(r.get(name, 0) for r in on_runs) if on_runs else 0

            print(
                f"| {q} | {fmt(med('spark'))} | {fmt(med('main'))} | {fmt(med('prev'))} | {fmt(med('off'))} | {fmt(med('on'))} | "
                f"{'-' if not m else f'{m[0]:.2f}x'} | {'-' if not m else f'{m[1]:.2f}-{m[2]:.2f}'} | "
                f"{'-' if not o else f'{o[0]:.2f}x'} | "
                f"{mib(med('prev', 'bytes_scanned') or 0)} | {mib(med('on', 'bytes_scanned') or 0)} | "
                f"{pm('iceberg_runtime_file_tasks_pruned'):,.0f} | "
                f"{pm('iceberg_runtime_row_groups_pruned'):,.0f} ({pm('iceberg_runtime_row_groups_pruned_live'):,.0f}) |"
            )

    # --- string-join diagnostic: the expected count is the width in the query name -----------
    diag = [q for q in queries if suite_of[q] == "strjoin"]
    if diag:
        print("\n## strjoin: join results against the expected row count\n")
        print("Every build key exists exactly once in the fact table, so `n` must equal the width.\n")
        print("| query | expected | Spark | main | off | on |")
        print("|---|---|---|---|---|---|")
        for q in diag:
            expected = int(q.rsplit("_", 1)[1])
            cells = []
            for v in VARIANTS:
                runs = by_key[(q, v)]
                cells.append(runs[0].get("sample", "?") if runs and "error" not in runs[0] else "error")
            if any(not c.startswith(f"[({expected},") for c in cells):
                print(f"| {q} | {expected} | " + " | ".join(cells) + " |")

    # --- fuzz ----------------------------------------------------------------------------
    fuzz = [q for q in queries if suite_of[q] == "fuzz"]
    if fuzz:
        pruned = sum(
            1 for q in fuzz for r in by_key[(q, "on")]
            if r.get("iceberg_runtime_file_tasks_pruned", 0) or r.get("iceberg_runtime_row_groups_pruned", 0)
        )
        print("\n## fuzz\n")
        print(f"{len(fuzz)} generated queries, each run once per variant, results compared across variants.")
        print(f"Queries where the branch pruned at least one file or row group at runtime: {pruned}.\n")

    # --- regressions and problems --------------------------------------------------------
    regressions = [
        (q, d) for (q, base), d in detail.items() if base == "main" and d[2] < 0.9 and suite_of[q] != "fuzz"
    ]
    print("\n## Possible regressions (on vs main, interval entirely below 0.9x)\n")
    if regressions:
        for q, d in sorted(regressions, key=lambda x: x[1][0]):
            print(f"- {q}: {d[0]:.2f}x ({d[1]:.2f}-{d[2]:.2f})")
    else:
        print("None.")
    if warnings:
        print("\n## Spark and Comet differ (tpch floating point tolerance)\n")
        for q, why in warnings:
            print(f"- {q}")
    if errors:
        print("\n## Query errors\n")
        for q, v, e in sorted(set(errors))[:60]:
            print(f"- {q} / {v}: `{e}`")
    if failures:
        print("\n## Result mismatches\n")
        for q, why in failures:
            print(f"- **{q}**: {why}")
        sys.exit(1)


if __name__ == "__main__":
    main()
