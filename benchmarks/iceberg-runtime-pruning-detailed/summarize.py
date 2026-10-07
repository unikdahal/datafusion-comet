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

"""Validate complete matched results and report paired JVM-round timing intervals."""

import argparse
from collections import defaultdict
import json
import math
from pathlib import Path
import random
import statistics

from lib import VARIANTS


def quantile(values, q):
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def median(records, name):
    values = [r[name] for r in records if r.get(name) is not None]
    return statistics.median(values) if values else None


def paired_ratio(baseline, candidate, metric="total_ms", samples=4000):
    """Bootstrap independent JVM rounds, retaining paired variants and repetitions."""
    if any(
        "error" in run or run.get("correctness") not in ("exact", "tolerance")
        for run in baseline + candidate
    ):
        return None
    left, right = defaultdict(list), defaultdict(list)
    for runs, target in ((baseline, left), (candidate, right)):
        for run in runs:
            if metric in run:
                target[run["round"]].append(run[metric])
    rounds = sorted(left.keys() & right.keys())
    ratios = [statistics.median(left[r]) / statistics.median(right[r]) for r in rounds]
    if not ratios:
        return None
    estimate = math.exp(statistics.mean(math.log(r) for r in ratios))
    if len(rounds) < 3:
        return estimate, None, None, len(rounds)
    rng = random.Random(1)
    estimates = [
        math.exp(
            statistics.mean(math.log(x) for x in rng.choices(ratios, k=len(ratios)))
        )
        for _ in range(samples)
    ]
    return estimate, quantile(estimates, 0.025), quantile(estimates, 0.975), len(rounds)


def load_results(directory):
    records = []
    for path in sorted(directory.glob("results-*.jsonl")):
        with path.open(encoding="utf-8") as source:
            records.extend(json.loads(line) for line in source if line.strip())
    return records


def validate(records, directory, suites, rounds):
    failures = []
    by_instance = defaultdict(list)
    by_query = defaultdict(list)
    for r in records:
        key = (r["suite"], r["variant"], r["round"], r["query"], r["rep"])
        by_instance[key].append(r)
        by_query[(r["suite"], r["query"], r["round"])].append(r)
        if "error" in r:
            failures.append(
                f"{r['suite']}/{r['query']} {r['variant']}: {r['error'][:300]}"
            )
        elif r.get("correctness") not in ("oracle", "exact", "tolerance"):
            failures.append(
                f"{r['suite']}/{r['query']} {r['variant']}: {r.get('correctness', 'unvalidated result')}"
            )
    expected = set()
    for suite in suites:
        for variant in VARIANTS + ["spark"]:
            expected_rounds = (
                range(rounds) if variant != "spark" or suite == "fuzz" else [0]
            )
            for round_number in expected_rounds:
                path = (
                    directory / "manifests" / f"{suite}-{variant}-{round_number}.json"
                )
                if not path.is_file():
                    failures.append(f"Missing manifest: {path.name}")
                    continue
                manifest = json.loads(path.read_text(encoding="utf-8"))
                if not manifest.get("queries") or manifest.get("reps", 0) < 1:
                    failures.append(f"Empty manifest: {path.name}")
                for query in manifest["queries"]:
                    for rep in range(manifest["reps"]):
                        key = (suite, variant, round_number, query["query"], rep)
                        expected.add(key)
                        runs = by_instance[key]
                        if len(runs) != 1:
                            failures.append(
                                f"Expected one result for {key}, found {len(runs)}"
                            )
                        elif runs[0].get("sql_sha256") != query["sql_sha256"]:
                            failures.append(f"SQL does not match manifest: {key}")
    unexpected = set(by_instance) - expected
    failures.extend(f"Unexpected result: {key}" for key in sorted(unexpected))
    for key, runs in by_query.items():
        hashes = {r.get("sql_sha256") for r in runs}
        if len(hashes) > 1:
            failures.append(f"Variants ran different SQL: {key}")
    return failures


def fmt(value, digits=0):
    return "n/a" if value is None else f"{value:,.{digits}f}"


def ratio_cell(ratio):
    if ratio is None:
        return "n/a"
    if ratio[1] is None:
        return f"{ratio[0]:.2f}x (insufficient rounds)"
    return f"{ratio[0]:.2f}x [{ratio[1]:.2f}, {ratio[2]:.2f}]"


def mib(value):
    return "n/a" if value is None else f"{value / 1048576:,.1f}"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--rounds", type=int, default=4)
    parser.add_argument(
        "--suites", default="join,topk_minmax,layouts,fuzz,tpch,strjoin"
    )
    args = parser.parse_args(argv)
    suites = args.suites.split(",")
    records = load_results(args.directory)
    failures = validate(records, args.directory, suites, args.rounds)
    grouped = defaultdict(list)
    for record in records:
        grouped[(record["suite"], record["query"], record["variant"])].append(record)
    queries = sorted({(r["suite"], r["query"]) for r in records})
    print("## Validation\n")
    print(
        f"{len(queries)} queries; {len(records)} records; {len(failures)} validation failures."
    )
    print(
        "All results are checked against plain Spark. TPC-H permits only floating-point error"
    )
    print("within relative 1e-9 or absolute 1e-8; all other values remain exact.\n")
    tolerated = sum(r.get("correctness") == "tolerance" for r in records)
    print(
        f"Results accepted by semantic/tolerance comparison rather than an identical digest: {tolerated}.\n"
    )
    print("## Matched comparison\n")
    print(
        "Ratios are original / rewritten; above 1 is faster. Headline time includes SQL analysis,"
    )
    print(
        "physical planning and execution. Each round starts a fresh JVM; repetitions are clustered"
    )
    print(
        "within that JVM. Intervals bootstrap matched round medians, with four independent rounds."
    )
    print("These are warm-cache local-disk measurements on shared CI runners.")
    print("Comparisons containing wrong results or query errors are excluded from speedup ratios.\n")
    print(
        "| suite | measured queries | geomean original-on / rewritten-on | 95% wins | 95% losses |"
    )
    print("|---|---:|---:|---:|---:|")
    for suite in suites:
        if suite == "fuzz":
            continue
        ratios = []
        for _, query in [key for key in queries if key[0] == suite]:
            ratio = paired_ratio(
                grouped[(suite, query, "baseline_on")],
                grouped[(suite, query, "candidate_on")],
            )
            if ratio:
                ratios.append(ratio)
        geomean = (
            math.exp(statistics.mean(math.log(r[0]) for r in ratios))
            if ratios
            else None
        )
        wins = sum(r[1] is not None and r[1] > 1 for r in ratios)
        losses = sum(r[2] is not None and r[2] < 1 for r in ratios)
        print(f"| {suite} | {len(ratios)} | {fmt(geomean, 2)}x | {wins} | {losses} |")
    equality_cases = [
        (suite, query) for suite, query in queries if "eq_deletes" in query
    ]
    if equality_cases:
        print("\n## High-cardinality equality deletes\n")
        print(
            "Integer and string fixtures contain 4,096 genuine equality-delete keys. Ratios compare"
        )
        print(
            "the same delete files and fact data; off/off also isolates reader/delete evaluation.\n"
        )
        print("| query | old/new filters off, 95% CI | old/new filters on, 95% CI |")
        print("|---|---|---|")
        for suite, query in equality_cases:
            off = paired_ratio(
                grouped[(suite, query, "baseline_off")],
                grouped[(suite, query, "candidate_off")],
            )
            on = paired_ratio(
                grouped[(suite, query, "baseline_on")],
                grouped[(suite, query, "candidate_on")],
            )
            print(f"| {suite}/{query} | {ratio_cell(off)} | {ratio_cell(on)} |")
    fuzz_cases = [key for key in queries if key[0] == "fuzz"]
    if fuzz_cases:
        print("\n## Fuzz coverage\n")
        print(
            "Generated cases have a Spark oracle per seed; timing ratios are excluded.\n"
        )
        print(
            "| variant | validated records | cases pruning files/RGs | physical fallbacks |"
        )
        print("|---|---:|---:|---:|")
        for variant in VARIANTS:
            runs = [
                r
                for suite, query in fuzz_cases
                for r in grouped[(suite, query, variant)]
            ]
            correct = sum(r.get("correctness") in ("exact", "tolerance") for r in runs)
            pruned = sum(
                bool(
                    r.get("iceberg_runtime_file_tasks_pruned")
                    or r.get("iceberg_runtime_row_groups_pruned")
                )
                for r in runs
            )
            fallbacks = sum(
                r.get("native_iceberg_scans") == 0 for r in runs if "error" not in r
            )
            print(f"| {variant} | {correct} | {pruned} | {fallbacks} |")
    fallback = []
    regression = []
    for suite in suites:
        if suite == "fuzz":
            continue
        print(f"\n## {suite}: time and pruning benefit\n")
        print(
            "| query | old off ms | old on ms | new off ms | new on ms | old/new total, 95% CI | old/new execution, 95% CI | old pruning | new pruning |"
        )
        print("|---|---:|---:|---:|---:|---|---|---|---|")
        for _, query in [key for key in queries if key[0] == suite]:
            runs = {v: grouped[(suite, query, v)] for v in VARIANTS}
            total = paired_ratio(runs["baseline_on"], runs["candidate_on"])
            execution = paired_ratio(
                runs["baseline_on"], runs["candidate_on"], "exec_ms"
            )
            old_pruning = paired_ratio(runs["baseline_off"], runs["baseline_on"])
            new_pruning = paired_ratio(runs["candidate_off"], runs["candidate_on"])
            times = " | ".join(fmt(median(runs[v], "total_ms")) for v in VARIANTS)
            print(
                f"| {query} | {times} | {ratio_cell(total)} | {ratio_cell(execution)} | {ratio_cell(old_pruning)} | {ratio_cell(new_pruning)} |"
            )
            if total and total[2] is not None and total[2] < 0.9:
                regression.append(f"{suite}/{query}: {ratio_cell(total)}")
            if any(
                median(runs[v], "native_iceberg_scans") == 0
                for v in ("baseline_on", "candidate_on")
            ):
                fallback.append(f"{suite}/{query}")
        print(f"\n## {suite}: per-variant scan work\n")
        print(
            "Missing counters are n/a. Ranged reader I/O is independent of the OS cache; live pruning"
        )
        print(
            "depends on task scheduling and is summarized as a distribution. Every SQL stage metric"
        )
        print("and each physical plan is included in the artifact.\n")
        print(
            "| query | variant | SQL ms | plan ms | exec ms, IQR | MiB read | rows out | splits | predicate tasks | files pruned | RG pruned | live RG, min/max | refreshes | decoder rebuilds |"
        )
        print("|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|---|---:|---:|")
        for _, query in [key for key in queries if key[0] == suite]:
            for variant in VARIANTS:
                runs = grouped[(suite, query, variant)]
                values = [r["exec_ms"] for r in runs if "exec_ms" in r]
                iqr = (
                    "n/a"
                    if not values
                    else f"{fmt(statistics.median(values))} [{fmt(quantile(values, .25))}, {fmt(quantile(values, .75))}]"
                )
                live = [
                    r["iceberg_runtime_row_groups_pruned_live"]
                    for r in runs
                    if r.get("iceberg_runtime_row_groups_pruned_live") is not None
                ]
                med = lambda name: median(runs, name)
                live_range = (
                    "n/a"
                    if not live
                    else f"{fmt(statistics.median(live))} [{min(live)}, {max(live)}]"
                )
                print(
                    f"| {query} | {variant} | {fmt(med('sql_ms'))} | {fmt(med('plan_ms'))} | {iqr} | {mib(med('bytes_scanned'))} | {fmt(med('output_rows'))} | {fmt(med('num_splits'))} | {fmt(med('iceberg_runtime_predicate_tasks'))} | {fmt(med('iceberg_runtime_file_tasks_pruned'))} | {fmt(med('iceberg_runtime_row_groups_pruned'))} | {live_range} | {fmt(med('iceberg_runtime_predicate_refreshes'))} | {fmt(med('iceberg_runtime_decoder_rebuilds'))} |"
                )
    print("\n## Queries with at least one physical fallback\n")
    print("Inspect the captured plans before attributing timing changes to pruning.\n")
    for query in fallback:
        print(f"- {query}")
    print("\n## Possible regressions (entire interval below 0.9x)\n")
    print("\n".join(f"- {r}" for r in regression) or "None observed.")
    if failures:
        print("\n## Validation failures\n")
        for failure in failures:
            print(f"- {failure.replace(chr(10), ' ')}")
    return int(bool(failures))


if __name__ == "__main__":
    raise SystemExit(main())
