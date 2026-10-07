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
from resolve_revisions import MAIN_REPOSITORY, fixed_sha


def validate_provenance(directory, suites):
    failures, common = [], None
    for suite in suites:
        path = directory / f"environment-{suite}.json"
        try:
            environment = json.loads(path.read_text())
            resolved = environment["resolved_revisions"]
            baseline = resolved["baseline"]
            if (
                baseline["repository"] != MAIN_REPOSITORY
                or baseline["ref"] != "refs/heads/main"
            ):
                raise ValueError("Baseline is not Apache Comet main")
            for side in ("baseline", "candidate"):
                commit = fixed_sha(resolved[side]["comet"])
                build = environment["builds"][side]
                if build["comet"] != commit or build["resolved_revisions"] != resolved:
                    raise ValueError(
                        f"Build provenance differs from the resolver: {side}"
                    )
                dependency = [
                    p for p in build["dependencies"] if p["name"] == "iceberg"
                ]
                if dependency != [resolved[side]["iceberg_dependency"]]:
                    raise ValueError(
                        f"Locked Iceberg dependency differs from the resolver: {side}"
                    )
            if common is not None and resolved != common:
                raise ValueError("Suites used different resolved revisions")
            common = resolved
        except (OSError, ValueError, KeyError, TypeError) as error:
            failures.append(f"Invalid provenance for {suite}: {error}")
    return failures, common


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
        "error" in run
        or run.get("correctness") not in ("exact", "tolerance")
        or run.get("comparison_validated") is False
        for run in baseline + candidate
    ):
        return None
    # A plausible ratio from a partial or duplicated comparison is still invalid.
    identities = lambda runs: [(r["round"], r["rep"]) for r in runs]
    left_ids, right_ids = identities(baseline), identities(candidate)
    if (
        len(set(left_ids)) != len(left_ids)
        or len(set(right_ids)) != len(right_ids)
        or set(left_ids) != set(right_ids)
    ):
        return None
    hashes = {r.get("sql_sha256") for r in baseline + candidate}
    if len(hashes) != 1 or None in hashes:
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


def validate(records, directory, suites, rounds, invalid_queries=None):
    failures = []
    invalid = set() if invalid_queries is None else invalid_queries

    def fail(message, query=None, suite=None):
        failures.append(message)
        if query is not None:
            invalid.add(query)
        elif suite is not None:
            invalid.update(
                (r["suite"], r["query"]) for r in records if r["suite"] == suite
            )

    by_instance = defaultdict(list)
    by_query = defaultdict(list)
    for r in records:
        key = (r["suite"], r["variant"], r["round"], r["query"], r["rep"])
        by_instance[key].append(r)
        by_query[(r["suite"], r["query"], r["round"])].append(r)
        if "error" in r:
            fail(
                f"{r['suite']}/{r['query']} {r['variant']}: {r['error'][:300]}",
                (r["suite"], r["query"]),
            )
        elif r.get("correctness") not in ("oracle", "exact", "tolerance"):
            fail(
                f"{r['suite']}/{r['query']} {r['variant']}: {r.get('correctness', 'unvalidated result')}",
                (r["suite"], r["query"]),
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
                    fail(f"Missing manifest: {path.name}", suite=suite)
                    continue
                manifest = json.loads(path.read_text(encoding="utf-8"))
                if not manifest.get("queries") or manifest.get("reps", 0) < 1:
                    fail(f"Empty manifest: {path.name}", suite=suite)
                for query in manifest["queries"]:
                    for rep in range(manifest["reps"]):
                        key = (suite, variant, round_number, query["query"], rep)
                        expected.add(key)
                        runs = by_instance[key]
                        if len(runs) != 1:
                            fail(
                                f"Expected one result for {key}, found {len(runs)}",
                                (suite, query["query"]),
                            )
                        elif runs[0].get("sql_sha256") != query["sql_sha256"]:
                            fail(
                                f"SQL does not match manifest: {key}",
                                (suite, query["query"]),
                            )
    unexpected = set(by_instance) - expected
    for key in sorted(unexpected):
        fail(f"Unexpected result: {key}", (key[0], key[3]))
    for key, runs in by_query.items():
        hashes = {r.get("sql_sha256") for r in runs}
        if len(hashes) > 1:
            fail(f"Variants ran different SQL: {key}", (key[0], key[1]))
    return failures


def validate_warmups(directory, suites, require=False):
    failures, invalid = [], set()
    for suite in suites:
        manifests = [
            json.loads(p.read_text())
            for p in (directory / "manifests").glob(f"{suite}-*.json")
        ]
        queries = {(suite, q["query"]) for m in manifests for q in m["queries"]}
        expected = {}
        for manifest in manifests:
            if "warmups" not in manifest and require:
                failures.append(
                    f"Missing warmup count: {suite}/{manifest['variant']}/{manifest['round']}"
                )
                invalid.update(queries)
                continue
            count = manifest.get("warmups", 0)
            if (
                type(count) is not int
                or count < 0
                or (
                    require
                    and suite != "fuzz"
                    and manifest["variant"] != "spark"
                    and count < 1
                )
            ):
                failures.append(
                    f"Invalid warmup count: {suite}/{manifest['variant']}/{manifest['round']}"
                )
                invalid.update(queries)
                continue
            for query in manifest["queries"]:
                for iteration in range(count):
                    key = (
                        suite,
                        manifest["variant"],
                        manifest["round"],
                        query["query"],
                        iteration,
                    )
                    if key in expected:
                        failures.append(f"Duplicate warmup manifest instance: {key}")
                        invalid.add((suite, query["query"]))
                    expected[key] = query["sql_sha256"]
        observed = defaultdict(list)
        path = directory / f"warmup-validation-{suite}.jsonl"
        if path.is_file():
            try:
                for line in path.read_text().splitlines():
                    if not line.strip():
                        continue
                    r = json.loads(line)
                    key = (
                        r["suite"],
                        r["variant"],
                        r["round"],
                        r["query"],
                        r["iteration"],
                    )
                    observed[key].append(r)
            except (ValueError, KeyError, TypeError) as error:
                failures.append(f"Malformed warmup validation for {suite}: {error}")
                invalid.update(queries)
        for key, digest in expected.items():
            runs = observed[key]
            if len(runs) != 1 or runs[0].get("sql_sha256") != digest:
                failures.append(f"Warmup missing, duplicated or SQL differs: {key}")
                invalid.add((key[0], key[3]))
            elif "error" in runs[0] or runs[0].get("correctness") not in (
                "exact",
                "tolerance",
            ):
                failures.append(
                    f"Warmup failed: {key}: {runs[0].get('error',runs[0].get('correctness'))}"
                )
                invalid.add((key[0], key[3]))
        for key in set(observed) - set(expected):
            failures.append(f"Unexpected warmup: {key}")
            invalid.update(queries)
    return failures, invalid


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
    parser.add_argument("--require-latest-main", action="store_true")
    parser.add_argument(
        "--suites", default="join,topk_minmax,layouts,fuzz,tpch,strjoin"
    )
    args = parser.parse_args(argv)
    suites = args.suites.split(",")
    records = load_results(args.directory)
    invalid_queries = set()
    failures = validate(records, args.directory, suites, args.rounds, invalid_queries)
    warmup_failures, warmup_invalid = validate_warmups(
        args.directory, suites, require=args.require_latest_main
    )
    failures.extend(warmup_failures)
    invalid_queries.update(warmup_invalid)
    provenance_failures = []
    if args.require_latest_main:
        provenance_failures, provenance = validate_provenance(args.directory, suites)
        failures.extend(provenance_failures)
        if provenance is not None:
            print("## Resolved implementations\n")
            print(
                f"Apache main: `{provenance['baseline']['comet']}`; candidate: `{provenance['candidate']['comet']}`."
            )
            print(
                f"Main resolved once at {provenance['resolved_at_utc']}; each implementation retains its locked dependencies.\n"
            )
    records = [
        dict(
            r,
            comparison_validated=(
                not provenance_failures
                and (r["suite"], r["query"]) not in invalid_queries
            ),
        )
        for r in records
    ]
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
        "Ratios are baseline / candidate; above 1 is faster. Headline time includes SQL analysis,"
    )
    print(
        "physical planning and execution. Each round starts a fresh JVM; repetitions are clustered"
    )
    print(
        f"within that JVM. Exploratory intervals bootstrap matched medians from {args.rounds} JVM rounds."
    )
    print(
        "Intervals are not adjusted for multiple comparisons and are not causal or significance verdicts."
    )
    print("These are warm-cache local-disk measurements on shared CI runners.")
    print(
        "Comparisons containing wrong/missing/duplicate timed or warmup results, or invalid provenance, are excluded from speedup ratios.\n"
    )
    print("## JVM diagnostics\n")
    print(
        "Driver-wide GC/JIT deltas overlap query work; they are not executor CPU or peak RSS.\n"
    )
    print(
        "| suite | variant | samples with GC / monitored samples | median GC ms | median compilation ms |"
    )
    print("|---|---|---:|---:|---:|")
    for suite in suites:
        for variant in VARIANTS:
            runs = [
                r
                for r in records
                if r["suite"] == suite and r["variant"] == variant and "error" not in r
            ]
            diagnostics = [r.get("runtime_diagnostics", {}) for r in runs]
            monitored = [d for d in diagnostics if d.get("driver_gc_count") is not None]
            gc = sum(d["driver_gc_count"] > 0 for d in monitored)
            print(
                f"| {suite} | {variant} | {gc} / {len(monitored)} | {fmt(median(diagnostics, 'driver_gc_ms'), 2)} | {fmt(median(diagnostics, 'driver_compilation_ms'), 2)} |"
            )
    print()
    print(
        "| suite | measured queries | geomean baseline-on / candidate-on | exploratory intervals above 1 | below 1 |"
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
