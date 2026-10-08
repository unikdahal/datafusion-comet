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
from collections import Counter, defaultdict
import json
import math
from pathlib import Path
import random
import re
import shutil
import statistics

from lib import VARIANTS
from resolve_revisions import MAIN_REPOSITORY, fixed_sha
from run_suite import PARAMETERS, SUITES, partition_queries, query_catalog


def validate_provenance(directory, suites):
    failures, common = [], None
    for suite in suites:
        paths = sorted(directory.glob(f"environment-{suite}-shard-*.json"))
        if not paths:
            paths = [directory / f"environment-{suite}.json"]
        for path in paths:
            errors, resolved = validate_environment(path)
            failures.extend(errors)
            if resolved is not None:
                if common is not None and resolved != common:
                    failures.append("Shards used different resolved revisions")
                common = resolved
    return failures, common


def validate_environment(path):
    failures, resolved = [], None
    try:
        environment = json.loads(path.read_text())
        resolved = environment["resolved_revisions"]
        baseline = resolved["baseline"]
        if (baseline["repository"] != MAIN_REPOSITORY or baseline["ref"] != "refs/heads/main"):
            raise ValueError("Baseline is not Apache Comet main")
        for side in ("baseline", "candidate"):
            commit = fixed_sha(resolved[side]["comet"])
            build = environment["builds"][side]
            if build["comet"] != commit or build["resolved_revisions"] != resolved:
                raise ValueError(f"Build provenance differs from the resolver: {side}")
            dependency = [p for p in build["dependencies"] if p["name"] == "iceberg"]
            if dependency != [resolved[side]["iceberg_dependency"]]:
                raise ValueError(f"Locked Iceberg dependency differs from the resolver: {side}")
    except (OSError, ValueError, KeyError, TypeError) as error:
        failures.append(f"Invalid provenance for {path.name}: {error}")
    return failures, resolved


def merge_shards(directory, resolved):
    """Verify against the resolver's independent catalog, then join raw measurements.

    Keep downloaded shard artifacts intact. The derived directory lets the existing
    statistical/metric report consume one logical suite without dropping any samples.
    """
    campaign = resolved["campaign"]
    failures = []
    merged = directory / "merged"
    if merged.exists():
        shutil.rmtree(merged)
    (merged / "manifests").mkdir(parents=True)
    environments = []
    if campaign["parameters"] != PARAMETERS:
        failures.append("Resolved protocol differs from the report's canonical protocol")
    if set(campaign["suites"]) != set(SUITES) and not campaign["partial"]:
        failures.append("Omitted suites were not declared as a partial campaign")
    expected_artifacts = {f"matched-{m['suite']}-shard-{m['shard']}" for m in campaign["matrix"]}
    observed_artifacts = {p.name for p in directory.glob("matched-*") if p.is_dir()}
    if expected_artifacts != observed_artifacts:
        failures.append(f"Shard artifacts differ: missing={sorted(expected_artifacts - observed_artifacts)}, unexpected={sorted(observed_artifacts - expected_artifacts)}")
    for suite in campaign["suites"]:
        item = next(m for m in campaign["data_matrix"] if m["suite"] == suite)
        shards = item["shards"]
        catalogs = {str(r): query_catalog(suite, r) for r in range(int(PARAMETERS["rounds"]))}
        if catalogs != campaign["catalog"][suite]:
            failures.append(f"Resolved catalog differs from independently enumerated full query set: {suite}")
        requested = campaign["selection"]["queries"].get(suite)
        selected = {r: [q["query"] for q in queries if requested is None or q["query"] in requested]
                    for r, queries in catalogs.items()}
        if selected != campaign["query_sets"][suite]:
            failures.append(f"Selected query set differs from campaign request: {suite}")
        if any(len(selected[r]) != len(catalogs[r]) for r in catalogs) and not campaign["partial"]:
            failures.append(f"Omitted queries were not declared as partial: {suite}")
        assignments = {r: partition_queries(names, shards, suite) for r, names in selected.items()}
        if assignments != campaign["assignments"][suite]:
            failures.append(f"Shard assignments differ from deterministic partition: {suite}")
        snapshot = None
        for shard in range(shards):
            root = directory / f"matched-{suite}-shard-{shard}"
            suffix = f"{suite}-shard-{shard}"
            environment_path = root / f"environment-{suffix}.json"
            errors, provenance = validate_environment(environment_path)
            failures.extend(errors)
            if provenance != resolved:
                failures.append(f"Shard provenance differs from campaign: {suffix}")
            if environment_path.is_file():
                environment = json.loads(environment_path.read_text())
                if (environment.get("suite"), environment.get("shard"), environment.get("shards")) != (suite, shard, shards):
                    failures.append(f"Runner shard identity differs: {suffix}")
                if not all(k in environment.get("runner", {}) for k in ("cpu_model", "cores", "memory_mib", "cpuinfo", "free_m")):
                    failures.append(f"Missing runner hardware metadata: {suffix}")
                environments.append(environment)
                shutil.copyfile(environment_path, merged / environment_path.name)
            try:
                data = json.loads((root / f"data-{suffix}.json").read_text())
                if not data or (snapshot is not None and snapshot != data):
                    failures.append(f"Shards did not use byte-identical shared data: {suffix}")
                snapshot = data
            except (OSError, ValueError) as error:
                failures.append(f"Missing/invalid data digests for {suffix}: {error}")
            for prefix in ("results", "warmup-validation"):
                if prefix == "warmup-validation" and suite == "fuzz":
                    continue
                source = root / f"{prefix}-{suffix}.jsonl"
                if not source.is_file():
                    failures.append(f"Missing raw records: {source.name}")
                    continue
                for line in source.read_text().splitlines():
                    if line.strip():
                        record = json.loads(line)
                        if (record.get("suite"), record.get("shard"), record.get("shards")) != (suite, shard, shards):
                            failures.append(f"Raw record shard identity differs: {source.name}")
                            break
                with (merged / f"{prefix}-{suite}.jsonl").open("ab") as target:
                    target.write(source.read_bytes())
            plans = root / "plans" / suite / f"shard-{shard}"
            if plans.is_dir():
                # Preserve every shard's plans, including differing plans for identical SQL.
                shutil.copytree(plans, merged / "plans" / suite / f"shard-{shard}")
        for variant in VARIANTS + ["spark"]:
            expected_rounds = range(int(PARAMETERS["rounds"])) if variant != "spark" or suite == "fuzz" else [0]
            for round_number in expected_rounds:
                r = str(round_number)
                union = []
                for shard in range(shards):
                    path = directory / f"matched-{suite}-shard-{shard}" / "manifests" / f"{suite}-{variant}-{round_number}-shard-{shard}.json"
                    expected_names = assignments[r][shard]
                    expected_queries = [q for q in catalogs[r] if q["query"] in expected_names]
                    reps = 1 if variant == "spark" or suite == "fuzz" else int(SUITES[suite]["reps"])
                    warmups = 0 if variant == "spark" or suite == "fuzz" else int(PARAMETERS["warmups"])
                    try:
                        manifest = json.loads(path.read_text())
                        identity = (manifest["suite"], manifest["variant"], manifest["round"], manifest["shard"], manifest["shards"], manifest["reps"], manifest["warmups"])
                        if identity != (suite, variant, round_number, shard, shards, reps, warmups):
                            failures.append(f"Manifest protocol/identity differs: {path.name}")
                        queries = manifest["queries"]
                        if sorted(queries, key=lambda q: q["query"]) != expected_queries:
                            failures.append(f"Shard query set/SQL differs from full catalog assignment: {path.name}")
                        union.extend(q["query"] for q in queries)
                    except (OSError, ValueError, KeyError, TypeError) as error:
                        failures.append(f"Missing/invalid shard manifest {path.name}: {error}")
                # For a full campaign, selected[r] is the full query set. Partial mode
                # changes only that explicit set, never the measurement protocol.
                if Counter(union) != Counter(selected[r]):
                    failures.append(f"Union of shard query sets != expected full/selected query set: {suite}/{variant}/{r}; missing={sorted(set(selected[r]) - set(union))}")
                logical = {"suite": suite, "variant": variant, "round": round_number,
                           "reps": reps, "warmups": warmups, "allow_empty": not selected[r],
                           "queries": [q for q in catalogs[r] if q["query"] in selected[r]]}
                (merged / "manifests" / f"{suite}-{variant}-{round_number}.json").write_text(json.dumps(logical))
    return merged, failures, environments


def report_shard_environment(environments, resolved):
    campaign = resolved["campaign"]
    label = "PARTIAL CAMPAIGN" if campaign["partial"] else "Full campaign"
    print(f"## {label}\n")
    print(f"Suites: {', '.join(campaign['suites'])}. Query selections: `{json.dumps(campaign['selection']['queries'], sort_keys=True)}`.\n")
    print("Each shard retains all eight balanced rounds, the original warmups/repetitions and Spark oracle checks.\n")
    print("## Runner hardware per shard\n")
    print("| suite | shard | CPU model | cores | memory MiB | data cache hit | baseline/candidate build cache hit |")
    print("|---|---:|---|---:|---:|---|---|")
    for environment in environments:
        runner = environment.get("runner", {})
        hits = "/".join(str(environment["builds"][s].get("cache_hit")) for s in ("baseline", "candidate"))
        print(f"| {environment['suite']} | {environment['shard']} / {environment['shards']} | {runner.get('cpu_model', 'missing')} | {runner.get('cores', 'missing')} | {runner.get('memory_mib', 'missing')} | {environment.get('data_cache_hit')} | {hits} |")
    print()


def quantile(values, q):
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def median(records, name):
    values = [r[name] for r in records if r.get(name) is not None]
    return statistics.median(values) if values else None


def stage_metric(record, name):
    """Sum an operator counter without turning absent instrumentation into zero."""
    values = [
        stage["metrics"][name]
        for stage in record.get("stages", [])
        if stage.get("metrics", {}).get(name) is not None
    ]
    return sum(values) if values else None


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


def native_scan_coverage(directory, suite, variant, sql_digest):
    """Identify counted native scans, including repeats of one table in a join.

    A dimension can remain native while its fact scan falls back to Spark. Counting
    native scans alone therefore cannot establish comparable reader-byte coverage.
    Metadata locations identify the shared immutable tables in captured plans.
    An unavailable or unfamiliar plan remains unknown, never an empty scan set.
    """
    if sql_digest is None:
        return None
    paths = sorted((directory / "plans" / suite).glob(f"shard-*/{variant}/{sql_digest}.txt"))
    if not paths:
        paths = [directory / "plans" / suite / variant / f"{sql_digest}.txt"]
    coverages = []
    for path in paths:
        try:
            plan = path.read_text()
        except OSError:
            return None
        coverage = scan_coverage(plan)
        if coverage is None:
            return None
        coverages.append(coverage)
    return coverages[0] if all(c == coverages[0] for c in coverages) else None


def scan_coverage(plan):
    if not plan.strip():
        return None
    locations = []
    for line in plan.splitlines():
        if "CometIcebergNativeScan" not in line:
            continue
        match = re.search(r", ([^,\n]+\.metadata\.json),", line)
        if match is None:
            return None
        locations.append(match.group(1))
    return tuple(sorted(Counter(locations).items()))


def validate(records, directory, suites, rounds, invalid_queries=None, empty_rounds=None):
    failures = []
    empty_rounds = set() if empty_rounds is None else empty_rounds
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
                if (not manifest.get("queries") and (suite, round_number) not in empty_rounds) or manifest.get("reps", 0) < 1:
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
    parser.add_argument("--resolved", type=Path)
    parser.add_argument(
        "--suites", default="join,topk_minmax,layouts,fuzz,tpch,strjoin"
    )
    args = parser.parse_args(argv)
    suites = args.suites.split(",")
    shard_failures = []
    empty_rounds = set()
    if args.resolved:
        resolved = json.loads(args.resolved.read_text())
        suites = resolved["campaign"]["suites"]
        empty_rounds = {(suite, int(r)) for suite in suites
                        for r, names in resolved["campaign"]["query_sets"][suite].items() if not names}
        if args.rounds != int(resolved["campaign"]["parameters"]["rounds"]):
            shard_failures.append("Report round count differs from resolved campaign")
        args.directory, errors, environments = merge_shards(args.directory, resolved)
        shard_failures.extend(errors)
        report_shard_environment(environments, resolved)
    records = load_results(args.directory)
    invalid_queries = set()
    failures = validate(records, args.directory, suites, args.rounds, invalid_queries, empty_rounds)
    failures.extend(shard_failures)
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
                not provenance_failures and not shard_failures
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
    coverage_changes = []
    regression = []
    inspection = []
    byte_increases = []
    valid_timings = set()
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
            if total:
                valid_timings.add((suite, query))
            if total and total[2] is not None and total[2] < 1:
                inspection.append(f"{suite}/{query}: {ratio_cell(total)}")
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
        print("and each physical plan is included in the artifact.")
        print(
            "Zero reader predicate tasks does not imply zero runtime filtering: a join can"
        )
        print(
            "filter decoded batches after a Spark exchange. Batch filtering is reported separately; its time is aggregate operator work."
        )
        print(
            "Bytes and output rows cover native Iceberg scans only, including dimension scans."
        )
        print(
            "A Spark fact-scan fallback can leave a small native dimension counter. Compare bytes"
        )
        print(
            "only after checking the counted native tables; different or unknown coverage is not an I/O gain or increase.\n"
        )
        print(
            "| query | variant | SQL ms | plan ms | exec ms, IQR | native MiB read | native rows out | native scan coverage vs main-on | splits | predicate tasks | files pruned | RG pruned | live RG, min/max | refreshes | decoder rebuilds | batch rows evaluated | batch rows pruned | batch eval ms |"
        )
        print(
            "|---|---|---:|---:|---|---:|---:|---|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|"
        )
        for _, query in [key for key in queries if key[0] == suite]:
            query_runs = [
                r for variant in VARIANTS for r in grouped[(suite, query, variant)]
            ]
            digests = {r.get("sql_sha256") for r in query_runs}
            digest = next(iter(digests)) if len(digests) == 1 else None
            main_coverage = native_scan_coverage(
                args.directory, suite, "baseline_on", digest
            )
            candidate_coverage = native_scan_coverage(
                args.directory, suite, "candidate_on", digest
            )
            if (
                main_coverage is not None
                and candidate_coverage is not None
                and main_coverage != candidate_coverage
            ):
                coverage_changes.append(f"{suite}/{query}")
            main_bytes = median(grouped[(suite, query, "baseline_on")], "bytes_scanned")
            candidate_bytes = median(
                grouped[(suite, query, "candidate_on")], "bytes_scanned"
            )
            if (
                (suite, query) in valid_timings
                and main_coverage is not None
                and main_coverage == candidate_coverage
                and main_bytes is not None
                and candidate_bytes is not None
                and candidate_bytes > main_bytes
            ):
                byte_increases.append(
                    f"{suite}/{query}: main {main_bytes:,.0f}, candidate {candidate_bytes:,.0f} native bytes requested"
                )
            for variant in VARIANTS:
                runs = grouped[(suite, query, variant)]
                coverage = native_scan_coverage(args.directory, suite, variant, digest)
                scope = (
                    "unknown"
                    if main_coverage is None or coverage is None
                    else (
                        "same native tables"
                        if coverage == main_coverage
                        else "different native tables"
                    )
                )
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
                batch = {
                    key: median([{key: stage_metric(r, key)} for r in runs], key)
                    for key in (
                        "dynamic_filter_join_rows_evaluated",
                        "dynamic_filter_join_rows_pruned",
                        "dynamic_filter_join_eval_time",
                    )
                }
                batch_ms = batch["dynamic_filter_join_eval_time"]
                batch_ms = None if batch_ms is None else batch_ms / 1_000_000
                live_range = (
                    "n/a"
                    if not live
                    else f"{fmt(statistics.median(live))} [{min(live)}, {max(live)}]"
                )
                print(
                    f"| {query} | {variant} | {fmt(med('sql_ms'))} | {fmt(med('plan_ms'))} | {iqr} | {mib(med('bytes_scanned'))} | {fmt(med('output_rows'))} | {scope} | {fmt(med('num_splits'))} | {fmt(med('iceberg_runtime_predicate_tasks'))} | {fmt(med('iceberg_runtime_file_tasks_pruned'))} | {fmt(med('iceberg_runtime_row_groups_pruned'))} | {live_range} | {fmt(med('iceberg_runtime_predicate_refreshes'))} | {fmt(med('iceberg_runtime_decoder_rebuilds'))} | {fmt(batch['dynamic_filter_join_rows_evaluated'])} | {fmt(batch['dynamic_filter_join_rows_pruned'])} | {fmt(batch_ms)} |"
                )
    print("\n## Queries with at least one physical fallback\n")
    print("Inspect the captured plans before attributing timing changes to pruning.\n")
    for query in fallback:
        print(f"- {query}")
    print("\n## Queries with different counted native scan coverage\n")
    print(
        "Correct timing comparisons remain valid, but native-only byte totals cannot measure the cross-implementation I/O difference. Inspect both plans, including partial Spark fallbacks.\n"
    )
    print("\n".join(f"- {query}" for query in coverage_changes) or "None observed.")
    print("\n## Possible regressions (entire interval below 0.9x)\n")
    print("\n".join(f"- {r}" for r in regression) or "None observed.")
    print("\n## Cases requiring closer inspection (entire interval below parity)\n")
    print(
        "Exploratory intervals are not adjusted for multiple comparisons and do not establish the cause.\n"
    )
    print("\n".join(f"- {r}" for r in inspection) or "None observed.")
    print("\n## Native byte increases with the same counted scan coverage\n")
    print(
        "Inspect predicate/index reads, coalesced gaps and repeated requests before attributing a cause.\n"
    )
    print("\n".join(f"- {r}" for r in byte_increases) or "None observed.")
    if failures:
        print("\n## Validation failures\n")
        for failure in failures:
            print(f"- {failure.replace(chr(10), ' ')}")
    return int(bool(failures))


if __name__ == "__main__":
    raise SystemExit(main())
