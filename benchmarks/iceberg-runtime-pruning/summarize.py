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

Validate an independently declared experiment before printing any comparisons.
"""

import argparse
import json
import math
import random
import statistics
import sys
from collections import defaultdict
from pathlib import Path

VARIANTS = ["main", "off", "on"]
LABELS = {
    "main": "main",
    "off": "branch, runtime filters off",
    "on": "branch, runtime filters on",
}


class ValidationError(ValueError):
    """The input cannot support a complete, matched comparison."""


def integer(value, minimum=0):
    return type(value) is int and value >= minimum


def nonnegative_finite(value):
    if type(value) not in (int, float) or value < 0:
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValidationError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def read_json(text):
    def invalid_constant(value):
        raise ValidationError(f"invalid JSON constant: {value}")

    try:
        return json.loads(text, object_pairs_hook=unique_object, parse_constant=invalid_constant)
    except (ValueError, TypeError) as error:
        raise ValidationError(str(error)) from error


def load_records(path):
    records = []
    with open(path, encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                records.append(read_json(line))
            except ValidationError as error:
                raise ValidationError(f"line {line_number}: {error}") from error
    return records


def validate_records(records, manifest):
    """Require each declared query/variant/round/repetition exactly once.

    Never infer the experiment from the measurements: a wholly omitted query,
    round or variant must fail just like an omitted individual sample.
    """
    if not isinstance(manifest, dict) or type(manifest.get("protocol_version")) is not int or manifest.get("protocol_version") != 1:
        raise ValidationError("expected protocol_version 1 manifest")
    if manifest.get("variants") != VARIANTS:
        raise ValidationError("expected variants main, off, on")
    rounds, reps = manifest.get("rounds"), manifest.get("reps")
    if not integer(rounds, 1) or not integer(reps, 1):
        raise ValidationError("rounds and reps must be positive integers")
    catalog = manifest.get("queries")
    if not isinstance(catalog, list) or not catalog:
        raise ValidationError("expected a nonempty query catalog")
    queries = {}
    for entry in catalog:
        if not isinstance(entry, dict):
            raise ValidationError("invalid query catalog entry")
        name, digest = entry.get("query"), entry.get("sql_sha256")
        if not isinstance(name, str) or not name or not all(c.isalnum() or c == "_" for c in name):
            raise ValidationError("invalid query name")
        if name in queries or not hex_digest(digest, 64):
            raise ValidationError(f"duplicate query or invalid SQL digest: {name}")
        queries[name] = digest
    by_key = defaultdict(list)
    seen = set()
    for index, record in enumerate(records, 1):
        if not isinstance(record, dict):
            raise ValidationError(f"record {index}: expected an object")
        query, variant = record.get("query"), record.get("variant")
        if not isinstance(query, str) or query not in queries or variant not in VARIANTS:
            raise ValidationError(f"record {index}: unexpected query or variant")
        if record.get("sql_sha256") != queries[query]:
            raise ValidationError(f"record {index}: SQL differs from declared catalog")
        round_id, rep = record.get("round"), record.get("rep")
        if not integer(round_id) or round_id >= rounds or not integer(rep) or rep >= reps:
            raise ValidationError(f"record {index}: invalid round or repetition")
        identity = (query, variant, round_id, rep)
        if identity in seen:
            raise ValidationError(f"duplicate sample: {identity}")
        seen.add(identity)
        if record.get("error") is not None or record.get("status", "ok") != "ok":
            raise ValidationError(f"record {index}: unsuccessful sample")
        if not integer(record.get("rows")) or not hex_digest(record.get("checksum"), (16, 64)):
            raise ValidationError(f"record {index}: invalid or missing result")
        if not isinstance(record.get("schema_json"), str) or not record["schema_json"]:
            raise ValidationError(f"record {index}: missing result schema")
        for field in ("plan_ms", "exec_ms", "total_ms"):
            value = record.get(field)
            if not nonnegative_finite(value):
                raise ValidationError(f"record {index}: invalid {field}")
        if record["exec_ms"] <= 0 or not math.isclose(
            record["total_ms"], record["plan_ms"] + record["exec_ms"], rel_tol=1e-6, abs_tol=1e-6
        ):
            raise ValidationError(f"record {index}: inconsistent timing fields")
        for field in SCAN_METRICS:
            value = record.get(field)
            if value is not None and not integer(value):
                raise ValidationError(f"record {index}: invalid counter {field}")
        scope = record.get("native_scan_metadata")
        if scope is not None and (
            not isinstance(scope, list) or any(not isinstance(location, str) or not location for location in scope)
            or len(scope) != record.get("native_iceberg_scans")
        ):
            raise ValidationError(f"record {index}: invalid native scan scope")
        by_key[(query, variant)].append(record)
    expected_count = len(queries) * len(VARIANTS) * rounds * reps
    if len(seen) != expected_count:
        missing = next(
            (q, v, r, p)
            for q in queries for v in VARIANTS for r in range(rounds) for p in range(reps)
            if (q, v, r, p) not in seen
        )
        raise ValidationError(f"incomplete matrix: {len(seen)}/{expected_count} samples; missing {missing}")
    for query in queries:
        runs = [record for variant in VARIANTS for record in by_key[(query, variant)]]
        if any(len({r[field] for r in runs}) != 1 for field in ("checksum", "rows", "schema_json")):
            raise ValidationError(f"result mismatch: {query} (checksum, row count or schema)")
    return list(queries), by_key


def validate_oracle(records, queries, by_key):
    """Exactly one plain-Spark result per query, using the same SQL and snapshot."""
    seen = set()
    for record in records:
        if not isinstance(record, dict):
            raise ValidationError("invalid Spark oracle record")
        query = record.get("query")
        if not isinstance(query, str) or query not in queries or query in seen:
            raise ValidationError("unexpected or duplicate Spark oracle query")
        if record.get("variant") != "spark" or record.get("error") is not None or record.get("status", "ok") != "ok":
            raise ValidationError(f"invalid Spark oracle sample: {query}")
        if not integer(record.get("rows")) or not hex_digest(record.get("checksum"), (16, 64)):
            raise ValidationError(f"invalid Spark oracle result: {query}")
        reference = by_key[(query, "main")][0]
        if any(record.get(field) != reference[field] for field in ("sql_sha256", "checksum", "rows", "schema_json")):
            raise ValidationError(f"Spark oracle mismatch: {query}")
        seen.add(query)
    if seen != set(queries):
        raise ValidationError("incomplete Spark oracle")


def hex_digest(value, lengths):
    if isinstance(lengths, int):
        lengths = (lengths,)
    return isinstance(value, str) and len(value) in lengths and all(c in "0123456789abcdef" for c in value)


def same_native_scope(baseline, candidate):
    scopes = [r.get("native_scan_metadata") for r in baseline + candidate]
    return bool(scopes) and bool(scopes[0]) and all(scope == scopes[0] for scope in scopes)


SCAN_METRICS = (
    "bytes_scanned", "output_rows", "num_splits", "native_iceberg_scans",
    "iceberg_runtime_predicate_tasks", "iceberg_runtime_file_tasks_pruned",
    "iceberg_runtime_row_groups_pruned", "iceberg_runtime_row_groups_pruned_live",
    "iceberg_runtime_predicate_refreshes", "iceberg_runtime_decoder_rebuilds",
)


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


def round_ratio_ci(baseline, candidate, rng, samples=2000):
    """Diagnostic paired bootstrap of JVM-round medians, keeping reps clustered."""
    def round_medians(records):
        groups = defaultdict(list)
        for record in records:
            groups[record["round"]].append(record["exec_ms"])
        return {key: statistics.median(values) for key, values in groups.items()}

    base, cand = round_medians(baseline), round_medians(candidate)
    if base.keys() != cand.keys() or len(base) < 2:
        return None
    rounds = sorted(base)
    ratios = []
    for _ in range(samples):
        selected = rng.choices(rounds, k=len(rounds))
        ratios.append(statistics.median(base[r] for r in selected) / statistics.median(cand[r] for r in selected))
    return quantile(ratios, 0.025), quantile(ratios, 0.975)


def mib(value):
    return f"{value / (1024 * 1024):,.1f}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results")
    parser.add_argument("--manifest", required=True, help="experiment declared before collecting samples")
    parser.add_argument("--oracle", required=True, help="plain Spark results from the same generated tables")
    args = parser.parse_args()
    try:
        manifest = read_json(Path(args.manifest).read_text(encoding="utf-8"))
        queries, by_key = validate_records(load_records(args.results), manifest)
        validate_oracle(load_records(args.oracle), queries, by_key)
    except (ValidationError, OSError) as error:
        print(f"Benchmark validation failed: {error}", file=sys.stderr)
        return 1
    rng = random.Random(1)

    print("## Correctness\n")
    print("| query | identical results across variants and plain Spark | rows |")
    print("|---|---|---|")
    for query in queries:
        rows = {r["rows"] for v in VARIANTS for r in by_key[(query, v)]}
        print(f"| {query} | yes | {sorted(rows)} |")

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
    print("Ratio of pooled median execution times; >1 is faster. The original independent")
    print("sample bootstrap is retained. Paired JVM-round median intervals are exploratory")
    print("diagnostics with repetitions kept in their round; few rounds limit inference.\n")
    print("| query | vs main | independent 95% CI | paired round 95% CI | vs branch off | independent 95% CI | paired round 95% CI |")
    print("|---|---|---|---|---|---|---|")
    for query in queries:
        on = [r["exec_ms"] for r in by_key[(query, "on")]]
        cells = []
        for baseline in ["main", "off"]:
            base = [r["exec_ms"] for r in by_key[(query, baseline)]]
            if not on or not base:
                cells += ["-", "-"]
                continue
            low, high = ratio_ci(base, on, rng)
            paired = round_ratio_ci(by_key[(query, baseline)], by_key[(query, "on")], random.Random(1))
            interval = f"{paired[0]:.2f}-{paired[1]:.2f}" if paired else "n/a (<2 rounds)"
            cells += [f"{statistics.median(base) / statistics.median(on):.2f}x", f"{low:.2f}-{high:.2f}", interval]
        print(f"| {query} | " + " | ".join(cells) + " |")

    print("\n## Reader I/O and pruning (median per query run)\n")
    print("`bytes_scanned` is the byte total of ranged reads issued by the native Iceberg reader,")
    print("including metadata and deletes. Live pruning can vary with scheduling.\n")
    print("Counters cover native scans only; compare saved plans before attributing an I/O gain.")
    print("Missing counters are n/a. Driver planning includes SQL analysis and physical planning,\n")
    print("not an isolated measurement of manifest replanning or cache lookup.\n")
    print(
        "| query | variant | MiB read | vs main | file tasks | tasks with runtime predicate | "
        "files pruned before open | row groups pruned (live) | refreshes | decoder rebuilds |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|")
    for query in queries:
        def med(variant, name):
            values = [r.get(name) for r in by_key[(query, variant)]]
            return None if any(value is None for value in values) else statistics.median(values)

        def count(variant, name):
            value = med(variant, name)
            return "n/a" if value is None else f"{value:,.0f}"

        main_bytes = med("main", "bytes_scanned")
        for variant in VARIANTS:
            runs = by_key[(query, variant)]
            if not runs:
                continue

            read = med(variant, "bytes_scanned")
            scope_matches = same_native_scope(by_key[(query, "main")], runs)
            share = f"{read / main_bytes:.1%}" if main_bytes and read is not None and scope_matches else "n/a (scope/bytes)"
            read_text = "n/a" if read is None else mib(read)
            print(
                f"| {query} | {LABELS[variant]} | {read_text} | {share} | {count(variant, 'num_splits')} | "
                f"{count(variant, 'iceberg_runtime_predicate_tasks')} | "
                f"{count(variant, 'iceberg_runtime_file_tasks_pruned')} | "
                f"{count(variant, 'iceberg_runtime_row_groups_pruned')} "
                f"({count(variant, 'iceberg_runtime_row_groups_pruned_live')}) | "
                f"{count(variant, 'iceberg_runtime_predicate_refreshes')} | "
                f"{count(variant, 'iceberg_runtime_decoder_rebuilds')} |"
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
