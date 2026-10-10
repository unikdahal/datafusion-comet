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
    if not isinstance(manifest, dict) or manifest.get("protocol_version") != 1:
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
        for field in ("plan_ms", "exec_ms", "total_ms"):
            value = record.get(field)
            if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                raise ValidationError(f"record {index}: invalid {field}")
        if record["exec_ms"] <= 0 or not math.isclose(
            record["total_ms"], record["plan_ms"] + record["exec_ms"], rel_tol=1e-6, abs_tol=1e-6
        ):
            raise ValidationError(f"record {index}: inconsistent timing fields")
        for field in SCAN_METRICS:
            value = record.get(field)
            if value is not None and not integer(value):
                raise ValidationError(f"record {index}: invalid counter {field}")
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
        if len({r["checksum"] for r in runs}) != 1 or len({r["rows"] for r in runs}) != 1:
            raise ValidationError(f"result mismatch: {query} (checksum or row count)")
    return list(queries), by_key


def hex_digest(value, lengths):
    if isinstance(lengths, int):
        lengths = (lengths,)
    return isinstance(value, str) and len(value) in lengths and all(c in "0123456789abcdef" for c in value)


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


def mib(value):
    return f"{value / (1024 * 1024):,.1f}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results")
    parser.add_argument("--manifest", required=True, help="experiment declared before collecting samples")
    args = parser.parse_args()
    try:
        manifest = read_json(Path(args.manifest).read_text(encoding="utf-8"))
        queries, by_key = validate_records(load_records(args.results), manifest)
    except (ValidationError, OSError) as error:
        print(f"Benchmark validation failed: {error}", file=sys.stderr)
        return 1
    rng = random.Random(1)

    print("## Correctness\n")
    print("| query | identical results across variants | rows |")
    print("|---|---|---|")
    for query in queries:
        checksums = {r["checksum"] for v in VARIANTS for r in by_key[(query, v)]}
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
    print("including metadata and deletes. Live pruning can vary with scheduling.\n")
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

    return 0


if __name__ == "__main__":
    sys.exit(main())
