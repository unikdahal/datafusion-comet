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

"""Summarize the copy-on-write MERGE INTO benchmark results as Markdown."""

import json
import statistics
import sys
from collections import defaultdict

ORDER = ["spark", "comet-main", "comet-pr", "comet-pr-jvmmerge"]


def main():
    runs = defaultdict(list)
    for line in open(sys.argv[1]):
        record = json.loads(line)
        runs[(record["case"], record["variant"])].append(record)
    cases = list(dict.fromkeys(case for case, _ in runs))
    variants = [v for v in ORDER if any(v == var for _, var in runs)] + sorted(
        {var for _, var in runs} - set(ORDER)
    )

    print("Median seconds over rounds; the factor is plain Spark's time divided by the variant's")
    print("(above 1.00x is faster than Spark). `native` means Comet's MergeRows ran,")
    print("`JVM` means MergeRows stayed on Spark's JVM operator.\n")
    print("| case | " + " | ".join(variants) + " |")
    print("|---|" + "---:|" * len(variants))
    spark_median = {}
    for case in cases:
        records = runs.get((case, "spark"), [])
        ok = [r["seconds"] for r in records if "error" not in r]
        if ok:
            spark_median[case] = statistics.median(ok)
    for case in cases:
        cells = []
        for variant in variants:
            records = runs.get((case, variant), [])
            if not records:
                cells.append("-")
                continue
            ok = [r["seconds"] for r in records if "error" not in r]
            if ok:
                med = statistics.median(ok)
                cell = f"{med:.1f} s"
            else:
                med = statistics.median(r["seconds"] for r in records)
                cell = f"error after {med:.1f} s"
            failed = len(records) - len(ok)
            if failed and ok:
                cell += f" ({failed} failed)"
            if variant != "spark" and case in spark_median and ok:
                cell += f" = {spark_median[case] / med:.2f}x"
            if variant.startswith("comet") and variant != "comet-pr-jvmmerge":
                cell += " native" if all(r["native_merge_rows"] for r in records) else " JVM"
            cells.append(cell)
        print(f"| {case} | " + " | ".join(cells) + " |")

    print()
    mismatches = []
    for case in cases:
        sums = {
            r["checksum"]
            for variant in variants
            for r in runs.get((case, variant), [])
            if "checksum" in r
        }
        if len(sums) > 1:
            mismatches.append(case)
    print("Results identical across variants: " + ("yes" if not mismatches else f"NO: {mismatches}"))
    fallback_report(runs)
    errors = sorted({(r["variant"], r["case"], r["error"]) for rs in runs.values() for r in rs if "error" in r})
    if errors:
        print("\nFailures:\n")
        for variant, case, error in errors:
            print(f"- {variant} / {case}: `{error}`")


def fallback_report(runs):
    reasons = defaultdict(set)
    for (case, variant), records in runs.items():
        for r in records:
            for note in r.get("fallback", []):
                reasons[(variant, case)].add(note)
    if not reasons:
        return
    print("\nWhy MergeRows stayed on the JVM (Comet's notes):\n")
    for (variant, case), notes in sorted(reasons.items()):
        if variant == "comet-pr-jvmmerge":
            continue
        print(f"- {variant} / {case}: " + "; ".join(f"`{n[:200]}`" for n in sorted(notes)))


if __name__ == "__main__":
    main()
