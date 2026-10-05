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

"""Summarize MERGE INTO benchmark results as Markdown."""

import json
import statistics
import sys
from collections import defaultdict

ORDER = ["spark", "comet-main", "comet-previous", "comet-pr"]


def main():
    runs = defaultdict(list)
    for line in open(sys.argv[1]):
        record = json.loads(line)
        runs[(record["case"], record["variant"])].append(record)
    cases = list(dict.fromkeys(case for case, _ in runs))
    variants = [v for v in ORDER if any(v == var for _, var in runs)] + sorted(
        {var for _, var in runs} - set(ORDER)
    )

    print("| case | " + " | ".join(variants) + " |")
    print("|---|" + "---:|" * len(variants))
    for case in cases:
        cells = []
        for variant in variants:
            records = runs.get((case, variant), [])
            ok = [r["seconds"] for r in records if "error" not in r]
            failed = len(records) - len(ok)
            if not records:
                cells.append("-")
                continue
            cell = f"{statistics.median(ok):.1f} s" if ok else "failed"
            if failed and ok:
                cell += f" ({failed} failed)"
            if variant != "spark" and not all(r["native_merge_rows"] for r in records):
                cell += " (JVM MergeRows)"
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
    errors = sorted({(r["variant"], r["case"], r["error"]) for rs in runs.values() for r in rs if "error" in r})
    if errors:
        print("\nFailures:\n")
        for variant, case, error in errors:
            print(f"- {variant} / {case}: `{error}`")


if __name__ == "__main__":
    main()
