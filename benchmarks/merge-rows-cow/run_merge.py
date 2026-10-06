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

"""Run every MERGE INTO case once for one variant and round.

Each case resets its target to the generated snapshot, times the MERGE, and
records the result checksum and whether native MergeRows ran. A failed MERGE
(for example a memory error) is recorded, not raised.
"""

import json
import os
import time

from pyspark.sql import SparkSession

UPDATE = "WHEN MATCHED THEN UPDATE SET t.value = s.value"
MIXED = (
    "WHEN MATCHED AND s.op = 'd' THEN DELETE "
    "WHEN MATCHED THEN UPDATE SET t.value = s.value "
    "WHEN NOT MATCHED THEN INSERT (id, value, payload) VALUES (s.id, s.value, 'new')"
)
ON = "t.id = s.id"
ON_P = "t.id = s.id AND t.p = s.p"
# case -> (target, source, join condition, clauses)
CASES = {
    "update_all": ("t_cow_4", "s_all", ON, UPDATE),
    "update_half": ("t_cow_4", "s_half", ON, UPDATE),
    "update_10pct": ("t_cow_4", "s_10pct", ON, UPDATE),
    "update_1pct": ("t_cow_4", "s_1pct", ON, UPDATE),
    "update_001pct": ("t_cow_4", "s_001pct", ON, UPDATE),
    "update_all_sorted_source": ("t_cow_4", "s_all_sorted", ON, UPDATE),
    "update_all_64_files": ("t_cow_64", "s_all", ON, UPDATE),
    "update_1pct_64_files": ("t_cow_64", "s_1pct", ON, UPDATE),
    "update_all_partitioned": ("t_cow_part", "sp_all", ON_P, UPDATE),
    "update_1pct_partitioned": ("t_cow_part", "sp_1pct", ON_P, UPDATE),
    "update_expression": (
        "t_cow_4",
        "s_half",
        ON,
        "WHEN MATCHED THEN UPDATE SET t.value = t.value + s.value, t.payload = upper(t.payload)",
    ),
    "delete_half": ("t_cow_4", "s_half", ON, "WHEN MATCHED THEN DELETE"),
    "delete_1pct": ("t_cow_4", "s_1pct", ON, "WHEN MATCHED THEN DELETE"),
    "conditional_update_delete": (
        "t_cow_4",
        "s_all",
        ON,
        "WHEN MATCHED AND s.id % 3 = 0 THEN DELETE "
        "WHEN MATCHED AND s.id % 3 = 1 THEN UPDATE SET t.value = s.value",
    ),
    "upsert_update_insert": (
        "t_cow_4",
        "s_mixed",
        ON,
        "WHEN MATCHED THEN UPDATE SET t.value = s.value "
        "WHEN NOT MATCHED THEN INSERT (id, value, payload) VALUES (s.id, s.value, 'new')",
    ),
    "mixed_delete_update_insert": ("t_cow_4", "s_mixed", ON, MIXED),
    # Insert-only and no-match: no row rewrites, a control for everything else.
    "insert_only": (
        "t_cow_4",
        "s_new",
        ON,
        "WHEN NOT MATCHED THEN INSERT (id, value, payload) VALUES (s.id, s.value, 'new')",
    ),
    "matched_clause_no_match": ("t_cow_4", "s_new", ON, UPDATE),
    # Duplicate source rows: the MERGE must fail; the time is how long failing takes.
    "cardinality_violation": ("t_cow_4", "s_dup", ON, UPDATE),
}


def fallback_notes(spark, statement):
    """Comet's own explanation of every operator it left on the JVM, one line each."""
    text = "\n".join(r[0] for r in spark.sql(f"EXPLAIN EXTENDED {statement}").collect())
    notes = {line.strip() for line in text.splitlines() if "[COMET:" in line or "Comet" in line and "cannot" in line}
    return sorted(notes)[:6]


def main():
    variant = os.environ["BENCH_VARIANT"]
    rnd = int(os.environ["BENCH_ROUND"])
    only = [c for c in os.environ.get("BENCH_CASES", "").split(",") if c]
    with open(os.environ["BENCH_SNAPSHOTS"]) as snapshots_file:
        snapshots = json.load(snapshots_file)
    spark = SparkSession.builder.appName(f"merge-rows-e2e-{variant}").getOrCreate()
    with open(os.environ["BENCH_OUTPUT"], "a") as out:
        for case, (target, source, on, clauses) in CASES.items():
            if only and case not in only:
                continue
            spark.sql(f"CALL bench.system.set_current_snapshot('db.{target}', {snapshots[target]})")
            spark.sql(f"REFRESH TABLE bench.db.{target}")
            statement = (
                f"MERGE INTO bench.db.{target} t USING bench.db.{source} s ON {on} {clauses}"
            )
            plan = "\n".join(r[0] for r in spark.sql(f"EXPLAIN {statement}").collect())
            native = "CometMergeRows" in plan
            record = {
                "variant": variant,
                "round": rnd,
                "case": case,
                "native_merge_rows": native,
            }
            if variant.startswith("comet") and not native:
                record["fallback"] = fallback_notes(spark, statement)
            start = time.perf_counter()
            try:
                spark.sql(statement)
                record["seconds"] = time.perf_counter() - start
                row = spark.sql(
                    f"SELECT count(*), sum(xxhash64(id, value, payload)) FROM bench.db.{target}"
                ).first()
                record["checksum"] = f"{row[0]}:{row[1]}"
            except Exception as error:  # noqa: BLE001 - recorded, not raised
                record["seconds"] = time.perf_counter() - start
                record["error"] = str(error).splitlines()[0][:300]
            print(json.dumps(record), flush=True)
            out.write(json.dumps(record) + "\n")
    spark.stop()


if __name__ == "__main__":
    main()
