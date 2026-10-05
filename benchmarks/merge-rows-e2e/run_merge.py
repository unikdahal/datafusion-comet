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
CASES = {
    "cow_update_all": ("t_cow_4", "s_all", UPDATE),
    "cow_update_1pct": ("t_cow_4", "s_1pct", UPDATE),
    "mor_update_all": ("t_mor_4", "s_all", UPDATE),
    "mor_update_half": ("t_mor_4", "s_half", UPDATE),
    "mor_update_all_64_files": ("t_mor_64", "s_all", UPDATE),
    "mor_delete_half": ("t_mor_4", "s_half", "WHEN MATCHED THEN DELETE"),
    "mor_mixed": (
        "t_mor_4",
        "s_mixed",
        "WHEN MATCHED AND s.op = 'd' THEN DELETE "
        "WHEN MATCHED THEN UPDATE SET t.value = s.value "
        "WHEN NOT MATCHED THEN INSERT (id, value, payload) VALUES (s.id, s.value, 'new')",
    ),
    # Insert-only: no cardinality check, a control for everything else.
    "mor_insert_only": (
        "t_mor_4",
        "s_mixed",
        "WHEN NOT MATCHED THEN INSERT (id, value, payload) VALUES (s.id, s.value, 'new')",
    ),
}


def main():
    variant = os.environ["BENCH_VARIANT"]
    rnd = int(os.environ["BENCH_ROUND"])
    only = [c for c in os.environ.get("BENCH_CASES", "").split(",") if c]
    with open(os.environ["BENCH_SNAPSHOTS"]) as snapshots_file:
        snapshots = json.load(snapshots_file)
    spark = SparkSession.builder.appName(f"merge-rows-e2e-{variant}").getOrCreate()
    with open(os.environ["BENCH_OUTPUT"], "a") as out:
        for case, (target, source, clauses) in CASES.items():
            if only and case not in only:
                continue
            spark.sql(f"CALL bench.system.set_current_snapshot('db.{target}', {snapshots[target]})")
            spark.sql(f"REFRESH TABLE bench.db.{target}")
            statement = (
                f"MERGE INTO bench.db.{target} t USING bench.db.{source} s ON t.id = s.id {clauses}"
            )
            plan = "\n".join(r[0] for r in spark.sql(f"EXPLAIN {statement}").collect())
            record = {
                "variant": variant,
                "round": rnd,
                "case": case,
                "native_merge_rows": "CometMergeRows" in plan,
            }
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
