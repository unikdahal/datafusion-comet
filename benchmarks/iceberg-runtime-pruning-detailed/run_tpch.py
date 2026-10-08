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

"""Run the 22 TPC-H queries over both Iceberg layouts in one Spark JVM.

The queries come from benchmarks/tpc/queries/tpch. Each query is warmed BENCH_WARMUPS times per layout and
then timed BENCH_REPS times; every result is checksummed.
"""

import glob
import os
from pathlib import Path
import re
import sys

from lib import append, env, query_order, record_warmup, run_query, write_manifest

VARIANT = env("BENCH_VARIANT", "x")
ROUND = int(env("BENCH_ROUND", "0"))
REPS = int(env("BENCH_REPS", "1"))
OUTPUT = env("BENCH_OUTPUT", "results.jsonl")
QUERY_DIR = env("TPCH_QUERIES", str(Path(__file__).resolve().parents[1] / "tpc/queries/tpch"))
DATABASES = env("TPCH_DBS", "tpch_nat,tpch_clu").split(",")


def statements(path):
    text = open(path, encoding="utf-8").read()
    parts = [p.strip() for p in text.split(";")]
    return [p for p in parts if re.sub(r"--[^\n]*", "", p).strip()]


def is_select(sql):
    body = re.sub(r"--[^\n]*", "", sql).strip().lower()
    return body.startswith("select") or body.startswith("with") or body.startswith("(")


def query_files():
    files = sorted(
        glob.glob(os.path.join(QUERY_DIR, "q*.sql")),
        key=lambda p: int(re.findall(r"\d+", p)[-1]),
    )
    if len(files) != 22:
        raise RuntimeError(f"Expected 22 TPC-H query files, found {len(files)}")
    return files


def query_name(database, path, index):
    number = int(re.findall(r"\d+", os.path.basename(path))[0])
    suffix = f"_{index}" if len(statements(path)) > 2 else ""
    return f"{database}__q{number:02d}{suffix}"


def queries_for():
    manifest_queries = []
    for database in DATABASES:
        for path in query_files():
            for index, sql in enumerate(statements(path)):
                if is_select(sql):
                    # Include the current namespace in the oracle key.
                    qualified = qualify(sql, database)
                    manifest_queries.append(
                        (query_name(database, path, index), qualified, False, None)
                    )
    return manifest_queries


def main():
    from pyspark.sql import SparkSession
    from run_suite import select_queries

    spark = SparkSession.builder.appName(f"tpch-pruning-{VARIANT}").getOrCreate()
    manifest_queries = select_queries(queries_for(), "tpch", ROUND)
    selected = {q[0] for q in manifest_queries}
    reps = 1 if VARIANT == "spark" else REPS
    write_manifest(manifest_queries, "tpch", VARIANT, ROUND, reps)
    failed = False
    for database in DATABASES:
        files = [
            path for path in query_files()
            if any(query_name(database, path, index) in selected
                   for index, sql in enumerate(statements(path)) if is_select(sql))
        ]
        spark.sql(f"USE bench.{database}")
        warmups = 0 if VARIANT == "spark" else int(env("BENCH_WARMUPS", "2"))
        if VARIANT != "spark" and warmups < 1:
            raise ValueError("Timed native variants must warm their workload")
        for repetition in range(reps + warmups):
            warmed = repetition < warmups
            # Shuffle whole query files, preserving q15's create/select/drop sequence.
            for path in query_order(files, ROUND, repetition, warmup=warmed):
                for index, sql in enumerate(statements(path)):
                    sql = re.sub(r"(?i)create\s+view", "create temp view", sql)
                    if not is_select(sql):
                        spark.sql(sql).collect()
                        continue
                    name = query_name(database, path, index)
                    if name not in selected:
                        continue
                    record = run_query(
                        spark, name, qualify(sql, database), ordered=False
                    )
                    failed |= "error" in record or record.get("correctness") not in (
                        "oracle",
                        "exact",
                        "tolerance",
                    )
                    if warmed:
                        record_warmup(record, "tpch", VARIANT, ROUND, repetition)
                        continue
                    rep = repetition - warmups
                    record.update(
                        {
                            "suite": "tpch",
                            "variant": VARIANT,
                            "round": ROUND,
                            "rep": rep,
                        }
                    )
                    append(OUTPUT, record)
    spark.stop()
    return int(failed)


def qualify(sql, database):
    # USE selects the namespace before execution. Include it in the SQL digest
    # without rewriting identifiers: q8 and q9 also use "nation" as an alias.
    # A comment preserves those aliases and q15's temporary view references.
    return f"-- benchmark namespace: bench.{database}\n{sql}"


if __name__ == "__main__":
    sys.exit(main())
