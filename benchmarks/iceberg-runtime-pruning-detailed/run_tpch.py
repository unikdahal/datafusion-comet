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

The queries come from benchmarks/tpc/queries/tpch. Each query is warmed once per layout and
then timed BENCH_REPS times; every result is checksummed.
"""

import glob
import os
import re

from pyspark.sql import SparkSession

from lib import append, env, run_query

VARIANT = env("BENCH_VARIANT", "x")
ROUND = int(env("BENCH_ROUND", "0"))
REPS = int(env("BENCH_REPS", "1"))
OUTPUT = env("BENCH_OUTPUT", "results.jsonl")
QUERY_DIR = env("TPCH_QUERIES", "benchmarks/tpc/queries/tpch")
DATABASES = env("TPCH_DBS", "tpch_nat,tpch_clu").split(",")


def statements(path):
    text = open(path, encoding="utf-8").read()
    parts = [p.strip() for p in text.split(";")]
    return [p for p in parts if re.sub(r"--[^\n]*", "", p).strip()]


def is_select(sql):
    body = re.sub(r"--[^\n]*", "", sql).strip().lower()
    return body.startswith("select") or body.startswith("with") or body.startswith("(")


def main():
    spark = SparkSession.builder.appName(f"tpch-pruning-{VARIANT}").getOrCreate()
    files = sorted(glob.glob(os.path.join(QUERY_DIR, "q*.sql")), key=lambda p: int(re.findall(r"\d+", p)[-1]))
    for database in DATABASES:
        spark.sql(f"USE bench.{database}")
        for repetition in range(REPS + 1):  # the first pass is the warm-up
            for path in files:
                number = int(re.findall(r"\d+", os.path.basename(path))[0])
                for index, sql in enumerate(statements(path)):
                    sql = sql.replace("create view", "create temp view")
                    if not is_select(sql):
                        spark.sql(sql).collect()
                        continue
                    suffix = f"_{index}" if len(statements(path)) > 2 else ""
                    name = f"{database}__q{number:02d}{suffix}"
                    record = run_query(spark, name, sql, ordered=False)
                    if repetition == 0:
                        continue
                    record.update({"suite": "tpch", "variant": VARIANT, "round": ROUND, "rep": repetition - 1})
                    append(OUTPUT, record)
    spark.stop()


if __name__ == "__main__":
    main()
