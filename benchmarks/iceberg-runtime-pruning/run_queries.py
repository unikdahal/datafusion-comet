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

"""Run every benchmark query in one Spark JVM and append JSON lines.

The variant is described by environment variables; the Comet jar and the
Comet settings are supplied by spark-submit. Each query is warmed once, then
timed BENCH_REPS times in a rotated order. Planning and execution are timed
separately so driver-side planning costs stay visible.
"""

import hashlib
import json
import os
import time
from pathlib import Path

from pyspark.sql import SparkSession
from workloads import QUERIES, sql_digest

VARIANT = os.environ["BENCH_VARIANT"]
ROUND = int(os.environ["BENCH_ROUND"])
REPS = int(os.environ.get("BENCH_REPS", "3"))
OUTPUT = os.environ["BENCH_OUTPUT"]

SCAN_METRICS = [
    "bytes_scanned",
    "output_rows",
    "num_splits",
    "iceberg_runtime_predicate_tasks",
    "iceberg_runtime_file_tasks_pruned",
    "iceberg_runtime_row_groups_pruned",
    "iceberg_runtime_row_groups_pruned_live",
    "iceberg_runtime_predicate_refreshes",
    "iceberg_runtime_decoder_rebuilds",
]


def plan_nodes(node):
    yield node
    children = node.children()
    for index in range(children.size()):
        yield from plan_nodes(children.apply(index))


def scan_metrics(plan):
    totals = {name: 0 for name in SCAN_METRICS}
    missing = set()
    scans = 0
    locations = []
    for node in plan_nodes(plan):
        if node.getClass().getSimpleName() != "CometIcebergNativeScanExec":
            continue
        scans += 1
        try:
            locations.append(str(node.metadataLocation()))
        except Exception:
            # Older/newer baseline APIs may omit the accessor. Unknown scope
            # must never become an apparent reader-I/O improvement.
            locations.append(None)
        metrics = node.metrics()
        for name in SCAN_METRICS:
            if metrics.contains(name):
                totals[name] += metrics.apply(name).value()
            else:
                missing.add(name)
    for name in SCAN_METRICS:
        if scans == 0 or name in missing:
            totals[name] = None
    totals["native_iceberg_scans"] = scans
    totals["native_scan_metadata"] = sorted(locations) if all(locations) else None
    return totals


def run(spark, name, sql, identity):
    started = time.perf_counter()
    df = spark.sql(sql)
    plan = df._jdf.queryExecution().executedPlan()
    planned = time.perf_counter()
    rows = df.collect()
    finished = time.perf_counter()
    result = repr([tuple(row) for row in rows])
    plans = Path(os.environ.get("BENCH_PLAN_DIR", "plans"))
    plans.mkdir(parents=True, exist_ok=True)
    (plans / f"{VARIANT}-{ROUND}-{identity}-{name}.txt").write_text(plan.toString(), encoding="utf-8")
    return {
        "query": name,
        "sql_sha256": sql_digest(sql),
        "plan_ms": (planned - started) * 1000.0,
        "exec_ms": (finished - planned) * 1000.0,
        "total_ms": (finished - started) * 1000.0,
        "rows": len(rows),
        "checksum": hashlib.sha256(result.encode()).hexdigest()[:16],
        "schema_json": df.schema.json(),
        **scan_metrics(plan),
    }


def main():
    spark = SparkSession.builder.appName(f"iceberg-runtime-pruning-{VARIANT}").getOrCreate()
    plans = Path(os.environ.get("BENCH_PLAN_DIR", "plans"))
    plans.mkdir(parents=True, exist_ok=True)
    settings = {key: value for key, value in spark.sparkContext.getConf().getAll()
                if key.startswith(("spark.comet.", "spark.sql.", "spark.memory.", "spark.shuffle.", "spark.plugins"))}
    (plans / f"{VARIANT}-{ROUND}-settings.json").write_text(json.dumps(settings, indent=2), encoding="utf-8")
    with open(OUTPUT, "a", encoding="utf-8") as out:
        for name, sql in QUERIES:
            run(spark, name, sql, "warmup")
        for rep in range(REPS):
            shift = (ROUND * REPS + rep) % len(QUERIES)
            for name, sql in QUERIES[shift:] + QUERIES[:shift]:
                record = run(spark, name, sql, f"rep-{rep}")
                record.update({"variant": VARIANT, "round": ROUND, "rep": rep})
                out.write(json.dumps(record) + "\n")
                out.flush()
    spark.stop()


if __name__ == "__main__":
    main()
