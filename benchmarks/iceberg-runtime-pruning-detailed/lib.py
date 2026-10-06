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

"""Shared helpers for the detailed runtime pruning benchmark."""

import hashlib
import json
import math
import os
import time

# Number of ids per calendar day in the date-keyed tables.
IDS_PER_DAY = 4000

# (name, Spark SQL type) for every key flavour a table can use. The key column is always `id`.
KEY_TYPES = ["int", "long", "str", "date", "dec", "nulls", "nan"]


def typed(ktype, expr):
    """SQL expression that maps an integer expression onto the key type."""
    if ktype in ("int", "nulls"):
        return f"cast({expr} as int)"
    if ktype == "long":
        return f"cast({expr} as bigint)"
    if ktype == "str":
        return f"concat('k', lpad(cast({expr} as string), 10, '0'))"
    if ktype == "date":
        return f"date_add(DATE'2000-01-01', cast(({expr}) / {IDS_PER_DAY} as int))"
    if ktype == "dec":
        return f"cast({expr} as decimal(18,2))"
    if ktype == "nan":
        return f"cast({expr} as double)"
    raise ValueError(ktype)


def plan_nodes(node):
    yield node
    children = node.children()
    for index in range(children.size()):
        yield from plan_nodes(children.apply(index))


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


def scan_metrics(plan):
    totals = {name: 0 for name in SCAN_METRICS}
    scans = 0
    for node in plan_nodes(plan):
        if node.getClass().getSimpleName() != "CometIcebergNativeScanExec":
            continue
        scans += 1
        metrics = node.metrics()
        for name in SCAN_METRICS:
            if metrics.contains(name):
                totals[name] += metrics.apply(name).value()
    totals["native_iceberg_scans"] = scans
    return totals


def normalize(value):
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        return f"{value:.6g}"
    return value


def digest(rows, ordered):
    items = [repr(tuple(normalize(v) for v in row)) for row in rows]
    if not ordered:
        items.sort()
    h = hashlib.sha256()
    for item in items:
        h.update(item.encode())
        h.update(b"\n")
    return h.hexdigest()[:16]


def run_query(spark, name, sql, ordered=True, confs=None):
    """Plan and execute one query; returns a record with timings, checksum and scan metrics."""
    confs = confs or {}
    previous = {key: spark.conf.get(key, None) for key in confs}
    for key, value in confs.items():
        spark.conf.set(key, value)
    try:
        df = spark.sql(sql)
        started = time.perf_counter()
        plan = df._jdf.queryExecution().executedPlan()
        planned = time.perf_counter()
        rows = df.collect()
        finished = time.perf_counter()
    except Exception as error:  # noqa: BLE001 - recorded, not raised
        return {"query": name, "error": str(error).splitlines()[0][:300]}
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)
    return {
        "query": name,
        "plan_ms": (planned - started) * 1000.0,
        "exec_ms": (finished - planned) * 1000.0,
        "total_ms": (finished - started) * 1000.0,
        "rows": len(rows),
        "checksum": digest(rows, ordered),
        "sample": repr([tuple(normalize(v) for v in row) for row in rows[:3]])[:300] if len(rows) <= 3 else "",
        **scan_metrics(plan),
    }


def env(name, default):
    return os.environ.get(name, default)


def append(path, record):
    with open(path, "a", encoding="utf-8") as out:
        out.write(json.dumps(record) + "\n")
