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

"""Correctness, timing and metric helpers for the matched pruning benchmark."""

import datetime
import decimal
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import time

IDS_PER_DAY = 4000
KEY_TYPES = ["int", "long", "str", "date", "dec", "nulls", "nan"]
VARIANTS = ["baseline_off", "baseline_on", "candidate_off", "candidate_on"]
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


def env(name, default):
    return os.environ.get(name, default)


def append(path, record):
    with open(path, "a", encoding="utf-8") as out:
        out.write(json.dumps(record, allow_nan=False) + "\n")


def signature(sql, confs=None):
    return hashlib.sha256(
        json.dumps([sql, confs or {}], sort_keys=True).encode()
    ).hexdigest()


def plan_nodes(node, seen=None):
    seen = set() if seen is None else seen
    identity = node.id()
    if identity in seen:
        return
    seen.add(identity)
    yield node
    children = node.children()
    for index in range(children.size()):
        yield from plan_nodes(children.apply(index), seen)


def stage_metrics(plan):
    """Keep every available SQL metric, including scan, join, sort and exchange stages."""
    stages = []
    for node in plan_nodes(plan):
        metrics = {}
        iterator = node.metrics().iterator()
        while iterator.hasNext():
            entry = iterator.next()
            metrics[str(entry._1())] = int(entry._2().value())
        stages.append(
            {
                "id": node.id(),
                "node": node.getClass().getSimpleName(),
                "metrics": metrics,
            }
        )
    return stages


def scan_metrics(stages):
    scans = [s for s in stages if s["node"] == "CometIcebergNativeScanExec"]
    # Absence is not zero: old revisions do not expose every new counter.
    totals = {name: None for name in SCAN_METRICS}
    for name in SCAN_METRICS:
        values = [s["metrics"][name] for s in scans if name in s["metrics"]]
        if values:
            totals[name] = sum(values)
    return {**totals, "native_iceberg_scans": len(scans)}


def canonical(value):
    """Lossless JSON representation of SQL scalar/nested values; no float rounding."""
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        return {"float": value.hex()}
    if isinstance(value, decimal.Decimal):
        return {"decimal": str(value)}
    if isinstance(value, datetime.datetime):
        return {"timestamp": value.isoformat()}
    if isinstance(value, datetime.date):
        return {"date": value.isoformat()}
    if isinstance(value, (bytes, bytearray)):
        return {"bytes": bytes(value).hex()}
    if isinstance(value, dict):
        return {
            str(k): canonical(v)
            for k, v in sorted(value.items(), key=lambda x: str(x[0]))
        }
    if isinstance(value, (list, tuple)):
        return [canonical(v) for v in value]
    raise TypeError(f"Unrecognized SQL result type: {type(value).__name__}")


def result_rows(rows, ordered):
    result = [canonical(tuple(row)) for row in rows]
    if not ordered:
        result.sort(key=lambda row: json.dumps(row, sort_keys=True))
    return result


def digest(rows):
    h = hashlib.sha256()
    for row in rows:
        h.update(json.dumps(row, sort_keys=True, separators=(",", ":")).encode())
        h.update(b"\n")
    return h.hexdigest()


def equivalent(actual, expected, floating_tolerance=False):
    """Explicit tolerance applies only to TPC-H floating-point values; integers stay exact."""
    if type(actual) is not type(expected):
        return False
    if isinstance(actual, list):
        return len(actual) == len(expected) and all(
            equivalent(a, e, floating_tolerance) for a, e in zip(actual, expected)
        )
    if isinstance(actual, dict):
        if actual.keys() != expected.keys():
            return False
        if actual.keys() == {"float"}:
            a, e = float.fromhex(actual["float"]), float.fromhex(expected["float"])
            if math.isnan(a) or math.isnan(e):
                return math.isnan(a) and math.isnan(e)
            if a == e:
                return True
            return floating_tolerance and math.isclose(a, e, rel_tol=1e-9, abs_tol=1e-8)
        if actual.keys() == {"decimal"}:
            return decimal.Decimal(actual["decimal"]) == decimal.Decimal(
                expected["decimal"]
            )
        return all(
            equivalent(actual[k], expected[k], floating_tolerance) for k in actual
        )
    return actual == expected


def check_result(name, sql, confs, rows, ordered, schema=None):
    key = signature(sql, confs)
    directory = Path(os.environ["BENCH_EXPECTED"])
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{key}.json.gz"
    result = result_rows(rows, ordered)
    checksum = digest(result)
    if env("BENCH_VARIANT", "") == "spark":
        with gzip.open(path, "wt", encoding="utf-8") as out:
            json.dump(
                {
                    "query": name,
                    "sql_sha256": key,
                    "rows": result,
                    "checksum": checksum,
                    "schema": schema,
                },
                out,
            )
        return {"checksum": checksum, "correctness": "oracle", "sql_sha256": key}
    if not path.is_file():
        return {
            "checksum": checksum,
            "correctness": "missing oracle",
            "sql_sha256": key,
        }
    with gzip.open(path, "rt", encoding="utf-8") as source:
        expected = json.load(source)
    tolerance = env("BENCH_SUITE", "") == "tpch"
    passed = schema == expected.get("schema") and equivalent(
        result, expected["rows"], floating_tolerance=tolerance
    )
    exact = checksum == expected["checksum"]
    return {
        "checksum": checksum,
        "oracle_checksum": expected["checksum"],
        "sql_sha256": key,
        "correctness": (
            "exact" if passed and exact else "tolerance" if passed else "mismatch"
        ),
    }


def run_query(spark, name, sql, ordered=True, confs=None):
    """Time SQL construction, physical planning and collection; validate against plain Spark."""
    confs = confs or {}
    previous = {key: spark.conf.get(key, None) for key in confs}
    for key, value in confs.items():
        spark.conf.set(key, value)
    try:
        started = time.perf_counter()
        df = spark.sql(sql)
        constructed = time.perf_counter()
        plan = df._jdf.queryExecution().executedPlan()
        planned = time.perf_counter()
        rows = df.collect()
        finished = time.perf_counter()
        stages = stage_metrics(plan)
        result = check_result(name, sql, confs, rows, ordered, df.schema.json())
        plan_dir = Path(env("BENCH_PLANS", "plans")) / env("BENCH_VARIANT", "unknown")
        plan_dir.mkdir(parents=True, exist_ok=True)
        plan_path = plan_dir / f"{signature(sql, confs)}.txt"
        if not plan_path.exists():
            plan_path.write_text(
                f"-- {name}\n{sql}\n\n{plan.toString()}\n", encoding="utf-8"
            )
        return {
            "query": name,
            "sql_ms": (constructed - started) * 1000.0,
            "plan_ms": (planned - constructed) * 1000.0,
            "exec_ms": (finished - planned) * 1000.0,
            "total_ms": (finished - started) * 1000.0,
            "rows": len(rows),
            "sample": repr(rows[:3])[:500],
            "stages": stages,
            **result,
            **scan_metrics(stages),
        }
    except (
        Exception
    ) as error:  # Continue collecting diagnostics; the runner fails at the end.
        return {
            "query": name,
            "sql_sha256": signature(sql, confs),
            "error": str(error)[:2000],
        }
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)


def write_manifest(queries, suite, variant, round_number, reps):
    directory = Path(env("BENCH_MANIFESTS", "manifests"))
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{suite}-{variant}-{round_number}.json"
    manifest = {
        "suite": suite,
        "variant": variant,
        "round": round_number,
        "reps": reps,
        "queries": [
            {"query": name, "sql_sha256": signature(sql, confs)}
            for name, sql, _, confs in queries
        ],
    }
    path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
