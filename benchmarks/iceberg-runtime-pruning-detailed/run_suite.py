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

"""Launch the oracle and balanced original/rewrite comparisons on one runner."""

import os
from pathlib import Path
import subprocess
import sys

from lib import env

# Williams design: each native variant occupies every position once and each ordered
# adjacent pair appears once. All timed variants warm every query in their own JVM.
ORDERS = [
    ["baseline_off", "baseline_on", "candidate_on", "candidate_off"],
    ["baseline_on", "candidate_off", "baseline_off", "candidate_on"],
    ["candidate_off", "candidate_on", "baseline_on", "baseline_off"],
    ["candidate_on", "baseline_off", "candidate_off", "baseline_on"],
]


def arguments(variant):
    args = ["spark-submit", "--master", "local[4]", "--driver-memory", "7g"]
    settings = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.bench": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.bench.type": "hadoop",
        "spark.sql.catalog.bench.warehouse": os.environ["WAREHOUSE"],
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.shuffle.partitions": "4",
        "spark.sql.iceberg.aggregate-push-down.enabled": "false",
        "spark.ui.enabled": "false",
        "spark.sql.session.timeZone": "UTC",
    }
    if variant == "spark":
        args.extend(["--jars", "jars/iceberg.jar"])
    else:
        side, mode = variant.split("_")
        jar = str(Path(f"jars/pruning-{side}/comet.jar").resolve())
        if not Path(jar).is_file():
            raise FileNotFoundError(jar)
        enabled = "true" if mode == "on" else "false"
        args.extend(["--jars", f"jars/iceberg.jar,{jar}"])
        settings.update(
            {
                "spark.driver.extraClassPath": jar,
                "spark.executor.extraClassPath": jar,
                "spark.plugins": "org.apache.spark.CometPlugin",
                "spark.shuffle.manager": "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager",
                "spark.comet.exec.shuffle.enabled": "true",
                "spark.memory.offHeap.enabled": "true",
                "spark.memory.offHeap.size": "5g",
                "spark.comet.scan.icebergNative.enabled": "true",
                "spark.comet.expression.Cast.allowIncompatible": "true",
                "spark.comet.explain.fallback.enabled": "true",
                # Keep the same Top-K execution algorithm when switching pruning off.
                "spark.comet.exec.topK.fusion.enabled": "true",
                "spark.comet.exec.join.dynamicFilter.enabled": enabled,
                "spark.comet.exec.topK.dynamicFilter.enabled": enabled,
                "spark.comet.exec.aggregate.dynamicFilter.enabled": enabled,
            }
        )
    for key, value in settings.items():
        args.extend(["--conf", f"{key}={value}"])
    suite = os.environ["BENCH_SUITE"]
    script = "run_tpch.py" if suite == "tpch" else "run_queries.py"
    args.append(f"benchmarks/iceberg-runtime-pruning-detailed/{script}")
    return args


def launch(variant, round_number):
    suite = os.environ["BENCH_SUITE"]
    child_env = {
        **os.environ,
        "BENCH_VARIANT": variant,
        "BENCH_ROUND": str(round_number),
        "BENCH_SEED": str(round_number + 1),
    }
    path = Path(f"run-{suite}-{round_number}-{variant}.log")
    print(f"{suite}: round {round_number}, {variant}", flush=True)
    with path.open("w", encoding="utf-8") as out:
        result = subprocess.run(
            arguments(variant), env=child_env, stdout=out, stderr=subprocess.STDOUT
        )
    if result.returncode:
        print(f"Failed: {path}; exit={result.returncode}", flush=True)
        with path.open(encoding="utf-8") as source:
            tail = source.readlines()[-100:]
        print("".join(tail), flush=True)
    return result.returncode == 0


def main():
    rounds = int(env("BENCH_ROUNDS", "4"))
    if rounds != len(ORDERS):
        raise ValueError(
            "This benchmark requires four rounds for its balanced comparison"
        )
    suite = os.environ["BENCH_SUITE"]
    passed = True
    for round_number, order in enumerate(ORDERS):
        if round_number == 0 or suite == "fuzz":
            passed &= launch("spark", round_number)
        for variant in order:
            passed &= launch(variant, round_number)
    return int(not passed)


if __name__ == "__main__":
    sys.exit(main())
