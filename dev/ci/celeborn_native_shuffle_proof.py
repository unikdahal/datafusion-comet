#!/usr/bin/env python3
#
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

import argparse
import hashlib
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def digest_rows(rows):
    payload = json.dumps([list(row) for row in rows], separators=(",", ":"), sort_keys=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def prepare(spark, root):
    root = Path(root)
    left = root / "left"
    right = root / "right"
    spark.range(0, 250_000, 1, 48).selectExpr(
        "id",
        "CAST(id % 257 AS INT) AS k",
        "CAST((id * 17) % 100003 AS BIGINT) AS v"
    ).write.mode("overwrite").parquet(str(left))
    spark.range(0, 257, 1, 8).selectExpr(
        "CAST(id AS INT) AS k",
        "CAST(id * 3 + 7 AS BIGINT) AS w"
    ).write.mode("overwrite").parquet(str(right))


def verify_loaded_celeborn(spark):
    jvm = spark._jvm
    modifier = jvm.java.lang.reflect.Modifier
    required = [
        ("org.apache.celeborn.common.network.client.TransportClientFactory", "clientBootstraps"),
        ("org.apache.celeborn.common.network.client.TransportClient", "channel"),
        ("org.apache.celeborn.common.network.client.TransportResponseHandler", "outstandingPushes"),
        ("org.apache.celeborn.client.ShuffleClientImpl", "pushDataRetryPool"),
    ]
    loader = jvm.Thread.currentThread().getContextClassLoader()
    for owner_name, field_name in required:
        owner = jvm.java.lang.Class.forName(owner_name, False, loader)
        field = owner.getDeclaredField(field_name)
        modifiers = field.getModifiers()
        assert modifier.isVolatile(modifiers), f"{owner_name}.{field_name} is not volatile"
        assert not modifier.isFinal(modifiers), f"{owner_name}.{field_name} is still final"

    client = jvm.java.lang.Class.forName(
        "org.apache.celeborn.client.ShuffleClientImpl", False, loader
    )
    source = client.getProtectionDomain().getCodeSource().getLocation().toString()
    print("CELEBORN_CODE_SOURCE=" + source)
    assert "celeborn-client-spark-3-shaded_2.12-0.7.0" in source, source

    byte_buffer = jvm.java.lang.Class.forName("java.nio.ByteBuffer", False, loader)
    int_type = jvm.java.lang.Integer.TYPE
    direct_push = client.getMethod(
        "pushDataDirect",
        int_type,
        int_type,
        int_type,
        int_type,
        byte_buffer,
        int_type,
        int_type,
        int_type,
    )
    direct_crc = client.getMethod(
        "computeBatchCRCDirect",
        int_type,
        int_type,
        int_type,
        int_type,
        byte_buffer,
        int_type,
    )
    assert direct_push.getReturnType() == int_type
    assert direct_crc.getReturnType() == jvm.java.lang.Void.TYPE
    assert not modifier.isStatic(direct_push.getModifiers())
    assert not modifier.isStatic(direct_crc.getModifiers())
    print("CELEBORN_DIRECT_PUSH_CONTRACT_OK")


def run_query(spark, root, mode, output):
    root = Path(root)
    left = spark.read.parquet(str(root / "left"))
    right = spark.read.parquet(str(root / "right"))

    result = (
        left.filter((F.col("id") % 11) != 0)
        .repartition(64, "k")
        .join(right, "k")
        .groupBy("k")
        .agg(
            F.count("*").alias("rows"),
            F.sum("v").alias("sum_v"),
            F.sum("w").alias("sum_w"),
        )
        .repartition(32, "k")
        .orderBy("k")
    )

    rows = result.collect()
    plan = result._jdf.queryExecution().executedPlan().toString()
    print("EXECUTED_PLAN_BEGIN")
    print(plan)
    print("EXECUTED_PLAN_END")

    if mode == "comet":
        verify_loaded_celeborn(spark)
        assert "CometExchange" in plan, "expected at least one native Comet exchange"

    summary = {
        "digest": digest_rows(rows),
        "groups": len(rows),
        "row_count_sum": sum(int(r["rows"]) for r in rows),
        "sum_v": sum(int(r["sum_v"]) for r in rows),
        "sum_w": sum(int(r["sum_w"]) for r in rows),
    }
    Path(output).write_text(json.dumps(summary, sort_keys=True) + "\n")
    print("PROOF_RESULT=" + json.dumps(summary, sort_keys=True))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["prepare", "baseline", "comet"], required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--output")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    try:
        if args.mode == "prepare":
            prepare(spark, args.data)
        else:
            if not args.output:
                raise SystemExit("--output is required for run modes")
            run_query(spark, args.data, args.mode, args.output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
