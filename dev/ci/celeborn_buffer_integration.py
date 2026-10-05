# /*
#  * Licensed to the Apache Software Foundation (ASF) under one
#  * or more contributor license agreements.  See the NOTICE file
#  * distributed with this work for additional information
#  * regarding copyright ownership.  The ASF licenses this file
#  * to you under the Apache License, Version 2.0 (the
#  * "License"); you may not use this file except in compliance
#  * with the License.  You may obtain a copy of the License at
#  *
#  *   http://www.apache.org/licenses/LICENSE-2.0
#  *
#  * Unless required by applicable law or agreed to in writing,
#  * software distributed under the License is distributed on an
#  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  * KIND, either express or implied.  See the License for the
#  * specific language governing permissions and limitations
#  * under the License.
#  */
# 
import argparse
import hashlib
import json
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def queries(spark, root):
    left = spark.read.parquet(str(Path(root) / "left"))
    right = spark.read.parquet(str(Path(root) / "right"))
    joined = left.repartition(16, "k").join(right.repartition(16, "k"), "k")
    yield "join-aggregate", joined.groupBy("k").agg(
        F.count("*").alias("n"), F.sum("v").alias("v"), F.sum("w").alias("w")
    yield "range", left.repartitionByRange(16, "k").groupBy("k").agg(F.sum("v"))
    yield "single", left.repartition(1).agg(F.sum("v"), F.count("*"))
    yield "empty", left.filter("id < 0").repartition(16, "k").groupBy("k").count()
    yield "complex-data", left.select("k", F.array("id", "v").alias("a")).repartition(16, "k").groupBy("k").agg(F.sum(F.size("a")))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["prepare", "baseline", "heap", "direct"], required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--rows", type=int, default=1000000)
    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    try:
        if args.mode == "prepare":
            spark.range(args.rows, numPartitions=16).selectExpr(
                "id", "cast(id % 257 as int) as k", "cast(id * 17 % 100003 as bigint) as v"
            ).write.mode("overwrite").parquet(str(Path(args.data) / "left"))
            spark.range(257, numPartitions=4).selectExpr(
                "cast(id as int) as k", "cast(id * 3 + 7 as bigint) as w"
            ).write.mode("overwrite").parquet(str(Path(args.data) / "right"))
            return
        if args.mode != "baseline":
            cls = spark._jvm.java.lang.Class.forName("org.apache.celeborn.client.ShuffleClientImpl")
            assert any(m.getName() == "pushDataAsync" for m in cls.getMethods())
            assert spark._jvm.org.apache.comet.shuffle.CelebornShufflePartitionPusher.nativePushCompletionUnavailableReason(cls) is None
            print("BUFFER_CONTRACT_SOURCE=" + cls.getProtectionDomain().getCodeSource().getLocation().toString())
        records = {}
        # Rebuild each query for every measurement so previously materialized exchanges do not
        # turn repeats into reads of an already completed shuffle.
        for repeat in range(4):
            for name, query in queries(spark, args.data):
                start = time.perf_counter()
                rows = query.collect()
                elapsed = time.perf_counter() - start
                stable = sorted((list(row) for row in rows), key=lambda row: json.dumps(row))
                digest = hashlib.sha256(json.dumps(stable, separators=(",", ":")).encode()).hexdigest()
                plan = query._jdf.queryExecution().executedPlan().toString()
                if args.mode != "baseline" and name != "empty":
                    assert "CometExchange" in plan, (name, plan)
                record = records.setdefault(name, {"sha256": digest, "rows": len(rows), "seconds": []})
                assert record["sha256"] == digest
                if repeat > 0:
                    record["seconds"].append(elapsed)
                Path(args.output + "." + name + ".plan.txt").write_text(plan)
        Path(args.output).write_text(json.dumps(records, indent=2))
        print("RESULTS=" + json.dumps(records, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
