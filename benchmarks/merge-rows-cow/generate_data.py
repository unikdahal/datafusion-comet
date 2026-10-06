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

"""Generate the Iceberg tables for the MERGE INTO benchmark.

Run once with plain Spark and the Iceberg runtime (no Comet), so every variant
merges into byte-identical tables. Each target's starting snapshot is written to
BENCH_SNAPSHOTS so every run can roll back to it.
"""

import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROWS = int(os.environ.get("BENCH_ROWS", "5000000"))


def create(df, table, mode, files):
    props = {
        "format-version": "2",
        "write.target-file-size-bytes": str(1 << 30),
        "write.distribution-mode": "none",
        "write.update.mode": mode,
        "write.delete.mode": mode,
        "write.merge.mode": mode,
    }
    writer = df.repartitionByRange(files, "id").sortWithinPartitions("id").writeTo(f"bench.db.{table}")
    for key, value in props.items():
        writer = writer.tableProperty(key, value)
    writer.create()


def main():
    spark = SparkSession.builder.appName("merge-rows-cow-data").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bench.db")
    target = spark.range(ROWS).select(
        F.col("id"),
        (F.col("id") * 3).alias("value"),
        F.sha2(F.col("id").cast("string"), 256).alias("payload"),
    )
    create(target, "t_cow_4", "copy-on-write", 4)
    create(target, "t_cow_64", "copy-on-write", 64)

    # Partitioned by id % 16: a merge touching few partitions rewrites few files.
    part = target.withColumn("p", (F.col("id") % 16).cast("int"))
    (
        part.sortWithinPartitions("p", "id")
        .writeTo("bench.db.t_cow_part")
        .partitionedBy(F.col("p"))
        .tableProperty("format-version", "2")
        .tableProperty("write.distribution-mode", "none")
        .tableProperty("write.update.mode", "copy-on-write")
        .tableProperty("write.delete.mode", "copy-on-write")
        .tableProperty("write.merge.mode", "copy-on-write")
        .create()
    )

    # Sources are written in random order, so matches arrive shuffled.
    def source(df, name):
        df.orderBy(F.rand(11)).writeTo(f"bench.db.{name}").create()

    ids = spark.range(ROWS).select("id", (F.col("id") + 1).alias("value"))
    source(ids, "s_all")
    source(ids.where("id % 2 = 0"), "s_half")
    source(ids.where("id % 10 = 0"), "s_10pct")
    source(ids.where("id % 100 = 0"), "s_1pct")
    source(ids.where("id % 10000 = 0"), "s_001pct")
    ids.orderBy("id").writeTo("bench.db.s_all_sorted").create()
    # Partition-aligned sources for the partitioned target.
    with_p = ids.withColumn("p", (F.col("id") % 16).cast("int"))
    source(with_p, "sp_all")
    source(with_p.where("id % 100 = 0"), "sp_1pct")
    # Half of these ids match the target (alternately updated and deleted), half are new.
    mixed = spark.range(ROWS // 2, ROWS + ROWS // 2).select(
        "id",
        (F.col("id") + 1).alias("value"),
        F.when(F.col("id") % 2 == 0, F.lit("u")).otherwise(F.lit("d")).alias("op"),
    )
    source(mixed, "s_mixed")
    # Only ids absent from the target.
    source(
        spark.range(ROWS, ROWS + ROWS // 2).select("id", (F.col("id") + 1).alias("value")),
        "s_new",
    )
    # Every 100th id appears twice: a MERGE using it must fail the cardinality check.
    dup = ids.where("id % 100 = 0")
    source(dup.unionByName(dup), "s_dup")

    snapshots = {}
    for table in ["t_cow_4", "t_cow_64", "t_cow_part"]:
        snapshots[table] = spark.sql(
            f"SELECT snapshot_id FROM bench.db.{table}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).first()[0]
        files = spark.sql(f"SELECT count(*), sum(file_size_in_bytes) FROM bench.db.{table}.files").first()
        print(f"TABLE {table} data_files={files[0]} bytes={files[1]}")
    with open(os.environ["BENCH_SNAPSHOTS"], "w") as out:
        json.dump(snapshots, out)
    spark.stop()


if __name__ == "__main__":
    main()
