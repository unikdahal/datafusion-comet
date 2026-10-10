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

"""Generate the Iceberg tables for the runtime pruning benchmark.

Run once with plain Spark and the Iceberg runtime (no Comet), so every
variant reads byte-identical files.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROWS = int(os.environ.get("BENCH_ROWS", "16000000"))
FILES = int(os.environ.get("BENCH_FILES", "16"))
ROW_GROUP_BYTES = os.environ.get("BENCH_ROW_GROUP_BYTES", str(8 * 1024 * 1024))
SPLIT_BYTES = os.environ.get("BENCH_SPLIT_BYTES", str(128 * 1024 * 1024))
PROFILE = os.environ.get("BENCH_PROFILE", "smoke")
DIM_START = ROWS * 3 // 5

PROPERTIES = {
    "format-version": "2",
    "write.parquet.row-group-size-bytes": ROW_GROUP_BYTES,
    "write.target-file-size-bytes": str(1 << 30),
    "write.distribution-mode": "none",
    "write.delete.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.merge.mode": "merge-on-read",
    "read.split.target-size": SPLIT_BYTES,
}


def write(df, table):
    writer = df.writeTo(f"bench.db.{table}")
    for key, value in PROPERTIES.items():
        writer = writer.tableProperty(key, value)
    writer.create()


def main():
    spark = SparkSession.builder.appName("iceberg-runtime-pruning-data").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bench.db")
    base = spark.range(ROWS).select(
        F.col("id").cast("int").alias("id"),
        (F.col("id") * 3).alias("value"),
        F.sha2(F.col("id").cast("string"), 256).alias("payload"),
    )
    # Range-partitioned and sorted: each file and row group covers a key range.
    sorted_df = base.repartitionByRange(FILES, "id").sortWithinPartitions("id")
    write(sorted_df, "fact_sorted")
    write(sorted_df, "fact_pos_deletes")
    # Merge-on-read DELETE writes position delete files against every data file.
    spark.sql("DELETE FROM bench.db.fact_pos_deletes WHERE id % 101 = 0")
    # Randomly ordered: statistics cannot prune, so this measures overhead.
    write(base.repartition(FILES).sortWithinPartitions(F.rand(7)), "fact_unsorted")
    write(
        spark.range(DIM_START, DIM_START + 256, 2).select(F.col("id").cast("int").alias("id")),
        "dim",
    )
    tables = ["fact_sorted", "fact_pos_deletes", "fact_unsorted", "dim"]
    if PROFILE == "extended":
        # A full-domain dimension exercises non-selective joins without a large broadcast.
        write(spark.range(ROWS).select(F.col("id").cast("int").alias("id")).repartition(FILES), "dim_all")
        # Old INT physical files coexist with newly written LONG files after promotion.
        write(base.filter(F.col("id") < ROWS // 2).repartitionByRange(FILES, "id").sortWithinPartitions("id"), "fact_evolved")
        spark.sql("ALTER TABLE bench.db.fact_evolved ALTER COLUMN id TYPE BIGINT")
        base.filter(F.col("id") >= ROWS // 2).withColumn("id", F.col("id").cast("long")).repartitionByRange(FILES, "id").sortWithinPartitions("id").writeTo("bench.db.fact_evolved").append()
        tables += ["dim_all", "fact_evolved"]
    for table in tables:
        files = spark.sql(f"SELECT count(*), sum(file_size_in_bytes) FROM bench.db.{table}.files").first()
        deletes = spark.sql(f"SELECT count(*) FROM bench.db.{table}.delete_files").first()[0]
        print(f"TABLE {table} data_files={files[0]} bytes={files[1]} delete_files={deletes}")
    spark.stop()


if __name__ == "__main__":
    main()
