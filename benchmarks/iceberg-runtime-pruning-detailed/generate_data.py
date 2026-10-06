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

"""Generate the Iceberg tables for one suite of the detailed runtime pruning benchmark.

Run once with plain Spark and the Iceberg runtime (no Comet), so every variant reads
byte-identical files. The key column of every fact table is named `id`; its type depends
on the layout (see lib.typed). Every fact table also has `value` BIGINT, `payload` STRING
and `k2` INT.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lib import env, typed

SUITE = env("BENCH_SUITE", "join")
ROWS = int(env("BENCH_ROWS", "16000000"))
FILES = int(env("BENCH_FILES", "16"))
FUZZ_ROWS = int(env("BENCH_FUZZ_ROWS", "4000000"))
ROW_GROUP_BYTES = str(8 * 1024 * 1024)

PROPERTIES = {
    "format-version": "2",
    "write.parquet.row-group-size-bytes": ROW_GROUP_BYTES,
    "write.target-file-size-bytes": str(1 << 30),
    "write.distribution-mode": "none",
    "write.delete.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.merge.mode": "merge-on-read",
}


def write(df, table, props=None, partition=None):
    writer = df.writeTo(f"bench.db.{table}")
    for key, value in {**PROPERTIES, **(props or {})}.items():
        writer = writer.tableProperty(key, value)
    if partition is not None:
        writer = writer.partitionedBy(partition)
    writer.create()


def base(spark, rows, ktype="int"):
    """id, value, payload, k2 with the key mapped onto `ktype`."""
    df = spark.range(rows)
    key = typed(ktype, "id")
    if ktype == "nulls":
        key = f"CASE WHEN id % 10 = 0 THEN NULL ELSE {key} END"
    if ktype == "nan":
        key = (
            "CASE WHEN id % 97 = 0 THEN double('NaN') WHEN id % 89 = 0 THEN NULL "
            "WHEN id % 83 = 0 THEN -0.0D ELSE cast(id as double) END"
        )
    return df.select(
        F.expr(key).alias("id"),
        (F.col("id") * 3).alias("value"),
        F.sha2(F.col("id").cast("string"), 256).alias("payload"),
        (F.col("id") % 1000).cast("int").alias("k2"),
        F.col("id").alias("raw"),
    )


def cols(df):
    return df.drop("raw")


def sorted_files(df, files=FILES):
    return df.repartitionByRange(files, "raw").sortWithinPartitions("raw")


def build(spark, kind, table, rows):
    if kind in ("sorted", "long", "str", "date", "dec", "nulls", "nan"):
        ktype = "int" if kind == "sorted" else kind
        write(cols(sorted_files(base(spark, rows, ktype))), table)
    elif kind == "unsorted":
        df = base(spark, rows).repartition(FILES).sortWithinPartitions(F.rand(7))
        write(cols(df), table)
    elif kind == "pos_deletes":
        write(cols(sorted_files(base(spark, rows))), table)
        spark.sql(f"DELETE FROM bench.db.{table} WHERE id % 101 = 0")
    elif kind == "updated":
        # Updates rewrite 1% of rows into new files whose key bounds span the whole range.
        write(cols(sorted_files(base(spark, rows))), table)
        spark.sql(f"UPDATE bench.db.{table} SET value = value + 1 WHERE id % 100 = 0")
        spark.sql(f"DELETE FROM bench.db.{table} WHERE id % 101 = 0")
    elif kind == "reversed_files":
        # Ascending inside each file but the files are numbered from the highest key range down.
        df = base(spark, rows).withColumn("part", (F.col("raw") * FILES / rows).cast("int"))
        df = df.withColumn("part", F.lit(FILES - 1) - F.col("part"))
        df = df.repartitionByRange(FILES, "part").sortWithinPartitions("raw").drop("part")
        write(cols(df), table)
    elif kind == "overlap":
        # Every file holds the whole key range: sorted inside the file, no file-level pruning.
        df = base(spark, rows).repartition(FILES).sortWithinPartitions("raw")
        write(cols(df), table)
    elif kind == "small_rg":
        write(cols(sorted_files(base(spark, rows))), table, {"write.parquet.row-group-size-bytes": str(1 << 20)})
    elif kind == "many_files":
        write(cols(sorted_files(base(spark, rows), 256)), table)
    elif kind == "partitioned":
        df = base(spark, rows).withColumn("part", (F.col("raw") / (rows // 16)).cast("int"))
        df = df.repartition(16, "part").sortWithinPartitions("raw")
        write(cols(df), table, partition="part")
    elif kind == "wide":
        df = base(spark, rows)
        for index in range(5):
            df = df.withColumn(f"w{index}", F.sha2(F.concat(F.col("payload"), F.lit(str(index))), 512))
        write(cols(sorted_files(df)), table)
    elif kind == "skewed":
        # 90% of the rows sit in the lowest 1% of the key range.
        hot = rows // 100
        key = F.when(F.col("raw") % 10 != 0, (F.col("raw") * 7919) % hot).otherwise(F.col("raw"))
        df = base(spark, rows).withColumn("id", key.cast("int"))
        write(cols(sorted_files(df.withColumn("raw", F.col("id")))), table)
    elif kind == "snapshots":
        # Eight appends in key order with a delete between each: many manifests and snapshots.
        step = rows // 8
        for index in range(8):
            part = base(spark, rows).where(f"raw >= {index * step} AND raw < {(index + 1) * step}")
            part = cols(sorted_files(part, 2))
            if index == 0:
                write(part, table)
            else:
                part.writeTo(f"bench.db.{table}").append()
            spark.sql(f"DELETE FROM bench.db.{table} WHERE id % 101 = {index} AND id < {(index + 1) * step}")
    elif kind == "evolved":
        # Type promotion, a rename and an added column across the table's history.
        half = rows // 2
        first = cols(sorted_files(base(spark, rows).where(f"raw < {half}"), 4)).withColumnRenamed("value", "val")
        first = first.withColumn("val", F.col("val").cast("int"))
        write(first, table)
        spark.sql(f"ALTER TABLE bench.db.{table} ALTER COLUMN val TYPE bigint")
        spark.sql(f"ALTER TABLE bench.db.{table} RENAME COLUMN val TO value")
        spark.sql(f"ALTER TABLE bench.db.{table} ADD COLUMN extra STRING")
        second = cols(sorted_files(base(spark, rows).where(f"raw >= {half}"), 4))
        second = second.withColumn("extra", F.lit("x"))
        second.select("id", "value", "payload", "k2", "extra").writeTo(f"bench.db.{table}").append()
    elif kind == "part_evolved":
        half = rows // 2
        first = base(spark, rows).where(f"raw < {half}").withColumn("part", (F.col("raw") / (rows // 8)).cast("int"))
        first = first.repartition(4, "part").sortWithinPartitions("raw")
        write(cols(first), table, partition="part")
        spark.sql(f"ALTER TABLE bench.db.{table} ADD PARTITION FIELD bucket(4, id)")
        second = base(spark, rows).where(f"raw >= {half}").withColumn("part", (F.col("raw") / (rows // 8)).cast("int"))
        cols(second.repartition(4).sortWithinPartitions("raw")).writeTo(f"bench.db.{table}").append()
    else:
        raise ValueError(kind)


def dims(spark, rows):
    """Build-side tables, all integer keys; typed variants are derived in SQL."""
    start = rows * 3 // 5

    def put(name, df):
        write(df.select(F.col("id").cast("int").alias("id")), name, {"write.target-file-size-bytes": str(1 << 28)})

    put("dim_empty", spark.range(0))
    put("dim_1", spark.range(start, start + 1))
    put("dim_128", spark.range(start, start + 256, 2))
    put("dim_128_spread", spark.range(0, rows, max(rows // 128, 1)))
    put("dim_128_low", spark.range(0, 256, 2))
    put("dim_128_high", spark.range(rows - 256, rows, 2))
    put("dim_two_ranges", spark.range(rows // 10, rows // 10 + 128, 2).union(spark.range(rows * 9 // 10, rows * 9 // 10 + 128, 2)))
    put("dim_10k", spark.range(start, start + 10_000))
    put("dim_10k_spread", spark.range(0, rows, max(rows // 10_000, 1)))
    put("dim_1pct", spark.range(start, start + rows // 100))
    put("dim_10pct", spark.range(rows * 4 // 10, rows * 4 // 10 + rows // 10))
    put("dim_50pct", spark.range(rows // 4, rows // 4 + rows // 2))
    put("dim_all", spark.range(0, rows))
    # Second build side for star joins: the five values of k2 that exist in every file.
    spark.range(0, 1000, 200).select(F.col("id").cast("int").alias("k2")).writeTo("bench.db.dim_k2").create()


PLAN = {
    "join": ("f_", ["sorted", "unsorted", "pos_deletes", "long", "str", "date", "dec", "nulls", "nan"], ROWS, True),
    "topk_minmax": (
        "f_",
        ["sorted", "unsorted", "pos_deletes", "long", "str", "date", "nulls", "nan", "reversed_files", "overlap"],
        ROWS,
        True,
    ),
    "layouts": (
        "f_",
        ["sorted", "updated", "snapshots", "evolved", "part_evolved", "small_rg", "many_files", "partitioned", "wide", "skewed"],
        ROWS,
        True,
    ),
    "fuzz": (
        "fz_",
        [
            "sorted", "unsorted", "pos_deletes", "updated", "long", "str", "date", "dec", "nulls", "nan",
            "snapshots", "evolved", "part_evolved", "small_rg", "overlap", "reversed_files", "skewed",
        ],
        FUZZ_ROWS,
        False,
    ),
}


def main():
    spark = SparkSession.builder.appName(f"detailed-pruning-data-{SUITE}").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bench.db")
    prefix, kinds, rows, with_dims = PLAN[SUITE]
    for kind in kinds:
        build(spark, kind, f"{prefix}{kind}", rows)
    if SUITE == "layouts":
        # Scale: four times the rows, sorted.
        build(spark, "sorted", "f_big", ROWS * 4)
    if with_dims:
        dims(spark, ROWS)
    names = [r.tableName for r in spark.sql("SHOW TABLES IN bench.db").collect()]
    for table in names:
        files = spark.sql(f"SELECT count(*), sum(file_size_in_bytes) FROM bench.db.{table}.files").first()
        deletes = spark.sql(f"SELECT count(*) FROM bench.db.{table}.delete_files").first()[0]
        rows_in = spark.sql(f"SELECT count(*) FROM bench.db.{table}").first()[0]
        print(f"TABLE {table} rows={rows_in} data_files={files[0]} bytes={files[1]} delete_files={deletes}")
    spark.stop()


if __name__ == "__main__":
    main()
