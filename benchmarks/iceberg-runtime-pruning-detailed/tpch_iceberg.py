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

"""Write the TPC-H Parquet files as two Iceberg layouts, with plain Spark and no Comet.

`tpch_nat` keeps the generator's order. `tpch_clu` clusters the two big tables on their date
columns (lineitem by l_shipdate, orders by o_orderdate), the way a tuned table would be laid out.
"""

import os

from pyspark.sql import SparkSession

SOURCE = os.environ["TPCH_PARQUET"]
TABLES = [
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
]
CLUSTER = {"lineitem": "l_shipdate", "orders": "o_orderdate"}
PROPS = {
    "format-version": "2",
    "write.parquet.row-group-size-bytes": str(8 * 1024 * 1024),
    "write.target-file-size-bytes": str(256 * 1024 * 1024),
    "write.distribution-mode": "none",
}


def put(df, namespace, table):
    writer = df.writeTo(f"bench.{namespace}.{table}")
    for key, value in PROPS.items():
        writer = writer.tableProperty(key, value)
    writer.create()


def main():
    spark = SparkSession.builder.appName("tpch-iceberg-data").getOrCreate()
    for namespace in ["tpch_nat", "tpch_clu"]:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS bench.{namespace}")
        for table in TABLES:
            df = spark.read.parquet(os.path.join(SOURCE, f"{table}.parquet"))
            if namespace == "tpch_clu" and table in CLUSTER:
                df = df.repartitionByRange(16, CLUSTER[table]).sortWithinPartitions(
                    CLUSTER[table]
                )
            put(df, namespace, table)
            stats = spark.sql(
                f"SELECT count(*), sum(file_size_in_bytes) FROM bench.{namespace}.{table}.files WHERE content = 0"
            ).first()
            print(f"TABLE {namespace}.{table} data_files={stats[0]} bytes={stats[1]}")
    spark.stop()


if __name__ == "__main__":
    main()
