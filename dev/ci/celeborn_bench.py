#!/usr/bin/env python3
"""Runs TPC-H and a shuffle-heavy query against a real Celeborn cluster and records metrics.

One invocation is one Spark application (one JVM), so peak RSS (VmHWM) is per mode and pass.
"""
import argparse
import glob
import json
import os
import re
import time
import urllib.request

from pyspark.sql import SparkSession

TPCH_TABLES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]


def read_status(pid):
    values = {}
    with open(f"/proc/{pid}/status") as status:
        for line in status:
            key, _, rest = line.partition(":")
            if key in ("VmHWM", "VmRSS"):
                values[key] = int(rest.split()[0]) * 1024
    return values


def rest(spark, path):
    url = spark.sparkContext.uiWebUrl + "/api/v1/applications/" + spark.sparkContext.applicationId + path
    with urllib.request.urlopen(url) as response:
        return json.load(response)


def stage_totals(spark):
    totals = {"shuffleWriteBytes": 0, "shuffleWriteTime": 0, "executorCpuTime": 0,
              "executorRunTime": 0, "jvmGcTime": 0, "shuffleReadBytes": 0}
    for stage in rest(spark, "/stages?status=complete"):
        for key in totals:
            totals[key] += stage.get(key, 0) or 0
    return totals


def gc_totals(spark):
    jvm = spark._jvm
    count = 0
    millis = 0
    for bean in jvm.java.lang.management.ManagementFactory.getGarbageCollectorMXBeans():
        count += max(bean.getCollectionCount(), 0)
        millis += max(bean.getCollectionTime(), 0)
    threads = jvm.java.lang.management.ManagementFactory.getThreadMXBean()
    ids = threads.getAllThreadIds()
    allocated = sum(max(v, 0) for v in threads.getThreadAllocatedBytes(ids))
    return {"gcCount": count, "gcMillis": millis, "liveThreadAllocatedBytes": allocated}


def normalize(rows):
    def value(v):
        if isinstance(v, float):
            return round(v, 4)
        if hasattr(v, "is_finite"):  # Decimal
            return str(round(v, 4))
        return v if v is None or isinstance(v, (int, str, bool)) else str(v)
    return sorted(tuple(value(v) for v in row) for row in rows)


def statements(path):
    text = open(path).read()
    text = "\n".join(line for line in text.splitlines() if not line.strip().startswith("--"))
    text = re.sub(r"(?i)create\s+view", "create temp view", text)
    return [s.strip() for s in text.split(";") if s.strip()]


# Shuffle-bound queries over Parquet, so that the exchanges are native Comet exchanges.
SHUFFLE_QUERIES = [
    ("shuffle_repartition", """
select count(*) as n, sum(v) as total, max(s) as smax, min(t) as tmin
from (select * from shuffle_input distribute by k) shuffled
"""),
    ("shuffle_aggregate", """
select k % 997 as bucket, count(*) as n, sum(v) as total, max(s) as smax, min(t) as tmin
from (select * from shuffle_input distribute by k) shuffled
group by k % 997
"""),
]


def prepare_shuffle_input(spark, path, rows):
    if os.path.exists(path):
        return
    spark.range(0, rows, 1, 16).selectExpr(
        "id", "(id * 7919) % 4000037 as k", "cast(id % 1000 as double) * 1.5 as v",
        "concat('s-', cast(id * 31 % 100003 as string), '-xxxxxxxxxxxxxxxx') as s",
        "concat('t-', cast(id % 7 as string), '-yyyyyyyyyyyyyyyyyyyyyyyy') as t",
    ).write.parquet(path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--queries", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--expect-native", choices=["yes", "no", "any"], default="any")
    parser.add_argument("--shuffle-rows", type=int, default=60_000_000)
    parser.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"celeborn-bench-{args.mode}").getOrCreate()
    pid = spark._jvm.ProcessHandle.current().pid()
    for table in TPCH_TABLES:
        paths = glob.glob(os.path.join(args.data, "tpch", table + "*"))
        spark.read.parquet(*paths).createOrReplaceTempView(table)
    shuffle_path = os.path.join(args.data, "shuffle_input")
    prepare_shuffle_input(spark, shuffle_path, args.shuffle_rows)
    if args.prepare_only:
        spark.stop()
        return
    spark.read.parquet(shuffle_path).createOrReplaceTempView("shuffle_input")

    queries = [(name, [sql]) for name, sql in SHUFFLE_QUERIES]
    for number in range(1, 23):
        queries.append((f"q{number}", statements(os.path.join(args.queries, f"q{number}.sql"))))

    results = {"mode": args.mode, "queries": {}, "pid": pid}
    for name, sqls in queries:
        before = stage_totals(spark)
        gc_before = gc_totals(spark)
        start = time.perf_counter()
        rows = None
        native_exchanges = 0
        exchanges = 0
        for sql in sqls:
            df = spark.sql(sql)
            if sql.lower().lstrip().startswith(("create", "drop")):
                continue
            rows = df.collect()
            plan = df._jdf.queryExecution().executedPlan().toString()
            native_exchanges += plan.count("CometExchange")
            exchanges += len(re.findall(r"(?<![A-Za-z])Exchange", plan))
        elapsed = time.perf_counter() - start
        after = stage_totals(spark)
        gc_after = gc_totals(spark)
        metrics = {key: after[key] - before[key] for key in after}
        metrics.update({key: gc_after[key] - gc_before[key] for key in gc_after})
        metrics["seconds"] = elapsed
        metrics["nativeExchanges"] = native_exchanges
        metrics["exchanges"] = exchanges
        results["queries"][name] = {"metrics": metrics, "rows": normalize(rows)}
        print(f"BENCH {args.mode} {name} {elapsed:.3f}s native_exchanges={native_exchanges} "
              f"shuffle_write={metrics['shuffleWriteBytes']}", flush=True)

    status = read_status(pid)
    results["peakRssBytes"] = status["VmHWM"]
    results["finalRssBytes"] = status["VmRSS"]
    total_native = sum(q["metrics"]["nativeExchanges"] for q in results["queries"].values())
    if args.expect_native == "yes" and total_native == 0:
        raise SystemExit("expected native Comet exchanges but found none")
    if args.expect_native == "no" and total_native != 0:
        raise SystemExit("expected Spark exchanges only but found native Comet exchanges")
    with open(args.output, "w") as out:
        json.dump(results, out, default=str)
    spark.stop()


if __name__ == "__main__":
    main()
