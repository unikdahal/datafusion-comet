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

"""Fresh-exchange query benchmarks; run only by the Actions benchmark workflow."""
import argparse
import hashlib
import json
import os
import re
import struct
import subprocess
import threading
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def queries(spark, root, partitions):
    left = spark.read.parquet(str(Path(root) / "left"))
    right = spark.read.parquet(str(Path(root) / "right"))

    def aggregate(data):
        return data.groupBy("k").agg(F.count("*").alias("n"), F.sum("v").alias("v"),
                                    F.min("payload").alias("lo"), F.max("payload").alias("hi"))

    # Explicit repartitions precede aggregation: full variable-width payloads must cross the
    # exchange, rather than just the few partial-aggregate rows of a scan/aggregate benchmark.
    yield "hash", aggregate(left.repartition(partitions, "k"))
    yield "range", aggregate(left.repartitionByRange(partitions, "k", "id"))
    joined = left.repartition(partitions, "k").join(right.repartition(partitions, "k"), "k")
    yield "join", joined.groupBy("k").agg(F.count("*").alias("n"), F.sum("v").alias("v"),
                                         F.sum("w").alias("w"), F.min("payload").alias("lo"),
                                         F.max("payload").alias("hi"))


def process_stats(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return int(fields[21]) * os.sysconf("SC_PAGE_SIZE"), (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK"), int(fields[17])


class MemorySampler:
    def __init__(self, driver, services, spark=None):
        self.driver = driver
        self.services = services
        self.peak = {"driver_rss_bytes": 0, "service_rss_bytes": 0, "combined_rss_bytes": 0, "driver_threads": 0}
        self.samples = 0
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.jvm = spark._jvm if spark is not None else None
        self.error = None
        if self.jvm is not None:
            self.native = self.jvm.org.apache.comet.Native()
            self.heap = self.jvm.java.lang.management.ManagementFactory.getMemoryMXBean()
            cls = self.jvm.java.lang.Class.forName("java.lang.management.BufferPoolMXBean")
            self.buffers = self.jvm.java.lang.management.ManagementFactory.getPlatformMXBeans(cls)
            self.peak.update({key: 0 for key in ["native_live_bytes", "native_pool_reserved_bytes", "jvm_heap_used_bytes", "jvm_heap_committed_bytes", "jvm_direct_buffer_bytes"]})

    def sample(self):
        stats = process_stats(self.driver)
        driver = stats[0]
        service = sum(process_stats(pid)[0] for pid in self.services)
        for key, value in [("driver_rss_bytes", driver), ("service_rss_bytes", service),
                           ("combined_rss_bytes", driver + service), ("driver_threads", stats[2])]:
            self.peak[key] = max(self.peak[key], value)
        self.samples += 1
        if self.jvm is not None:
            native = list(self.native.getMemoryUsage())
            assert len(native) == 4
            heap = self.heap.getHeapMemoryUsage()
            self.live = {"native_live_bytes": native[0], "native_pool_reserved_bytes": native[1],
                         "jvm_heap_used_bytes": heap.getUsed(), "jvm_heap_committed_bytes": heap.getCommitted(),
                         "jvm_direct_buffer_bytes": sum(max(0, bean.getMemoryUsed()) for bean in self.buffers if bean.getName() == "direct")}
            for key, value in self.live.items():
                self.peak[key] = max(self.peak[key], value)

    def run(self):
        try:
            while not self.stop.is_set():
                self.sample()
                self.stop.wait(0.1)
        except Exception as error:
            self.error = error

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop.set()
        self.thread.join()
        if self.error is not None:
            raise RuntimeError("Memory diagnostics failed") from self.error
        self.sample()


def gc_stats(spark):
    beans = spark._jvm.java.lang.management.ManagementFactory.getGarbageCollectorMXBeans()
    return {"gc_count": sum(max(0, bean.getCollectionCount()) for bean in beans),
            "gc_ms": sum(max(0, bean.getCollectionTime()) for bean in beans)}


def residency_snapshot(spark, pid, output, label):
    # This diagnostic is outside every measured query. The public jcmd heap summary gives
    # the reserved Java heap address range; smaps then measures its resident pages directly.
    jcmd = Path(os.environ["JAVA_HOME"]) / "bin/jcmd"
    info = subprocess.check_output([str(jcmd), str(pid), "GC.heap_info"], text=True)
    Path(output + f".{label}.heap-info.txt").write_text(info)
    region = re.search(r"garbage-first heap.*\[(0x[0-9a-f]+),\s*(0x[0-9a-f]+)\)", info)
    assert region, info
    heap_start, heap_end = [int(value, 16) for value in region.groups()]
    result = {"total_rss_bytes": 0, "java_heap_rss_bytes": 0, "outside_heap_anonymous_rss_bytes": 0,
              "other_rss_bytes": 0}
    category = None
    for line in Path(f"/proc/{pid}/smaps").read_text().splitlines():
        mapping = re.match(r"([0-9a-f]+)-([0-9a-f]+)\s+\S+\s+\S+\s+\S+\s+\S+(?:\s+(.*))?", line)
        if mapping:
            start, end = [int(value, 16) for value in mapping.groups()[:2]]
            name = mapping.group(3) or ""
            if start >= heap_start and end <= heap_end:
                category = "java_heap_rss_bytes"
            elif start < heap_end and end > heap_start:
                raise RuntimeError("A process mapping straddles the Java heap boundary")
            elif not name or name in ["[heap]", "[stack]"] or name.startswith("[anon:"):
                category = "outside_heap_anonymous_rss_bytes"
            else:
                category = "other_rss_bytes"
        elif line.startswith("Rss:"):
            value = int(line.split()[1]) * 1024
            assert category is not None
            result[category] += value
            result["total_rss_bytes"] += value
    assert result["java_heap_rss_bytes"] > 0
    native = list(spark._jvm.org.apache.comet.Native().getMemoryUsage())
    result["native_live_bytes"] = native[0]
    result["native_pool_reserved_bytes"] = native[1]
    heap = spark._jvm.java.lang.management.ManagementFactory.getMemoryMXBean().getHeapMemoryUsage()
    result["java_heap_used_bytes"] = heap.getUsed()
    result["java_heap_committed_bytes"] = heap.getCommitted()
    values = list(spark._jvm.org.apache.comet.benchmark.CelebornJvmAllocation.nativeAllocatorSnapshot())
    assert len(values) == 5
    for key, value in zip(["allocator_arena_bytes", "allocator_live_arena_bytes", "allocator_free_arena_bytes", "allocator_mmap_bytes", "allocator_top_free_bytes"], values):
        result[key] = value
    return result


def plan_metrics(plan):
    result = []
    def visit(node):
        metrics = {}
        iterator = node.metrics().iterator()
        while iterator.hasNext():
            pair = iterator.next()
            metrics[pair._1()] = pair._2().value()
        result.append({"node": node.nodeName(), "metrics": metrics})
        children = node.children().iterator()
        while children.hasNext():
            visit(children.next())
    visit(plan)
    return result


def storage_snapshot(application_id):
    files = []
    for file in Path("/tmp/celeborn-worker").rglob("*"):
        if application_id not in str(file):
            continue
        try:
            if file.is_file():
                size = file.stat().st_size
                if size > 0:
                    files.append({"path": str(file), "file_bytes": size})
        except FileNotFoundError:
            # Spark's ContextCleaner may unregister older exchanges during observation.
            continue
    return files


def stored_frames(files):
    samples = []
    for file in files:
        if file["file_bytes"] < 32:
            continue
        try:
            with Path(file["path"]).open("rb") as stream:
                head = stream.read(32)
        except FileNotFoundError:
            continue
        if len(head) != 32:
            continue
        map_id, attempt, batch_id, payload_bytes = struct.unpack("<4i", head[:16])
        frame_body_bytes, fields = struct.unpack("<2q", head[16:32])
        if payload_bytes >= 16 and frame_body_bytes == payload_bytes - 8 and 0 < fields < 1024:
            samples.append({**file, "payload_bytes": payload_bytes, "field_count": fields,
                            "map_id": map_id, "attempt": attempt, "batch_id": batch_id})
    return samples


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["prepare", "reference", "original", "heap", "direct"], required=True)
    parser.add_argument("--data", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--width", type=int, required=True)
    parser.add_argument("--entropy", choices=["repeated", "hex"], default="repeated")
    parser.add_argument("--skew", type=int, default=0)
    parser.add_argument("--partitions", type=int, required=True)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--service-pids", default="")
    parser.add_argument("--allocation", action="store_true")
    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        if args.mode == "prepare":
            key = F.when((F.col("id") % 100) < args.skew, F.lit(0)).otherwise(F.col("id") % 4096).cast("int")
            data = spark.range(args.rows, numPartitions=16).select("id", key.alias("k"),
                                                                       ((F.col("id") * 17) % 100003).alias("v"))
            if args.entropy == "hex":
                payload = F.concat(*[F.sha2(F.concat(F.col("id").cast("string"), F.lit(f"-{i}")), 256)
                                     for i in range(max(1, (args.width + 63) // 64))])
            else:
                payload = F.lpad(F.col("k").cast("string"), max(1, args.width), "x")
            data.select("id", "k", "v", F.substring(payload, 1, args.width).alias("payload")).write.mode("overwrite").parquet(str(Path(args.data) / "left"))
            spark.range(4096, numPartitions=4).selectExpr("cast(id as int) as k", "cast(id * 3 + 7 as bigint) as w").write.mode("overwrite").parquet(str(Path(args.data) / "right"))
            return
        unavailable = None
        if args.mode in ["original", "heap", "direct"]:
            cls = spark._jvm.java.lang.Class.forName("org.apache.celeborn.client.ShuffleClientImpl")
            unavailable = spark._jvm.org.apache.comet.shuffle.CelebornShufflePartitionPusher.nativePushCompletionUnavailableReason(cls)
            assert (unavailable is not None) == (args.mode == "original"), unavailable
            print("NATIVE_CAPABILITY_REASON=" + str(unavailable), flush=True)
        pid = int(spark._jvm.java.lang.management.ManagementFactory.getRuntimeMXBean().getName().split("@")[0])
        services = [int(value) for value in args.service_pids.split(",") if value]
        records = {}
        repeats = 1 if args.mode == "reference" else args.warmups + args.samples
        for repeat in range(repeats):
            # Rotate query order too; never cache a DataFrame or reuse its completed exchange.
            cases = list(queries(spark, args.data, args.partitions))
            cases = cases[repeat % len(cases):] + cases[:repeat % len(cases)]
            for name, query in cases:
                group = f"{name}-{repeat}"
                spark.sparkContext.setJobGroup(group, group)
                before_gc = gc_stats(spark)
                allocation_before = json.loads(spark._jvm.org.apache.comet.benchmark.CelebornJvmAllocation.snapshot()) if args.allocation else {}
                before_cpu = process_stats(pid)[1]
                before_service_cpu = sum(process_stats(service)[1] for service in services)
                with MemorySampler(pid, services, spark if args.allocation else None) as memory:
                    start = time.perf_counter()
                    rows = query.collect()
                    elapsed = time.perf_counter() - start
                after_cpu = process_stats(pid)[1]
                after_service_cpu = sum(process_stats(service)[1] for service in services)
                allocation_after = json.loads(spark._jvm.org.apache.comet.benchmark.CelebornJvmAllocation.snapshot()) if args.allocation else {}
                after_gc = gc_stats(spark)
                heap = spark._jvm.java.lang.management.ManagementFactory.getMemoryMXBean().getHeapMemoryUsage()
                stable = sorted((list(row) for row in rows), key=lambda row: json.dumps(row))
                digest = hashlib.sha256(json.dumps(stable, separators=(",", ":")).encode()).hexdigest()
                executed = query._jdf.queryExecution().executedPlan()
                plan = executed.toString()
                native = "CometExchange" in plan
                assert native == (args.mode in ["heap", "direct"]), (args.mode, name, plan)
                record = records.setdefault(name, {"sha256": digest, "rows": len(rows), "native": native, "samples": []})
                assert record["sha256"] == digest
                sample = {"job_group": group, "repeat": repeat, "warmup": repeat < args.warmups,
                          "seconds": elapsed, "cpu_seconds": after_cpu - before_cpu,
                          "service_cpu_seconds": after_service_cpu - before_service_cpu,
                          "jvm_heap_used_after_bytes": heap.getUsed(), "jvm_heap_committed_after_bytes": heap.getCommitted(),
                          **memory.peak, "rss_sample_count": memory.samples,
                          **{key: after_gc[key] - before_gc[key] for key in before_gc},
                          "sql_metrics": plan_metrics(executed)}
                if args.allocation:
                    before = allocation_before["threads"]
                    after = allocation_after["threads"]
                    started = allocation_after["total_started"] - allocation_before["total_started"]
                    newly_alive = len(after.keys() - before.keys())
                    sample["jvm_allocated_bytes"] = sum(value - before.get(thread, 0) for thread, value in after.items())
                    sample["allocation_threads_ended"] = len(before.keys() - after.keys()) + max(0, started - newly_alive)
                    sample["allocation_threads_started"] = started
                    sample["live_memory_after"] = memory.live
                record["samples"].append(sample)
                Path(args.output + "." + name + ".plan.txt").write_text(plan)
                # Checkpoint every query so failures retain the successful measurements too.
                Path(args.output).write_text(json.dumps({"config": vars(args), "application_id": spark.sparkContext.applicationId,
                                                        "native_capability_reason": unavailable, "queries": records}, indent=2))
                print("SAMPLE=" + json.dumps({"mode": args.mode, "query": name, **{k: v for k, v in sample.items() if k != "sql_metrics"}}), flush=True)
        if args.mode in ["original", "heap", "direct"]:
            files = storage_snapshot(spark.sparkContext.applicationId)
            assert files, "No data in this application's live Celeborn worker storage"
            if spark.sparkContext.getConf().get("spark.celeborn.client.push.replicate.enabled") == "true":
                assert any(Path(file["path"]).name.split(".")[0].endswith("-1") for file in files), "No stored replicas"
            Path(args.output + ".worker-storage.json").write_text(json.dumps({
                "files": len(files), "bytes": sum(file["file_bytes"] for file in files),
                "examples": [file["path"] for file in files[:20]]}, indent=2))
        if args.mode in ["heap", "direct"]:
            frames = stored_frames(files)
            assert frames, "No native frames in this application's live worker storage"
            if spark.sparkContext.getConf().get("spark.celeborn.client.push.replicate.enabled") == "true":
                assert any(Path(frame["path"]).name.split(".")[0].endswith("-1") for frame in frames), "No native replicas"
            Path(args.output + ".native-frames.json").write_text(json.dumps(frames, indent=2))
        if args.allocation:
            residency = {"before_gc": residency_snapshot(spark, pid, args.output, "before-gc")}
            # Full GC is diagnostic only, after all warmups and measurements in this fresh JVM.
            spark._jvm.java.lang.System.gc()
            residency["after_gc"] = residency_snapshot(spark, pid, args.output, "after-gc")
            residency["trim_released_pages"] = spark._jvm.org.apache.comet.benchmark.CelebornJvmAllocation.trimNativeAllocator()
            residency["after_trim"] = residency_snapshot(spark, pid, args.output, "after-trim")
            Path(args.output + ".residency.json").write_text(json.dumps(residency, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
