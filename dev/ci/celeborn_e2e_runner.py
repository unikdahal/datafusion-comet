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

"""Actions-only orchestration: separate JVMs and fresh services in balanced mode order."""
import argparse
import json
import os
import shutil
import socket
import subprocess
import time
from pathlib import Path


def run(command, log, env=None):
    print("RUN=" + json.dumps([str(item) for item in command]), flush=True)
    with Path(log).open("w") as output:
        result = subprocess.run([str(item) for item in command], stdout=output, stderr=subprocess.STDOUT, env=env)
    if result.returncode:
        print(Path(log).read_text()[-18000:], flush=True)
        raise RuntimeError(f"Command failed ({result.returncode}): {log}")


def stop_services(dist, env):
    for script, instance in [("stop-worker.sh", "1"), ("stop-worker.sh", "2"), ("stop-master.sh", "1")]:
        subprocess.run([str(dist / "sbin" / script)], env={**env, "WORKER_INSTANCE": instance}, check=False)


def start_services(dist, target):
    storage = Path("/tmp/celeborn-worker")
    if storage.exists():
        shutil.rmtree(storage)
    for name in ["one", "two"]:
        (storage / name).mkdir(parents=True)
    conf = target / "conf"
    conf.mkdir()
    text = """celeborn.master.host 127.0.0.1
celeborn.master.port 9097
celeborn.master.endpoints 127.0.0.1:9097
celeborn.metrics.enabled false
celeborn.worker.storage.dirs /tmp/celeborn-worker/one:disktype=SSD
celeborn.worker.monitor.disk.enabled false
"""
    (conf / "celeborn-defaults.conf").write_text(text)
    (conf / "worker-two.conf").write_text(text.replace("/one", "/two") + "celeborn.worker.http.port 9095\n")
    pids = target / "pids"
    pids.mkdir()
    logs = target / "service-logs"
    logs.mkdir()
    env = {**os.environ, "CELEBORN_HOME": str(dist), "CELEBORN_CONF_DIR": str(conf),
           "CELEBORN_PID_DIR": str(pids), "CELEBORN_LOG_DIR": str(logs),
           "CELEBORN_MASTER_MEMORY": "512m", "CELEBORN_WORKER_MEMORY": "512m",
           "CELEBORN_WORKER_OFFHEAP_MEMORY": "512m"}
    run([dist / "sbin/start-master.sh"], target / "master-start.txt", env)
    run([dist / "sbin/start-worker.sh"], target / "worker-one-start.txt", {**env, "WORKER_INSTANCE": "1"})
    run([dist / "sbin/start-worker.sh", "--properties-file", conf / "worker-two.conf"],
        target / "worker-two-start.txt", {**env, "WORKER_INSTANCE": "2"})
    for attempt in range(60):
        try:
            with socket.create_connection(("127.0.0.1", 9097), timeout=1):
                break
        except OSError:
            time.sleep(1)
    else:
        raise RuntimeError("Celeborn master never became ready")
    time.sleep(5)
    ids = [int(file.read_text()) for file in pids.glob("*.pid")]
    assert len(ids) == 3, ids
    return env, ids


def event_metrics(directory):
    events = []
    for file in directory.rglob("*"):
        if file.is_file() and not file.name.startswith("."):
            events.extend(json.loads(line) for line in file.read_text().splitlines() if line.startswith("{"))
    groups = {}
    for event in events:
        if event["Event"] == "SparkListenerJobStart":
            group = (event.get("Properties") or {}).get("spark.jobGroup.id")
            if group:
                groups.setdefault(group, set()).update(event["Stage IDs"])
    result = {}
    for group, stages in groups.items():
        tasks = [e for e in events if e["Event"] == "SparkListenerTaskEnd" and e["Stage ID"] in stages]
        assert tasks and all(e["Task End Reason"]["Reason"] == "Success" for e in tasks), group
        totals = {"tasks": len(tasks)}
        for key in ["Executor Run Time", "Executor CPU Time", "JVM GC Time", "Memory Bytes Spilled", "Disk Bytes Spilled"]:
            totals[key] = sum(e.get("Task Metrics", {}).get(key, 0) for e in tasks)
        totals["peak_task_execution_bytes"] = max(e.get("Task Metrics", {}).get("Peak Execution Memory", 0) for e in tasks)
        for nested, keys in [("Shuffle Write Metrics", ["Shuffle Bytes Written", "Shuffle Records Written", "Shuffle Write Time"]),
                             ("Shuffle Read Metrics", ["Remote Bytes Read", "Local Bytes Read", "Fetch Wait Time", "Total Records Read"]),
                             ("Input Metrics", ["Bytes Read", "Records Read"])]:
            for key in keys:
                totals[key] = sum(e.get("Task Metrics", {}).get(nested, {}).get(key, 0) for e in tasks)
        result[group] = totals
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--forks", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=3)
    parser.add_argument("--samples", type=int, default=5)
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    source = Path(__file__).resolve().parent
    scenario = json.loads((source / "celeborn_e2e_scenarios.json").read_text())[args.scenario]
    (args.output / "scenario.json").write_text(json.dumps(scenario, indent=2))
    (args.output / "protocol.json").write_text(json.dumps({"forks": args.forks, "warmups": args.warmups,
                                                       "samples": args.samples, "memory_sample_interval_seconds": 0.1}, indent=2))
    data = Path("/tmp/e2e-data")
    submit = Path(os.environ["SPARK_HOME"]) / "bin/spark-submit"
    driver = source / "celeborn_e2e_benchmark.py"
    data_args = ["--data", data, "--rows", scenario["rows"], "--width", scenario["width"],
                 "--entropy", scenario["entropy"], "--skew", scenario["skew"], "--partitions", scenario["partitions"]]
    common = [submit, "--master", f"local[{scenario['cores']}]", "--driver-memory", "3g",
              "--conf", "spark.sql.adaptive.enabled=false", "--conf", "spark.sql.autoBroadcastJoinThreshold=-1",
              "--conf", "spark.sql.exchange.reuse=false", "--conf", "spark.sql.shuffle.partitions=" + str(scenario["partitions"])]
    run([*common, driver, "--mode", "prepare", *data_args, "--output", args.output / "unused"], args.output / "prepare.log")
    run([*common, driver, "--mode", "reference", *data_args, "--output", args.output / "reference.json"], args.output / "reference.log")
    expected = json.loads((args.output / "reference.json").read_text())["queries"]
    modes = ["original", "heap", "direct"]
    # A cyclic Latin square: each mode appears first, second and last exactly once.
    for fork in range(args.forks):
        for mode in modes[fork % len(modes):] + modes[:fork % len(modes)]:
            target = args.output / f"fork-{fork}-{mode}"
            target.mkdir()
            version = "original" if mode == "original" else "current"
            dist = (args.artifacts / ("celeborn-" + version) / "dist").resolve()
            comet = next((args.artifacts / ("comet-" + version)).glob("*.jar")).resolve()
            client = next((dist / "spark").glob("celeborn-client-spark-3-shaded_2.12-*.jar"))
            extra = [str(Path(os.environ["ALLOCATION_JAR"]).resolve())] if scenario.get("allocation") else []
            jars = [str(comet), str(client), *extra]
            shutil.copy(args.artifacts / ("comet-" + version) / "manifest.json", target / "comet-manifest.json")
            shutil.copy(args.artifacts / ("celeborn-" + version) / "manifest.json", target / "celeborn-manifest.json")
            env, pids = start_services(dist, target)
            event_dir = target / "events"
            event_dir.mkdir()
            conf = {
                "spark.plugins": "org.apache.spark.CometPlugin",
                "spark.shuffle.manager": "org.apache.spark.sql.comet.execution.shuffle.CometCelebornShuffleManager",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.celeborn.master.endpoints": "127.0.0.1:9097",
                "spark.celeborn.client.spark.stageRerun.enabled": "true",
                "spark.celeborn.client.spark.shuffle.fallback.policy": "NEVER",
                "spark.celeborn.client.shuffle.integrityCheck.enabled": "true",
                "spark.celeborn.client.push.replicate.enabled": str(scenario["replicate"]).lower(),
                "spark.celeborn.client.shuffle.compression.codec": scenario["codec"],
                "spark.shuffle.service.enabled": "false",
                "spark.shuffle.compress": str(scenario["codec"] != "none").lower(),
                "spark.comet.exec.enabled": "true", "spark.comet.shuffle.enabled": "true",
                "spark.comet.shuffle.mode": "native", "spark.comet.batchSize": str(scenario["batch"]),
                "spark.comet.shuffle.compression.codec": "lz4" if scenario["codec"] == "none" else scenario["codec"],
                "spark.comet.shuffle.celeborn.directBuffer.enabled": str(mode == "direct").lower(),
                "spark.comet.shuffle.rss.maxFrameBytes": scenario["frame"],
                "spark.comet.shuffle.rss.maxInFlightBytes": scenario["admission"],
                "spark.memory.offHeap.enabled": "true", "spark.memory.offHeap.size": "1g",
                "spark.eventLog.enabled": "true", "spark.eventLog.compress": "false",
                "spark.eventLog.dir": "file:" + str(event_dir.resolve()),
                "spark.executor.extraClassPath": ":".join(jars),
            }
            (target / "spark-conf.json").write_text(json.dumps(conf, indent=2))
            conf_args = [item for key, value in conf.items() for item in ["--conf", key + "=" + value]]
            try:
                run(["/usr/bin/time", "-v", "-o", target / "process-resources.txt", *common,
                     "--jars", ",".join(jars), "--driver-class-path", ":".join(jars),
                     *conf_args, driver, "--mode", mode, *data_args, "--warmups", args.warmups,
                     "--samples", args.samples, "--service-pids", ",".join(map(str, pids)),
                     "--output", target / "queries.json", *(["--allocation"] if scenario.get("allocation") else [])], target / "spark.log", env)
                record = json.loads((target / "queries.json").read_text())
                if mode != "original":
                    frames = json.loads((target / "queries.json.native-frames.json").read_text())
                    frame_limit = int(scenario["frame"][:-1]) * {"k": 1024, "m": 1048576}[scenario["frame"][-1]]
                    assert all(frame["payload_bytes"] <= frame_limit for frame in frames)
                    if args.scenario == "large-frames":
                        assert max(frame["payload_bytes"] for frame in frames) >= 1048576, "Large-frame case must actually push MiB-size frames"
                metrics = event_metrics(event_dir)
                for name, query in record["queries"].items():
                    assert query["sha256"] == expected[name]["sha256"] and query["rows"] == expected[name]["rows"], (mode, name)
                    for sample in query["samples"]:
                        sample["task_metrics"] = metrics[sample["job_group"]]
                        assert sample["task_metrics"]["Shuffle Bytes Written"] > 0, (mode, name)
                (target / "queries.json").write_text(json.dumps(record, indent=2))
                # Event logs can be hundreds of MB; retain gzip, with original metrics intact.
                run(["tar", "-czf", target / "events.tar.gz", "-C", event_dir, "."], target / "event-compress.log")
                shutil.rmtree(event_dir)
                print(f"PARITY_OK scenario={args.scenario} fork={fork} mode={mode}", flush=True)
            finally:
                stop_services(dist, env)
    print("SCENARIO_COMPLETE=" + args.scenario, flush=True)


if __name__ == "__main__":
    main()
