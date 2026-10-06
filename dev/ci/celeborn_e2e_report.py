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

"""Summarize paired fresh-JVM end-to-end results without assuming a speedup."""
import argparse
import json
import math
import random
import statistics
from pathlib import Path


def median(values):
    return statistics.median(values)


def resources(path):
    values = {}
    for line in path.read_text().splitlines():
        if "Maximum resident set size (kbytes):" in line:
            values["max_rss_bytes"] = int(line.rsplit(":", 1)[1]) * 1024
        elif "User time (seconds):" in line:
            values["user_seconds"] = float(line.rsplit(":", 1)[1])
        elif "System time (seconds):" in line:
            values["system_seconds"] = float(line.rsplit(":", 1)[1])
    assert len(values) == 3, path
    return values


def comparison(reference, candidate):
    pairs = [c / r for r, c in zip(reference, candidate)]
    rng = random.Random(8127)
    bootstrap = sorted(math.exp(sum(math.log(rng.choice(pairs)) for _ in pairs) / len(pairs)) for _ in range(10000))
    return {"ratio": math.exp(sum(map(math.log, pairs)) / len(pairs)),
            "fork_ratios": pairs, "ratio_min": min(pairs), "ratio_max": max(pairs),
            "exploratory_bootstrap_95_percent_ratio": [bootstrap[249], bootstrap[9749]],
            "direction": "faster_in_every_fork" if max(pairs) < 1 else "slower_in_every_fork" if min(pairs) > 1 else "mixed"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    result = {}
    lines = ["# End-to-end speed and memory", "",
             "All measurements ran in GitHub Actions on Ubuntu 24.04 with Java 17 and Spark 3.5.9. Each scenario uses one isolated runner, one master, and two workers on that machine. Modes run sequentially; no Spark benchmark shares its runner with another benchmark.", "",
             "**Original** runs the exact original Comet and Celeborn fork-main commits. Its capability check selects delegated row shuffle. **Heap** and **Direct** run the new native shuffle implementation; heap is asynchronous with one JNI copy, direct is a synchronous native-buffer borrow. Original/new comparisons therefore include the shuffle implementation and format change. Heap/direct comparisons isolate the buffer-mode tradeoff.", "",
             "Every query rebuilds its exchanges; exchange reuse and AQE are disabled. The mode order rotates between independent JVM forks. Warmups are excluded. Result digests match vanilla Spark. Native plans and current-app worker frame samples prove the intended paths; manifests verify source and binary hashes.", "",
             "RSS is resident process memory, not the framing allocation. Driver RSS includes native memory; combined RSS sums the Spark JVM, master and both workers (shared mappings may be counted twice). The per-query peak is sampled every 100 ms. A separate process-lifetime high-water RSS is obtained with /usr/bin/time. Worker storage is cleared between fresh service/JVM runs.", "",
             "Three independent JVM forks provide limited information about runner variability. Paired fork ranges and exploratory bootstrap bounds are retained in JSON; these results are diagnostic measurements on one host, not a multi-machine deployment or a production guarantee.", "",
             "## Query speed", "", "Lower seconds is faster. Percent change is measured against the original; negative means faster.", "",
             "| Scenario / query | Original s | Heap s | Direct s | Heap change | Direct change | Direct vs heap |",
             "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    memory_lines = ["", "## Query memory", "", "Median of the measured query's peak Spark JVM RSS, in MiB. Lower is less resident memory.", "",
                    "| Scenario / query | Original MiB | Heap MiB | Direct MiB | Heap change | Direct change |",
                    "| --- | ---: | ---: | ---: | ---: | ---: |"]
    diagnostic_lines = ["", "## Shuffle and GC diagnostics", "",
                        "Per-query median shuffle-write bytes and Spark JVM GC time. Encoding and partial aggregation differ between row and native shuffle, so byte differences are expected. Task shuffle-write time and all SQL metrics are preserved in the raw records.", "",
                        "| Scenario / query | Original shuffle MiB | Heap shuffle MiB | Direct shuffle MiB | Original GC ms | Heap GC ms | Direct GC ms |",
                        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    process_lines = ["", "## Process memory", "", "Median process-lifetime maximum Spark JVM RSS across independent forks, in MiB. Includes warmups and startup.", "",
                     "| Scenario | Original MiB | Heap MiB | Direct MiB | Heap change | Direct change |",
                     "| --- | ---: | ---: | ---: | ---: | ---: |"]
    configuration_lines = ["", "## Workload configuration", "",
                           "| Scenario | Input rows | Payload bytes/row | Payload | Partitions | Codec | Admission | Frame | Batch rows | Cores | Replicas | Skew percent |",
                           "| --- | ---: | ---: | --- | ---: | --- | --- | --- | ---: | ---: | --- | ---: |"]
    allocation_lines = ["", "## Complete-query Java heap allocation", "",
                        "Public JVM allocation counters surround the collect action. Counters for surviving threads are summed; if threads end during the interval, their allocation is omitted and the value is a lower bound. The ended-thread count includes threads created and terminated during the interval. This measures allocation traffic, not simultaneous live memory or native allocation.", "",
                        "| Scenario / query | Original allocated MiB | Heap allocated MiB | Direct allocated MiB | Direct vs heap | Maximum ended threads O/H/D |",
                        "| --- | ---: | ---: | ---: | ---: | --- |"]
    manifests = {}
    for directory in sorted(args.artifacts.glob("e2e-*")):
        name = directory.name.removeprefix("e2e-")
        config = json.loads((directory / "scenario.json").read_text())
        protocol = json.loads((directory / "protocol.json").read_text())
        assert protocol["forks"] > 0 and protocol["samples"] > 0
        reference = json.loads((directory / "reference.json").read_text())["queries"]
        entries = {mode: [] for mode in ["original", "heap", "direct"]}
        process = {mode: [] for mode in entries}
        frames = {mode: [] for mode in ["heap", "direct"]}
        for fork in range(protocol["forks"]):
            for mode in entries:
                target = directory / f"fork-{fork}-{mode}"
                record = json.loads((target / "queries.json").read_text())
                cm = json.loads((target / "comet-manifest.json").read_text())
                cb = json.loads((target / "celeborn-manifest.json").read_text())
                version = "original" if mode == "original" else "current"
                previous = manifests.setdefault(version, {"comet": cm, "celeborn": cb})
                assert previous == {"comet": cm, "celeborn": cb}
                for query_name, query in record["queries"].items():
                    assert query["sha256"] == reference[query_name]["sha256"] and query["rows"] == reference[query_name]["rows"]
                    assert query["native"] == (mode != "original")
                    samples = [s for s in query["samples"] if not s["warmup"]]
                    assert len(samples) == protocol["samples"]
                    assert all(s["task_metrics"]["Shuffle Bytes Written"] > 0 for s in samples)
                storage_file = target / "queries.json.worker-storage.json"
                # Older syntax/path smoke runs predate the generic storage record.
                if storage_file.exists():
                    assert json.loads(storage_file.read_text())["bytes"] > 0
                if mode != "original":
                    frame_records = json.loads((target / "queries.json.native-frames.json").read_text())
                    assert frame_records
                    frames[mode].extend(frame["payload_bytes"] for frame in frame_records)
                entries[mode].append(record["queries"])
                process[mode].append(resources(target / "process-resources.txt"))
        summary = {"config": config, "protocol": protocol, "queries": {}, "process": process, "frames": {mode: {"samples": len(values), "min": min(values), "median": median(values), "max": max(values)} for mode, values in frames.items()}}
        for query_name in reference:
            query_summary = {"sha256": reference[query_name]["sha256"], "rows": reference[query_name]["rows"]}
            for mode in entries:
                samples = [s for fork in entries[mode] for s in fork[query_name]["samples"] if not s["warmup"]]
                query_summary[mode] = {"samples": len(samples), **{key: median([s[key] for s in samples]) for key in
                     ["seconds", "cpu_seconds", "driver_rss_bytes", "service_rss_bytes", "combined_rss_bytes", "gc_ms", "gc_count"]},
                     "task_metrics": {key: median([s["task_metrics"][key] for s in samples]) for key in samples[0]["task_metrics"]},
                     "fork_median_seconds": [median([s["seconds"] for s in fork[query_name]["samples"] if not s["warmup"]]) for fork in entries[mode]]}
                if "jvm_allocated_bytes" in samples[0]:
                    query_summary[mode]["jvm_allocated_bytes"] = median([s["jvm_allocated_bytes"] for s in samples])
                    query_summary[mode]["allocation_threads_ended"] = max(s["allocation_threads_ended"] for s in samples)
            for ref, candidate in [("original", "heap"), ("original", "direct"), ("heap", "direct")]:
                query_summary[candidate + "_vs_" + ref] = comparison(query_summary[ref]["fork_median_seconds"], query_summary[candidate]["fork_median_seconds"])
            original, heap, direct = [query_summary[mode] for mode in entries]
            lines.append(f"| {name} / {query_name} | {original['seconds']:.3f} | {heap['seconds']:.3f} | {direct['seconds']:.3f} | {(heap['seconds']/original['seconds']-1)*100:+.1f}% | {(direct['seconds']/original['seconds']-1)*100:+.1f}% | {(direct['seconds']/heap['seconds']-1)*100:+.1f}% |")
            o, h, d = [v["driver_rss_bytes"] / 1048576 for v in [original, heap, direct]]
            memory_lines.append(f"| {name} / {query_name} | {o:.1f} | {h:.1f} | {d:.1f} | {(h/o-1)*100:+.1f}% | {(d/o-1)*100:+.1f}% |")
            diagnostic_lines.append(f"| {name} / {query_name} | {original['task_metrics']['Shuffle Bytes Written']/1048576:.1f} | {heap['task_metrics']['Shuffle Bytes Written']/1048576:.1f} | {direct['task_metrics']['Shuffle Bytes Written']/1048576:.1f} | {original['gc_ms']:.0f} | {heap['gc_ms']:.0f} | {direct['gc_ms']:.0f} |")
            if "jvm_allocated_bytes" in heap:
                o, h, d = [v["jvm_allocated_bytes"] / 1048576 for v in [original, heap, direct]]
                ended = "/".join(str(v["allocation_threads_ended"]) for v in [original, heap, direct])
                allocation_lines.append(f"| {name} / {query_name} | {o:.1f} | {h:.1f} | {d:.1f} | {(d/h-1)*100:+.1f}% | {ended} |")
            summary["queries"][query_name] = query_summary
        o, h, d = [median([fork["max_rss_bytes"] for fork in process[mode]]) / 1048576 for mode in process]
        process_lines.append(f"| {name} | {o:.1f} | {h:.1f} | {d:.1f} | {(h/o-1)*100:+.1f}% | {(d/o-1)*100:+.1f}% |")
        configuration_lines.append(f"| {name} | {config['rows']:,} | {config['width']} | {config['entropy']} | {config['partitions']} | {config['codec']} | {config['admission']} | {config['frame']} | {config.get('batch', 8192)} | {config['cores']} | {config['replicate']} | {config['skew']} |")
        result[name] = summary
    assert result
    lines.extend(memory_lines + process_lines + diagnostic_lines + allocation_lines + configuration_lines)
    lines.extend(["", "## Binary manifests", "", "```json", json.dumps(manifests, indent=2), "```", ""])
    (args.output / "performance.md").write_text("\n".join(lines))
    (args.output / "performance.json").write_text(json.dumps({"manifests": manifests, "scenarios": result}, indent=2) + "\n")
    print("VERIFIED_SCENARIOS=" + str(len(result)))


if __name__ == "__main__":
    main()
