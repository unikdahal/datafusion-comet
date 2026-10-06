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
    parser.add_argument("--allow-incomplete", action="store_true", help="Report incomplete scenarios explicitly, without performance claims for them")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    result = {}
    failures = {}
    lines = ["# End-to-end speed and memory", "",
             "All measurements ran in GitHub Actions on Ubuntu 24.04 with Java 17 and Spark 3.5.9. Each scenario uses one isolated runner, one master, and two workers on that machine. Modes run sequentially; no Spark benchmark shares its runner with another benchmark.", "",
             "**Original** runs the exact original Comet and Celeborn fork-main commits. Its capability check selects delegated row shuffle. **Heap** and **Direct** run the new native shuffle implementation; heap is asynchronous with one JNI copy, direct is a synchronous native-buffer borrow. Native shuffle also permits downstream operators to remain native: these original hash plans use Spark sort, while the new default uses Comet sort. Original/new comparisons therefore include shuffle format and downstream execution changes. Heap/direct comparisons isolate the buffer-mode tradeoff.", "",
             "Every query rebuilds its exchanges; exchange reuse and AQE are disabled. The mode order rotates between independent JVM forks. Warmups are excluded. Result digests match vanilla Spark. Native plans and current-app worker frame samples prove the intended paths; manifests verify source and binary hashes.", "",
             "RSS is resident process memory, not the framing allocation. Driver RSS includes native memory; combined RSS sums the Spark JVM, master and both workers (shared mappings may be counted twice). The per-query peak is sampled every 100 ms. A separate process-lifetime high-water RSS is obtained with /usr/bin/time. Worker storage is cleared between fresh service/JVM runs.", "",
             "Three independent JVM forks provide limited information about runner variability. Paired fork ranges and exploratory bootstrap bounds are retained in JSON; these results are diagnostic measurements on one host, not a multi-machine deployment or a production guarantee.", "",
             "## Query speed", "", "Median complete-query seconds across measured samples, excluding warmups. Lower seconds is faster. Speedup is original seconds divided by candidate seconds: 2x means half the elapsed time. Direct vs heap is elapsed-time change; negative means faster.", "",
             "| Scenario / query | Original s | Heap s | Direct s | Heap speedup | Direct speedup | Direct vs heap |",
             "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    consistency_lines = ["", "## Speed consistency across fresh JVMs", "",
                         "Elapsed-time ratios use paired medians from each independent JVM fork. The range shows the fastest and slowest candidate/reference ratio; below 1 is faster. The geometric mean gives every fork equal weight. Three forks do not establish a production latency distribution.", "",
                         "| Scenario / query | Heap/original mean ratio | Fork range | Direction | Direct/heap mean ratio | Fork range | Direction |",
                         "| --- | ---: | --- | --- | ---: | --- | --- |"]
    cpu_lines = ["", "## Query CPU and shuffle-write time", "",
                 "O/H/D means Original/Heap/Direct. Median process CPU seconds during the query include Spark JVM native threads. Combined CPU adds the master and both workers using each individual sample. CPU seconds can exceed elapsed seconds because threads execute concurrently. Task shuffle-write seconds sum across tasks and are not an additive component of query elapsed time.", "",
                 "| Scenario / query | Spark CPU s O/H/D | Combined CPU s O/H/D | Task shuffle-write s O/H/D |",
                 "| --- | --- | --- | --- |"]
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
                           "| Scenario | Input rows | Payload bytes/row | Payload | Partitions | Codec | Admission | Frame | Batch rows | Cores | Replicas | Skew percent | Offheap | Native sort |",
                           "| --- | ---: | ---: | --- | ---: | --- | --- | --- | ---: | ---: | --- | ---: | --- | --- |"]
    allocation_lines = ["", "## Complete-query Java heap allocation", "",
                        "Public JVM allocation counters surround the collect action. Counters for surviving threads are summed; if threads end during the interval, their allocation is omitted and the value is a lower bound. The ended-thread count includes threads created and terminated during the interval. This measures allocation traffic, not simultaneous live memory or native allocation.", "",
                        "| Scenario / query | Original allocated MiB | Heap allocated MiB | Direct allocated MiB | Direct vs heap | Maximum ended threads O/H/D |",
                        "| --- | ---: | ---: | ---: | ---: | --- |"]
    live_memory_lines = ["", "## Live memory diagnostics", "",
                         "These counters are sampled only in the separate allocation scenarios. Peak counters can occur at different instants, so their medians must not be added or subtracted to decompose RSS. Rust live bytes count allocator Layout bytes; they exclude retained allocator pages, fragmentation, libc allocations, and mmap. JVM direct-buffer counters do not cover every native allocation. After-query Rust bytes help distinguish live allocations from process residency, but do not provide an allocation-site profile.", "",
                         "| Scenario / query / mode | Peak Rust live MiB | Rust live after MiB | Peak pool reserved MiB | Peak Java heap used MiB | Peak Java heap committed MiB | Peak JVM direct-buffer MiB |",
                         "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    residency_lines = ["", "## Resident memory after all queries", "",
                       "Each fresh diagnostic JVM records Linux smaps before and after an explicit full GC, after all measured queries. The Java heap address range comes from the public jcmd GC.heap_info command. Other anonymous memory includes native arenas, JVM allocations and thread stacks; it does not identify allocation sites. File-backed and special mappings are reported separately. Values are medians across forks, in MiB.", "",
                       "| Scenario / mode / snapshot | Total RSS | Java heap RSS | Other anonymous RSS | Other RSS | Live Rust bytes | Java heap used | Java heap committed |",
                       "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    allocator_lines = ["", "## Native allocator retention", "",
                       "glibc mallinfo2 reports process-wide malloc arenas, including JVM and native-library allocations. Arena live/free counts exclude mmap allocations, which are reported separately. After all queries and full GC, malloc_trim(0) attempts to return unused resident pages to the OS. This diagnostic call never runs during timed queries and is not part of the product implementation. A reduction in RSS with unchanged live allocation counters demonstrates retained freed pages; counters do not identify individual allocation sites.", "",
                       "| Scenario / mode / snapshot | Arena MiB | Live arena MiB | Free arena MiB | Malloc mmap MiB |",
                       "| --- | ---: | ---: | ---: | ---: |"]
    manifests = {}
    for directory in sorted(args.artifacts.glob("e2e-*")):
        name = directory.name.removeprefix("e2e-")
        config = json.loads((directory / "scenario.json").read_text())
        protocol = json.loads((directory / "protocol.json").read_text())
        assert protocol["forks"] > 0 and protocol["samples"] > 0
        reference = json.loads((directory / "reference.json").read_text())["queries"]
        incomplete = []
        for fork in range(protocol["forks"]):
            for mode in ["original", "heap", "direct"]:
                target = directory / f"fork-{fork}-{mode}" / "queries.json"
                if not target.exists():
                    incomplete.append({"fork": fork, "mode": mode, "reason": "missing query record"})
                    continue
                queries = json.loads(target.read_text())["queries"]
                for query_name in reference:
                    samples = [s for s in queries.get(query_name, {}).get("samples", []) if not s["warmup"]]
                    if len(samples) != protocol["samples"]:
                        incomplete.append({"fork": fork, "mode": mode, "query": query_name, "measured_samples": len(samples), "required_samples": protocol["samples"]})
        if incomplete:
            assert args.allow_incomplete, (name, incomplete)
            failures[name] = {"config": config, "protocol": protocol, "incomplete": incomplete,
                              "benchmark_source": (directory / "benchmark-source.txt").read_text().strip()}
            continue
        entries = {mode: [] for mode in ["original", "heap", "direct"]}
        process = {mode: [] for mode in entries}
        residency = {mode: [] for mode in entries}
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
                residency_file = target / "queries.json.residency.json"
                if residency_file.exists():
                    residency[mode].append(json.loads(residency_file.read_text()))
        summary = {"config": config, "protocol": protocol, "benchmark_source": (directory / "benchmark-source.txt").read_text().strip(), "queries": {}, "process": process, "frames": {mode: {"samples": len(values), "min": min(values), "median": median(values), "max": max(values)} for mode, values in frames.items()}}
        for query_name in reference:
            query_summary = {"sha256": reference[query_name]["sha256"], "rows": reference[query_name]["rows"]}
            for mode in entries:
                samples = [s for fork in entries[mode] for s in fork[query_name]["samples"] if not s["warmup"]]
                query_summary[mode] = {"samples": len(samples), **{key: median([s[key] for s in samples]) for key in
                     ["seconds", "cpu_seconds", "driver_rss_bytes", "service_rss_bytes", "combined_rss_bytes", "gc_ms", "gc_count"]},
                     "task_metrics": {key: median([s["task_metrics"][key] for s in samples]) for key in samples[0]["task_metrics"]},
                     "fork_median_seconds": [median([s["seconds"] for s in fork[query_name]["samples"] if not s["warmup"]]) for fork in entries[mode]]}
                query_summary[mode]["combined_cpu_seconds"] = median([s["cpu_seconds"] + s.get("service_cpu_seconds", 0) for s in samples])
                for key in ["service_cpu_seconds", "driver_threads", "jvm_heap_used_after_bytes", "jvm_heap_committed_after_bytes"]:
                    if key in samples[0]:
                        query_summary[mode][key] = median([s[key] for s in samples])
                if "jvm_allocated_bytes" in samples[0]:
                    query_summary[mode]["jvm_allocated_bytes"] = median([s["jvm_allocated_bytes"] for s in samples])
                    query_summary[mode]["allocation_threads_ended"] = max(s["allocation_threads_ended"] for s in samples)
                if "live_memory_after" in samples[0]:
                    keys = list(samples[0]["live_memory_after"])
                    query_summary[mode]["live_memory_peak"] = {key: median([s[key] for s in samples]) for key in keys}
                    query_summary[mode]["live_memory_after"] = {key: median([s["live_memory_after"][key] for s in samples]) for key in keys}
                    values = query_summary[mode]["live_memory_peak"]
                    after = query_summary[mode]["live_memory_after"]
                    counters = [values["native_live_bytes"], after["native_live_bytes"], values["native_pool_reserved_bytes"], values["jvm_heap_used_bytes"], values["jvm_heap_committed_bytes"], values["jvm_direct_buffer_bytes"]]
                    live_memory_lines.append(f"| {name} / {query_name} / {mode} | " + " | ".join(f"{v/1048576:.1f}" for v in counters) + " |")
            for ref, candidate in [("original", "heap"), ("original", "direct"), ("heap", "direct")]:
                query_summary[candidate + "_vs_" + ref] = comparison(query_summary[ref]["fork_median_seconds"], query_summary[candidate]["fork_median_seconds"])
            original, heap, direct = [query_summary[mode] for mode in entries]
            lines.append(f"| {name} / {query_name} | {original['seconds']:.3f} | {heap['seconds']:.3f} | {direct['seconds']:.3f} | {original['seconds']/heap['seconds']:.2f}x | {original['seconds']/direct['seconds']:.2f}x | {(direct['seconds']/heap['seconds']-1)*100:+.1f}% |")
            h_ratio, d_ratio = query_summary["heap_vs_original"], query_summary["direct_vs_heap"]
            consistency_lines.append(f"| {name} / {query_name} | {h_ratio['ratio']:.3f} | {h_ratio['ratio_min']:.3f}-{h_ratio['ratio_max']:.3f} | {h_ratio['direction']} | {d_ratio['ratio']:.3f} | {d_ratio['ratio_min']:.3f}-{d_ratio['ratio_max']:.3f} | {d_ratio['direction']} |")
            spark_cpu = "/".join(f"{v['cpu_seconds']:.2f}" for v in [original, heap, direct])
            combined_cpu = "/".join(f"{v['combined_cpu_seconds']:.2f}" for v in [original, heap, direct])
            write_time = "/".join(f"{v['task_metrics']['Shuffle Write Time']/1e9:.3f}" for v in [original, heap, direct])
            cpu_lines.append(f"| {name} / {query_name} | {spark_cpu} | {combined_cpu} | {write_time} |")
            o, h, d = [v["driver_rss_bytes"] / 1048576 for v in [original, heap, direct]]
            memory_lines.append(f"| {name} / {query_name} | {o:.1f} | {h:.1f} | {d:.1f} | {(h/o-1)*100:+.1f}% | {(d/o-1)*100:+.1f}% |")
            diagnostic_lines.append(f"| {name} / {query_name} | {original['task_metrics']['Shuffle Bytes Written']/1048576:.1f} | {heap['task_metrics']['Shuffle Bytes Written']/1048576:.1f} | {direct['task_metrics']['Shuffle Bytes Written']/1048576:.1f} | {original['gc_ms']:.0f} | {heap['gc_ms']:.0f} | {direct['gc_ms']:.0f} |")
            if "jvm_allocated_bytes" in heap:
                o, h, d = [v["jvm_allocated_bytes"] / 1048576 for v in [original, heap, direct]]
                ended = "/".join(str(v["allocation_threads_ended"]) for v in [original, heap, direct])
                allocation_lines.append(f"| {name} / {query_name} | {o:.1f} | {h:.1f} | {d:.1f} | {(d/h-1)*100:+.1f}% | {ended} |")
            summary["queries"][query_name] = query_summary
        if any(residency.values()):
            assert all(len(values) == protocol["forks"] for values in residency.values())
            summary["residency"] = residency
            for mode, records in residency.items():
                for label in ["before_gc", "after_gc", "after_trim"]:
                    if label not in records[0]:
                        continue
                    keys = ["total_rss_bytes", "java_heap_rss_bytes", "outside_heap_anonymous_rss_bytes", "other_rss_bytes", "native_live_bytes", "java_heap_used_bytes", "java_heap_committed_bytes"]
                    values = [median([record[label][key] for record in records]) / 1048576 for key in keys]
                    residency_lines.append(f"| {name} / {mode} / {label} | " + " | ".join(f"{v:.1f}" for v in values) + " |")
                    if "allocator_arena_bytes" in records[0][label]:
                        keys = ["allocator_arena_bytes", "allocator_live_arena_bytes", "allocator_free_arena_bytes", "allocator_mmap_bytes"]
                        values = [median([record[label][key] for record in records]) / 1048576 for key in keys]
                        allocator_lines.append(f"| {name} / {mode} / {label} | " + " | ".join(f"{v:.1f}" for v in values) + " |")
        o, h, d = [median([fork["max_rss_bytes"] for fork in process[mode]]) / 1048576 for mode in process]
        process_lines.append(f"| {name} | {o:.1f} | {h:.1f} | {d:.1f} | {(h/o-1)*100:+.1f}% | {(d/o-1)*100:+.1f}% |")
        configuration_lines.append(f"| {name} | {config['rows']:,} | {config['width']} | {config['entropy']} | {config['partitions']} | {config['codec']} | {config['admission']} | {config['frame']} | {config.get('batch', 8192)} | {config['cores']} | {config['replicate']} | {config['skew']} | {config.get('offheap', '1g')} | {config.get('native_sort', True)} |")
        result[name] = summary
    assert result
    if failures:
        lines[2:2] = ["**Incomplete scenarios: " + ", ".join(sorted(failures)) + ".** These failed to complete the measurement protocol. No speed or memory comparison is claimed for them. The raw artifacts retain the error logs and partial records; JSON lists every missing measurement.", ""]
    lines.extend(consistency_lines + cpu_lines + memory_lines + process_lines + diagnostic_lines + allocation_lines + live_memory_lines + residency_lines + allocator_lines + configuration_lines)
    lines.extend(["", "## Binary manifests", "", "```json", json.dumps(manifests, indent=2), "```", ""])
    (args.output / "performance.md").write_text("\n".join(lines))
    (args.output / "performance.json").write_text(json.dumps({"manifests": manifests, "scenarios": result, "incomplete_scenarios": failures}, indent=2) + "\n")
    print("VERIFIED_SCENARIOS=" + str(len(result)))
    print("INCOMPLETE_SCENARIOS=" + str(len(failures)))


if __name__ == "__main__":
    main()
