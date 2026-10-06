#!/usr/bin/env python3
"""Checks result parity across modes and summarizes repeated runs as median and IQR."""
import glob
import json
import os
import re
import statistics
import sys


def quantiles(values):
    values = sorted(values)
    if len(values) < 2:
        return values[0], values[0], values[0]
    q = statistics.quantiles(values, n=4, method="inclusive")
    return q[0], statistics.median(values), q[2]


GC_LINE = re.compile(r"GC\(\d+\) Pause .* (\d+)M->(\d+)M\(")


def heap_allocated(result_path):
    """Bytes allocated on the Java heap over the whole application, from its G1 log.

    Each pause reports heap use before and after it; allocation between two pauses is the use
    before the second minus the use after the first. Unlike per-thread counters, this includes
    threads that have exited, such as native threads that attach to the JVM for one call.
    """
    log = os.path.join(os.path.dirname(result_path), "..", "logs",
                       os.path.basename(result_path)[:-len(".json")] + ".gc.log")
    if not os.path.exists(log):
        return 0
    allocated = 0
    previous_after = 0
    for line in open(log):
        match = GC_LINE.search(line)
        if match:
            before, after = int(match.group(1)), int(match.group(2))
            allocated += max(0, before - previous_after)
            previous_after = after
    return allocated << 20


def main(directory, baseline_mode, summary_path):
    runs = {}
    for path in sorted(glob.glob(f"{directory}/*.json")):
        run = json.load(open(path))
        run["heapAllocatedBytes"] = heap_allocated(path)
        runs.setdefault(run["mode"], []).append(run)
    modes = list(runs)
    baseline = runs[baseline_mode][0]
    failures = []
    for mode, mode_runs in runs.items():
        for run in mode_runs:
            for name, query in run["queries"].items():
                if query["rows"] != baseline["queries"][name]["rows"]:
                    failures.append(f"{mode} {name}")
    lines = ["## Result parity", ""]
    lines.append("All modes match vanilla Spark on every query in every pass."
                 if not failures else "MISMATCHES: " + ", ".join(failures))
    lines += ["", f"Passes per mode: " + ", ".join(f"{m}={len(r)}" for m, r in runs.items()), ""]

    def row(label, extract, scale=1.0, fmt="{:.2f}"):
        cells = []
        for mode in modes:
            q1, med, q3 = quantiles([extract(run) / scale for run in runs[mode]])
            cells.append(f"{fmt.format(med)} ({fmt.format(q1)}–{fmt.format(q3)})")
        return f"| {label} | " + " | ".join(cells) + " |"

    header = "| metric (median, IQR) | " + " | ".join(modes) + " |"
    sep = "|---|" + "---|" * len(modes)
    total = lambda key: (lambda run: sum(q["metrics"][key] for q in run["queries"].values()))
    shuffle = lambda key: (lambda run: sum(run["queries"][q]["metrics"][key] for q in ("shuffle_repartition", "shuffle_aggregate")))
    lines += ["## Totals over TPC-H q1-q22 and the shuffle-bound queries", "", header, sep,
              row("wall time (s)", total("seconds")),
              row("peak process RSS (MiB)", lambda run: run["peakRssBytes"], 1 << 20, "{:.0f}"),
              row("shuffle write time (s)", total("shuffleWriteTime"), 1e9),
              row("executor CPU time (s)", total("executorCpuTime"), 1e9),
              row("JVM GC time (ms)", total("gcMillis"), 1.0, "{:.0f}"),
              row("GC count", total("gcCount"), 1.0, "{:.0f}"),
              row("Java heap allocated, whole app, from GC log (GiB)",
                  lambda run: run["heapAllocatedBytes"], 1 << 30),
              row("shuffle bytes written (MiB)", total("shuffleWriteBytes"), 1 << 20, "{:.0f}"),
              "", "## Shuffle-bound queries (shuffle_repartition + shuffle_aggregate)", "", header, sep,
              row("wall time (s)", shuffle("seconds")),
              row("shuffle write time (s)", shuffle("shuffleWriteTime"), 1e9),
              row("executor CPU time (s)", shuffle("executorCpuTime"), 1e9),
              row("JVM GC time (ms)", shuffle("gcMillis"), 1.0, "{:.0f}"),
              row("shuffle bytes written (MiB)", shuffle("shuffleWriteBytes"), 1 << 20, "{:.0f}"),
              row("native exchanges", shuffle("nativeExchanges"), 1.0, "{:.0f}"),
              "", "## Per-query wall time (s)", "", "| query | " + " | ".join(modes) + " |", sep]
    for name in baseline["queries"]:
        lines.append(row(name, lambda run, n=name: run["queries"][n]["metrics"]["seconds"])
                     .replace("| " + name + " |", "| " + name + " |", 1))
    text = "\n".join(lines) + "\n"
    print(text)
    with open(summary_path, "a") as summary:
        summary.write(text)
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])
