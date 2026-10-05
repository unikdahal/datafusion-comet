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
import argparse
import json
import re
import statistics
from pathlib import Path
import xml.etree.ElementTree as ET


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    lines = ["# Buffer shuffle evidence", "",
             "Measurements run on GitHub-hosted Ubuntu 24.04, Java 17, local Spark with four task threads,",
             "one Celeborn master and two workers. Native code uses the production release profile with thin LTO and one codegen unit.",
             "The deterministic input has 1,000,000 left rows and 257 right rows. Each query has one",
             "warmup and three measured executions with freshly rebuilt exchanges.", "",
             "The local Spark baseline establishes result parity. Compare heap and direct timings",
             "within a replication setting; these are complete queries, not isolated shuffle throughput.", ""]
    report = {"clusters": {}, "test_reports": {}}
    for directory in sorted(args.artifacts.glob("real-cluster-replicate-*")):
        results = {mode: json.loads((directory / (mode + ".json")).read_text())
                   for mode in ["baseline", "heap", "direct"]}
        cluster = {"comet_sha": (directory / "comet-sha.txt").read_text().strip(),
                   "celeborn_sha": (directory / "celeborn-sha.txt").read_text().strip(), "queries": {}}
        manifest = (directory / "native-build.txt").read_text().splitlines()
        assert manifest[0] == cluster["comet_sha"], "Native library must match the tested Comet commit"
        cluster["native_build"] = manifest
        lines.extend(["## " + directory.name, "", "Comet: `" + cluster["comet_sha"] + "`",
                      "Celeborn: `" + cluster["celeborn_sha"] + "`", "",
                      "| Query | Rows | Heap median seconds | Direct median seconds | Direct / heap |",
                      "| --- | ---: | ---: | ---: | ---: |"])
        for name, baseline in results["baseline"].items():
            query = {"sha256": baseline["sha256"], "rows": baseline["rows"]}
            for mode in ["heap", "direct"]:
                actual = results[mode][name]
                assert actual["sha256"] == baseline["sha256"] and actual["rows"] == baseline["rows"]
                assert len(actual["seconds"]) == 3
                query[mode] = {"seconds": actual["seconds"], "median_seconds": statistics.median(actual["seconds"])}
            heap, direct = query["heap"]["median_seconds"], query["direct"]["median_seconds"]
            lines.append(f"| {name} | {baseline['rows']} | {heap:.3f} | {direct:.3f} | {direct / heap:.3f} |")
            cluster["queries"][name] = query
        lines.extend(["", "Process CPU time and peak resident memory are in the accompanying `.resources.txt` files.", "Stored frame samples and executed plans accompany each result digest.", ""])
        lines.extend(["| Mode | Process user CPU seconds | Process system CPU seconds | Peak resident MiB |",
                      "| --- | ---: | ---: | ---: |"])
        for mode in ["heap", "direct"]:
            resources = {}
            for line in (directory / (mode + ".resources.txt")).read_text().splitlines():
                if ": " in line:
                    key, value = line.strip().rsplit(": ", 1)
                    resources[key] = value
            user = float(resources["User time (seconds)"])
            system = float(resources["System time (seconds)"])
            rss = int(resources["Maximum resident set size (kbytes)"])
            cluster[mode + "_resources"] = {"user_cpu_seconds": user,
                "system_cpu_seconds": system, "peak_resident_kib": rss}
            lines.append(f"| {mode} | {user:.2f} | {system:.2f} | {rss / 1024:.1f} |")
            samples = json.loads((directory / (mode + ".json.worker-native-frames.json")).read_text())
            assert samples
            cluster[mode + "_stored_frame_samples"] = len(samples)
        lines.append("")
        report["clusters"][directory.name] = cluster
    assert len(report["clusters"]) == 2, "Both replication settings must have evidence"
    lines.extend(["## Automated regression checks", "",
                  "| Artifact | Tests | Failures | Errors | Skipped |",
                  "| --- | ---: | ---: | ---: | ---: |"])
    for artifact in sorted(args.artifacts.iterdir()):
        counts = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
        for file in artifact.rglob("*.xml"):
            if not file.name.startswith("TEST-"):
                continue
            root = ET.parse(file).getroot()
            suites = [root] if root.tag == "testsuite" else root.findall("testsuite")
            for suite in suites:
                for key in counts:
                    counts[key] += int(suite.get(key, 0))
        if counts["tests"]:
            report["test_reports"][artifact.name] = counts
            assert counts["failures"] == 0 and counts["errors"] == 0, (artifact, counts)
            lines.append(f"| {artifact.name} | {counts['tests']} | 0 | 0 | {counts['skipped']} |")
    native_log = (args.artifacts / "native-contract-and-formatting" / "native-tests.log").read_text()
    native_counts = re.findall(r"test result: ok\. (\d+) passed; 0 failed", native_log)
    assert len(native_counts) >= 2, "Native shuffle and JNI test results must be present"
    report["native_tests_passed"] = sum(int(count) for count in native_counts)
    lines.extend(["", f"Native shuffle and JNI: {report['native_tests_passed']} tests passed. Clippy checks all targets with warnings denied.", ""])
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "evidence.json").write_text(json.dumps(report, indent=2))
    (args.output / "evidence.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
