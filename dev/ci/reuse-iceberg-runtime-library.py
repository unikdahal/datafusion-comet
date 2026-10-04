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

"""Find a fork release artifact with identical release inputs."""

import json
import os
import re
import subprocess
import urllib.request

repo = os.environ["GITHUB_REPOSITORY"]
assert repo == "unikdahal/datafusion-comet"


def tree(ref, path):
    return subprocess.check_output(["git", "rev-parse", f"{ref}:{path}"], text=True).strip()


# This one external fixture is explicitly gated by #[cfg(test)] in join.rs.
# Its parent module and every other native input still must be identical.
TEST_FIXTURE = "native/core/src/execution/operators/dynamic_filter/join/tests.rs"


def native_matches(ref):
    if tree(ref, "native") == tree("HEAD", "native"):
        return True
    changed = subprocess.check_output([
        "git", "diff", "--name-only", ref, "HEAD", "--", "native"
    ], text=True).splitlines()
    if changed != [TEST_FIXTURE]:
        return False
    parent = subprocess.check_output([
        "git", "show", "HEAD:native/core/src/execution/operators/dynamic_filter/join.rs"
    ], text=True)
    return bool(re.search(r"#\[cfg\(test\)\]\s+mod tests;", parent))


paths = ("rust-toolchain.toml", ".github/actions/setup-builder/action.yaml")
current = tuple(tree("HEAD", path) for path in paths)


def build_settings(ref):
    workflow = subprocess.check_output([
        "git", "show", f"{ref}:.github/workflows/iceberg_runtime_benchmark.yml"
    ], text=True)
    return (
        re.findall(r"^\s+(?:RUSTFLAGS|runs-on|image):.*$", workflow, re.MULTILINE),
        re.findall(r"cargo build[^\n]+", workflow),
    )


settings = build_settings("HEAD")
request = urllib.request.Request(
    f"https://api.github.com/repos/{repo}/actions/artifacts?name=runtime-release-library&per_page=100",
    headers={"Authorization": "Bearer " + os.environ["GH_TOKEN"], "Accept": "application/vnd.github+json"},
)
with urllib.request.urlopen(request, timeout=30) as response:
    artifacts = json.load(response)["artifacts"]
for artifact in artifacts:
    run = artifact["workflow_run"]
    if artifact["expired"] or run["head_branch"] != "feat/iceberg-runtime-pruning":
        continue
    try:
        previous = tuple(tree(run["head_sha"], path) for path in paths)
    except subprocess.CalledProcessError:
        continue
    if previous != current or not native_matches(run["head_sha"]) or build_settings(run["head_sha"]) != settings:
        continue
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"artifact_id={artifact['id']}\nrun_id={run['id']}\n")
    print(f"Reusing release artifact {artifact['id']} from {run['head_sha']}; release inputs are identical (only the cfg(test) join fixture may differ).")
    break
else:
    print("No release artifact matches the release inputs; building from source.")
