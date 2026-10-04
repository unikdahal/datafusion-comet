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

# This fork run built the original revision with the same fixed toolchain and flags.
# Reuse is optional; absent/expired artifacts cause a source build.
ORIGINAL_RUN = 37192526799
ORIGINAL_HARNESS = "61570575f39af39d6c8a1f64d344f4ee7d5cf597"
BASELINE = "569eaa59d032f758669777964aee2d24eb55ebae"
repo = os.environ["GITHUB_REPOSITORY"]
assert repo == "unikdahal/datafusion-comet"


def settings(ref):
    workflow = subprocess.check_output([
        "git", "show", f"{ref}:.github/workflows/iceberg_code_benchmark.yml"
    ], text=True)
    assert f"&& '{BASELINE}' || github.sha" in workflow
    return (
        re.findall(r"^\s+(?:RUSTFLAGS|runs-on|image):.*$", workflow, re.MULTILINE),
        re.findall(r"cargo build[^\n]+", workflow),
        subprocess.check_output(["git", "rev-parse", f"{ref}:rust-toolchain.toml"], text=True),
    )


if settings("HEAD") != settings(ORIGINAL_HARNESS):
    print("Baseline build settings changed; building from source.")
else:
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/actions/runs/{ORIGINAL_RUN}/artifacts",
        headers={"Authorization": "Bearer " + os.environ["GH_TOKEN"], "Accept": "application/vnd.github+json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        artifacts = json.load(response)["artifacts"]
    for artifact in artifacts:
        if artifact["name"] == "code-benchmark-native-before" and not artifact["expired"]:
            with open(os.environ["GITHUB_OUTPUT"], "a") as output:
                output.write(f"artifact_id={artifact['id']}\nrun_id={ORIGINAL_RUN}\n")
            print(f"Reusing baseline artifact {artifact['id']} from the verified original fork run.")
            break
    else:
        print("No original baseline artifact available; building from source.")
