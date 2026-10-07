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

"""Resolve main once per workflow; both builds consume this immutable manifest."""

import datetime
import json
from pathlib import Path
import re
import subprocess
import sys
import tomllib

MAIN_REPOSITORY = "https://github.com/apache/datafusion-comet.git"


def git(*arguments):
    return subprocess.check_output(["git", *arguments], text=True).strip()


def fixed_sha(value):
    if not isinstance(value, str) or not re.fullmatch("[0-9a-f]{40}", value):
        raise ValueError("Implementation revisions must be full immutable commit SHAs")
    return value


def iceberg_dependency(commit):
    lock = tomllib.loads(git("show", f"{commit}:native/Cargo.lock"))
    packages = [p for p in lock["package"] if p["name"] == "iceberg"]
    if len(packages) != 1:
        raise ValueError(f"Expected one locked Iceberg dependency: {packages}")
    return {
        key: packages[0][key]
        for key in ("name", "version", "source")
        if key in packages[0]
    }


def resolve(configuration, main_repository=MAIN_REPOSITORY):
    if configuration["baseline"] != {
        "repository": MAIN_REPOSITORY,
        "ref": "refs/heads/main",
    }:
        raise ValueError("The baseline must be the latest Apache Comet main")
    # FETCH_HEAD belongs to this single sequential fetch. Do not resolve main independently
    # in build matrix jobs, where it could advance between the two builds.
    subprocess.run(
        ["git", "fetch", "--no-tags", main_repository, "refs/heads/main"], check=True
    )
    baseline = fixed_sha(git("rev-parse", "FETCH_HEAD^{commit}"))
    candidate = fixed_sha(configuration["candidate"]["comet"])
    if git("rev-parse", f"{candidate}^{{commit}}") != candidate:
        raise ValueError("Candidate revision does not identify a commit")
    dependency = iceberg_dependency(candidate)
    expected_iceberg = fixed_sha(configuration["candidate"]["iceberg"])
    if not dependency.get("source", "").endswith("#" + expected_iceberg):
        raise ValueError(
            "Candidate Iceberg dependency differs from its declared revision"
        )
    return {
        "resolved_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "harness": git("rev-parse", "HEAD"),
        "baseline": {
            "repository": MAIN_REPOSITORY,
            "ref": "refs/heads/main",
            "comet": baseline,
            "iceberg_dependency": iceberg_dependency(baseline),
        },
        "candidate": {**configuration["candidate"], "iceberg_dependency": dependency},
        "comparison": "Unmodified latest Apache Comet main versus the fixed candidate; each uses its own locked dependencies.",
    }


if __name__ == "__main__":
    configuration = json.loads(Path(sys.argv[1]).read_text())
    destination = Path(sys.argv[2])
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(resolve(configuration), indent=2) + "\n")
    print(destination.read_text())
