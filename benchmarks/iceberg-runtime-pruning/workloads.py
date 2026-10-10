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


"""Spark-free query catalog and pre-measurement experiment declaration."""

import hashlib
import json
import os

JOIN = """
SELECT /*+ BROADCAST(d) */ count(*), sum(f.value), sum(length(f.payload))
FROM bench.db.{table} f JOIN bench.db.dim d ON f.id = d.id
"""
TOPK = "SELECT id, value, payload FROM bench.db.{table} ORDER BY id LIMIT 10"
MIN = "SELECT min(id) FROM bench.db.{table}"

QUERIES = [
    ("join_sorted", JOIN.format(table="fact_sorted")),
    ("join_position_deletes", JOIN.format(table="fact_pos_deletes")),
    ("join_unsorted", JOIN.format(table="fact_unsorted")),
    ("topk_sorted", TOPK.format(table="fact_sorted")),
    ("topk_position_deletes", TOPK.format(table="fact_pos_deletes")),
    ("topk_unsorted", TOPK.format(table="fact_unsorted")),
    ("min_sorted", MIN.format(table="fact_sorted")),
    ("min_unsorted", MIN.format(table="fact_unsorted")),
    (
        "scan_control",
        "SELECT count(*), sum(value) FROM bench.db.fact_sorted WHERE payload LIKE 'ff%'",
    ),
]

EXTENDED_QUERIES = QUERIES + [
    ("max_sorted", "SELECT max(id) FROM bench.db.fact_sorted"),
    ("max_unsorted", "SELECT max(id) FROM bench.db.fact_unsorted"),
    ("topk_desc_sorted", "SELECT id, value, payload FROM bench.db.fact_sorted ORDER BY id DESC LIMIT 10"),
    ("join_evolved", JOIN.format(table="fact_evolved")),
    ("topk_evolved", TOPK.format(table="fact_evolved")),
    ("min_evolved", MIN.format(table="fact_evolved")),
    ("scan_evolved_control", "SELECT count(*), sum(value) FROM bench.db.fact_evolved"),
] + [
    (
        f"join_nonselective_{layout}",
        "SELECT /*+ SHUFFLE_HASH(d) */ count(*), sum(f.value), sum(length(f.payload)) "
        f"FROM bench.db.fact_{layout} f JOIN bench.db.dim_all d ON f.id = d.id",
    )
    for layout in ("sorted", "unsorted")
]

PROFILE = os.environ.get("BENCH_PROFILE", "smoke")
if PROFILE == "extended":
    QUERIES = EXTENDED_QUERIES
elif PROFILE != "smoke":
    raise ValueError(f"unknown benchmark profile: {PROFILE}")


def sql_digest(sql):
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


def experiment(rounds, reps):
    if type(rounds) is not int or rounds < 1 or type(reps) is not int or reps < 1:
        raise ValueError("rounds and reps must be positive integers")
    return {
        "protocol_version": 1,
        "profile": PROFILE,
        "variants": ["main", "off", "on"],
        "rounds": rounds,
        "reps": reps,
        "queries": [{"query": name, "sql_sha256": sql_digest(sql), "sql": sql} for name, sql in QUERIES],
    }


if __name__ == "__main__":
    print(json.dumps(experiment(int(os.environ["BENCH_ROUNDS"]), int(os.environ["BENCH_REPS"])), indent=2))
