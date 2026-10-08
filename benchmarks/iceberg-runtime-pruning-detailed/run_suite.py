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

"""Launch the oracle and balanced main/candidate comparisons on one runner."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys

from lib import env, signature

SUITES = {
    "join": {"rows": "16000000", "reps": "2", "shards": 3},
    "topk_minmax": {"rows": "16000000", "reps": "2", "shards": 5},
    "layouts": {"rows": "8000000", "reps": "2", "shards": 2},
    "fuzz": {"rows": "16000000", "reps": "1", "shards": 2},
    "tpch": {"rows": "0", "reps": "1", "shards": 3},
    "strjoin": {"rows": "16000000", "reps": "1", "shards": 3},
}
PARAMETERS = {"files": "16", "rounds": "8", "warmups": "4", "fuzz_rows": "4000000",
              "fuzz_queries": "500", "equality_delete_keys": "4096", "tpch_sf": "3"}


def runner_environment():
    cpuinfo = Path("/proc/cpuinfo").read_text()
    models = sorted({line.split(":", 1)[1].strip() for line in cpuinfo.splitlines()
                     if line.startswith("model name")})
    memory = subprocess.check_output(["free", "-m"], text=True)
    return {"cpu_model": "; ".join(models), "cpuinfo": cpuinfo,
            "cores": int(subprocess.check_output(["nproc"], text=True)),
            "memory_mib": int(next(line for line in memory.splitlines()
                                   if line.startswith("Mem:")).split()[1]),
            "free_m": memory}


def data_manifest(root):
    entries = []
    for path in sorted(root.rglob("*")):
        if path.is_file():
            digest = hashlib.sha256()
            with path.open("rb") as source:
                for block in iter(lambda: source.read(1048576), b""):
                    digest.update(block)
            entries.append({"file": str(path.relative_to(root)),
                            "bytes": path.stat().st_size, "sha256": digest.hexdigest()})
    if not entries:
        raise ValueError("Empty Iceberg warehouse")
    return entries


def query_catalog(suite, round_number):
    if suite == "tpch":
        from run_tpch import queries_for
        queries = queries_for()
    else:
        import run_queries
        run_queries.ROWS = int(SUITES[suite]["rows"])
        run_queries.FUZZ_ROWS = int(PARAMETERS["fuzz_rows"])
        run_queries.FUZZ_QUERIES = int(PARAMETERS["fuzz_queries"])
        queries = run_queries.queries_for(suite, seed=round_number + 1)
    names = [q[0] for q in queries]
    if not names or len(names) != len(set(names)):
        raise ValueError(f"Empty or duplicate canonical query names: {suite}")
    return [{"query": name, "sql_sha256": signature(sql, confs)}
            for name, sql, _, confs in sorted(queries)]


def partition_queries(names, shards, suite):
    # Largest measured costs first, with stable query-name and shard-id tie breaks.
    # Unknown/new names use the suite median (equal weights give sorted round-robin).
    weights = QUERY_WEIGHTS.get(suite, {})
    default_weight = statistics.median(weights.values()) if weights else 1.0
    groups, loads = [[] for _ in range(shards)], [0.0] * shards
    for name in sorted(names, key=lambda name: (-weights.get(name, default_weight), name)):
        shard = min(range(shards), key=lambda index: (loads[index], index))
        groups[shard].append(name)
        loads[shard] += weights.get(name, default_weight)
    return [sorted(group) for group in groups]


def make_campaign(configuration):
    suites = configuration.get("suites", list(SUITES))
    requested = configuration.get("queries", {})
    if (not isinstance(suites, list) or not suites or
            any(s not in SUITES for s in suites) or len(suites) != len(set(suites))):
        raise ValueError("suites must be a nonempty, unique list of known suites")
    if not isinstance(requested, dict) or any(s not in suites for s in requested):
        raise ValueError("queries must map selected suites to nonempty query-name lists")
    for names in requested.values():
        if (not isinstance(names, list) or not names or
                any(not isinstance(name, str) for name in names) or len(set(names)) != len(names)):
            raise ValueError("Query selections must be nonempty, unique name lists")
    generator_files = [Path(__file__).with_name(name) for name in
                       ("generate_data.py", "tpch_generate.py", "tpch_iceberg.py", "lib.py")]
    generator_hash = hashlib.sha256(b"".join(p.read_bytes() for p in generator_files)).hexdigest()
    campaign = {"suites": suites, "selection": {"suites": suites, "queries": requested},
                "partial": set(suites) != set(SUITES), "parameters": PARAMETERS,
                "catalog": {}, "query_sets": {}, "assignments": {}, "matrix": [], "data_matrix": []}
    for suite in suites:
        catalog = {str(r): query_catalog(suite, r) for r in range(int(PARAMETERS["rounds"]))}
        full_names = {q["query"] for queries in catalog.values() for q in queries}
        if suite in requested and not set(requested[suite]) <= full_names:
            raise ValueError(f"Unknown query names for {suite}: {sorted(set(requested[suite]) - full_names)}")
        selected = {r: [q["query"] for q in queries if suite not in requested or q["query"] in requested[suite]]
                    for r, queries in catalog.items()}
        campaign["partial"] |= any(len(selected[r]) != len(catalog[r]) for r in catalog)
        shards = min(SUITES[suite]["shards"], max(len(names) for names in selected.values()))
        assignments = {r: partition_queries(names, shards, suite) for r, names in selected.items()}
        parameters = {**PARAMETERS, "suite": suite, "rows": SUITES[suite]["rows"],
                      "spark": "3.5.9", "iceberg": "1.8.1", "duckdb": "1.4.4", "java": "17",
                      "master": "local[4]", "driver_memory": "7g", "shuffle_partitions": "16",
                      "warehouse": "/mnt/pruning-bench/warehouse"}
        # Measurement and selection parameters never affect the generated snapshot.
        for name in ("rounds", "warmups", "fuzz_queries"):
            parameters.pop(name)
        params_hash = hashlib.sha256(json.dumps(parameters, sort_keys=True).encode()).hexdigest()
        item = {"suite": suite, **SUITES[suite], "shards": shards,
                "data_key": f"iceberg-data-v1-{generator_hash}-{suite}-{parameters['rows']}-{params_hash}"}
        campaign["catalog"][suite] = catalog
        campaign["query_sets"][suite] = selected
        campaign["assignments"][suite] = assignments
        campaign["data_matrix"].append(item)
        campaign["matrix"].extend({**item, "shard": shard} for shard in range(shards))
    if len(campaign["matrix"]) > 18:
        raise ValueError("Campaign exceeds the 18-job concurrency budget")
    return campaign


def select_queries(queries, suite, round_number):
    if "BENCH_SHARD" not in os.environ:
        return queries
    resolved = json.loads(Path(env("BENCH_RESOLVED", "resolved/revisions.json")).read_text())
    campaign = resolved["campaign"]
    shard, shards = int(os.environ["BENCH_SHARD"]), int(os.environ["BENCH_SHARDS"])
    groups = campaign["assignments"][suite][str(round_number)]
    if len(groups) != shards or not 0 <= shard < shards:
        raise ValueError("Shard identity differs from resolved campaign")
    actual = {name: signature(sql, confs) for name, sql, _, confs in queries}
    canonical = {q["query"]: q["sql_sha256"] for q in campaign["catalog"][suite][str(round_number)]}
    if actual != canonical:
        raise ValueError(f"Query catalog differs from resolver: {suite}/{round_number}")
    selected = set(groups[shard])
    return [q for q in queries if q[0] in selected]


# Cost weights from run 37703972798; timing samples remain untouched by partitioning.
QUERY_WEIGHTS = {
    "join": {
        "equality_delete_scan__f_eq_deletes": 110436,
        "equality_delete_scan__f_eq_deletes_str": 417556,
        "join_anti__f_sorted__dim_128": 38436,
        "join_extra_predicate__f_sorted__dim_128": 159126,
        "join_grouped__f_sorted__dim_128": 44030,
        "join_inner__f_date__dim_10k": 115133,
        "join_inner__f_date__dim_128": 115785,
        "join_inner__f_date__dim_128_spread": 128608,
        "join_inner__f_dec__dim_10k": 166294,
        "join_inner__f_dec__dim_128": 161586,
        "join_inner__f_dec__dim_128_spread": 164089,
        "join_inner__f_eq_deletes__dim_128": 112143,
        "join_inner__f_eq_deletes__dim_128_spread": 114364,
        "join_inner__f_eq_deletes_str__dim_128": 421335,
        "join_inner__f_eq_deletes_str__dim_128_spread": 423326,
        "join_inner__f_long__dim_10k": 118481,
        "join_inner__f_long__dim_128": 119923,
        "join_inner__f_long__dim_128_spread": 129505,
        "join_inner__f_nan__dim_10k": 183830,
        "join_inner__f_nan__dim_128": 179532,
        "join_inner__f_nan__dim_128_spread": 181352,
        "join_inner__f_nulls__dim_10k": 137137,
        "join_inner__f_nulls__dim_128": 135276,
        "join_inner__f_nulls__dim_128_spread": 142705,
        "join_inner__f_pos_deletes__dim_10k": 124577,
        "join_inner__f_pos_deletes__dim_10pct": 147971,
        "join_inner__f_pos_deletes__dim_128": 124364,
        "join_inner__f_pos_deletes__dim_128_spread": 133236,
        "join_inner__f_pos_deletes__dim_1pct": 126936,
        "join_inner__f_pos_deletes__dim_all": 275080,
        "join_inner__f_pos_deletes__dim_empty": 13883,
        "join_inner__f_sorted__dim_1": 119813,
        "join_inner__f_sorted__dim_10k": 121283,
        "join_inner__f_sorted__dim_10k_spread": 163117,
        "join_inner__f_sorted__dim_10pct": 139834,
        "join_inner__f_sorted__dim_128": 119054,
        "join_inner__f_sorted__dim_128_high": 119439,
        "join_inner__f_sorted__dim_128_low": 119950,
        "join_inner__f_sorted__dim_128_spread": 127627,
        "join_inner__f_sorted__dim_1pct": 125058,
        "join_inner__f_sorted__dim_50pct": 201156,
        "join_inner__f_sorted__dim_all": 277480,
        "join_inner__f_sorted__dim_empty": 13241,
        "join_inner__f_sorted__dim_two_ranges": 122872,
        "join_inner__f_str__dim_10k": 172384,
        "join_inner__f_str__dim_128": 170732,
        "join_inner__f_str__dim_128_spread": 168959,
        "join_inner__f_unsorted__dim_10k": 151434,
        "join_inner__f_unsorted__dim_10pct": 179313,
        "join_inner__f_unsorted__dim_128": 127181,
        "join_inner__f_unsorted__dim_128_spread": 131457,
        "join_inner__f_unsorted__dim_1pct": 153682,
        "join_inner__f_unsorted__dim_all": 305163,
        "join_inner__f_unsorted__dim_empty": 13047,
        "join_semi__f_pos_deletes__dim_128": 37244,
        "join_semi__f_sorted__dim_128": 31943,
        "join_shuffled_hash__f_sorted__dim_128": 459285,
        "join_sort_merge__f_sorted__dim_128": 599619,
        "join_star__f_sorted__dim_128": 127399,
    },
    "topk_minmax": {
        "both__f_date": 13031,
        "both__f_eq_deletes": 155311,
        "both__f_eq_deletes_str": 691758,
        "both__f_long": 32946,
        "both__f_nan": 45340,
        "both__f_nulls": 22846,
        "both__f_overlap": 18088,
        "both__f_pos_deletes": 22794,
        "both__f_reversed_files": 21772,
        "both__f_sorted": 18013,
        "both__f_str": 273208,
        "both__f_unsorted": 18186,
        "count_all__f_sorted": 31614,
        "equality_delete_scan__f_eq_deletes": 157640,
        "equality_delete_scan__f_eq_deletes_str": 678700,
        "max__f_eq_deletes": 155149,
        "max__f_eq_deletes_str": 688664,
        "max__f_overlap": 15650,
        "max__f_pos_deletes": 18953,
        "max__f_reversed_files": 18293,
        "max__f_sorted": 15535,
        "max__f_unsorted": 16068,
        "max__f_unsorted__filtered": 49512,
        "min__f_eq_deletes": 156524,
        "min__f_eq_deletes_str": 687051,
        "min__f_overlap": 16160,
        "min__f_pos_deletes": 19575,
        "min__f_reversed_files": 22795,
        "min__f_sorted": 15717,
        "min__f_sorted__filtered": 46417,
        "min__f_unsorted": 15969,
        "min_grouped__f_sorted": 41720,
        "topk1__f_overlap": 153724,
        "topk1__f_pos_deletes": 162243,
        "topk1__f_reversed_files": 256826,
        "topk1__f_sorted": 155022,
        "topk1__f_unsorted": 157510,
        "topk10__f_eq_deletes": 165236,
        "topk10__f_eq_deletes_str": 689113,
        "topk10__f_long": 159440,
        "topk10__f_overlap": 156274,
        "topk10__f_pos_deletes": 160842,
        "topk10__f_reversed_files": 260333,
        "topk10__f_sorted": 155248,
        "topk10__f_str": 220917,
        "topk10__f_unsorted": 162583,
        "topk10_by_id1__f_sorted": 205213,
        "topk10_by_payload__f_unsorted": 209536,
        "topk10_by_value__f_sorted": 155231,
        "topk10_desc__f_eq_deletes": 175400,
        "topk10_desc__f_eq_deletes_str": 702867,
        "topk10_desc__f_long": 298376,
        "topk10_desc__f_overlap": 203396,
        "topk10_desc__f_pos_deletes": 300665,
        "topk10_desc__f_reversed_files": 284876,
        "topk10_desc__f_sorted": 298165,
        "topk10_desc__f_str": 417287,
        "topk10_desc__f_unsorted": 163344,
        "topk10_desc_nulls_first_two_keys__f_nulls": 203247,
        "topk10_desc_two_keys__f_date": 157660,
        "topk10_desc_two_keys__f_nan": 223088,
        "topk10_desc_two_keys__f_nulls": 299297,
        "topk10_nulls_last_two_keys__f_nulls": 163420,
        "topk10_offset__f_sorted": 155096,
        "topk10_two_keys__f_date": 143534,
        "topk10_two_keys__f_nan": 220189,
        "topk10_two_keys__f_nulls": 205551,
        "topk10_two_keys__f_reversed_files": 259997,
        "topk10_two_keys__f_sorted": 155099,
        "topk10_two_keys__f_unsorted": 163646,
        "topk100_by_k2id_two_keys__f_sorted": 199971,
        "topk100_filtered__f_sorted": 221229,
        "topk100_filtered__f_unsorted": 227329,
        "topk100_id_only__f_sorted": 19529,
        "topk1000__f_overlap": 157248,
        "topk1000__f_pos_deletes": 160299,
        "topk1000__f_reversed_files": 260127,
        "topk1000__f_sorted": 155350,
        "topk1000__f_unsorted": 215071,
        "topk1000_desc__f_sorted": 383633,
        "topk100000__f_overlap": 274738,
        "topk100000__f_pos_deletes": 246460,
        "topk100000__f_reversed_files": 393599,
        "topk100000__f_sorted": 235364,
        "topk100000__f_unsorted": 839193,
    },
    "layouts": {
        "count_all__f_big": 28290,
        "count_all__f_evolved": 12000,
        "count_all__f_many_files": 20247,
        "count_all__f_part_evolved": 15633,
        "count_all__f_partitioned": 12031,
        "count_all__f_skewed": 12448,
        "count_all__f_small_rg": 19719,
        "count_all__f_snapshots": 29636,
        "count_all__f_sorted": 11382,
        "count_all__f_updated": 18737,
        "count_all__f_wide": 17131,
        "join_inner__f_big__dim_10k": 140618,
        "join_inner__f_big__dim_128": 139106,
        "join_inner__f_evolved__dim_10k": 43125,
        "join_inner__f_evolved__dim_128": 42751,
        "join_inner__f_many_files__dim_10k": 57749,
        "join_inner__f_many_files__dim_128": 57266,
        "join_inner__f_part_evolved__dim_10k": 48912,
        "join_inner__f_part_evolved__dim_128": 49940,
        "join_inner__f_partitioned__dim_10k": 42336,
        "join_inner__f_partitioned__dim_128": 43149,
        "join_inner__f_skewed__dim_10k": 43978,
        "join_inner__f_skewed__dim_128": 44333,
        "join_inner__f_small_rg__dim_10k": 52548,
        "join_inner__f_small_rg__dim_128": 52350,
        "join_inner__f_snapshots__dim_10k": 73531,
        "join_inner__f_snapshots__dim_128": 73252,
        "join_inner__f_sorted__dim_10k": 42715,
        "join_inner__f_sorted__dim_128": 42157,
        "join_inner__f_updated__dim_10k": 56196,
        "join_inner__f_updated__dim_128": 56376,
        "join_inner__f_wide__dim_10k": 48149,
        "join_inner__f_wide__dim_128": 48279,
        "join_wide_columns__f_wide__dim_128": 114956,
        "max__f_big": 12860,
        "max__f_evolved": 10166,
        "max__f_many_files": 16381,
        "max__f_part_evolved": 11656,
        "max__f_partitioned": 8171,
        "max__f_skewed": 7817,
        "max__f_small_rg": 15782,
        "max__f_snapshots": 26850,
        "max__f_sorted": 8373,
        "max__f_updated": 14939,
        "max__f_wide": 12964,
        "min__f_big": 13090,
        "min__f_evolved": 10324,
        "min__f_many_files": 16222,
        "min__f_part_evolved": 11801,
        "min__f_partitioned": 8397,
        "min__f_skewed": 7801,
        "min__f_small_rg": 15942,
        "min__f_snapshots": 26240,
        "min__f_sorted": 8226,
        "min__f_updated": 15311,
        "min__f_wide": 13465,
        "static_range__f_big": 8007,
        "static_range__f_evolved": 8891,
        "static_range__f_many_files": 7129,
        "static_range__f_part_evolved": 10074,
        "static_range__f_partitioned": 7184,
        "static_range__f_skewed": 9792,
        "static_range__f_small_rg": 8739,
        "static_range__f_snapshots": 12461,
        "static_range__f_sorted": 7243,
        "static_range__f_updated": 11387,
        "static_range__f_wide": 7854,
        "topk10__f_big": 145553,
        "topk10__f_evolved": 42080,
        "topk10__f_many_files": 68059,
        "topk10__f_part_evolved": 47701,
        "topk10__f_partitioned": 41616,
        "topk10__f_small_rg": 49772,
        "topk10__f_snapshots": 88553,
        "topk10__f_sorted": 40762,
        "topk10__f_wide": 45696,
        "topk10_two_keys__f_skewed": 43320,
        "topk10_two_keys__f_updated": 54321,
        "topk1000_desc__f_big": 391555,
        "topk1000_desc__f_evolved": 89521,
        "topk1000_desc__f_many_files": 98004,
        "topk1000_desc__f_part_evolved": 68924,
        "topk1000_desc__f_partitioned": 71025,
        "topk1000_desc__f_small_rg": 114836,
        "topk1000_desc__f_snapshots": 108292,
        "topk1000_desc__f_sorted": 103025,
        "topk1000_desc__f_wide": 107517,
        "topk1000_desc_two_keys__f_skewed": 109026,
        "topk1000_desc_two_keys__f_updated": 107038,
    },
    "tpch": {
        "tpch_clu__q01": 270751,
        "tpch_clu__q02": 103488,
        "tpch_clu__q03": 213550,
        "tpch_clu__q04": 162533,
        "tpch_clu__q05": 484929,
        "tpch_clu__q06": 16658,
        "tpch_clu__q07": 249370,
        "tpch_clu__q08": 150711,
        "tpch_clu__q09": 300782,
        "tpch_clu__q10": 175394,
        "tpch_clu__q11": 54960,
        "tpch_clu__q12": 108602,
        "tpch_clu__q13": 146950,
        "tpch_clu__q14": 31049,
        "tpch_clu__q15_1": 44153,
        "tpch_clu__q16": 79826,
        "tpch_clu__q17": 225553,
        "tpch_clu__q18": 584194,
        "tpch_clu__q19": 127207,
        "tpch_clu__q20": 57998,
        "tpch_clu__q21": 1259678,
        "tpch_clu__q22": 80785,
        "tpch_nat__q01": 282426,
        "tpch_nat__q02": 109917,
        "tpch_nat__q03": 206654,
        "tpch_nat__q04": 129718,
        "tpch_nat__q05": 378101,
        "tpch_nat__q06": 45923,
        "tpch_nat__q07": 252868,
        "tpch_nat__q08": 162028,
        "tpch_nat__q09": 291161,
        "tpch_nat__q10": 206438,
        "tpch_nat__q11": 63353,
        "tpch_nat__q12": 133798,
        "tpch_nat__q13": 140363,
        "tpch_nat__q14": 72444,
        "tpch_nat__q15_1": 124425,
        "tpch_nat__q16": 89043,
        "tpch_nat__q17": 229380,
        "tpch_nat__q18": 406626,
        "tpch_nat__q19": 127873,
        "tpch_nat__q20": 98900,
        "tpch_nat__q21": 1109274,
        "tpch_nat__q22": 85602,
    },
    "strjoin": {
        "strjoin_int_broadcast_distinct_1000": 17885,
        "strjoin_int_broadcast_distinct_10000": 18679,
        "strjoin_int_broadcast_distinct_100000": 25327,
        "strjoin_int_broadcast_distinct_1000000": 67126,
        "strjoin_int_broadcast_distinct_3000000": 146172,
        "strjoin_int_broadcast_distinct_40000": 21056,
        "strjoin_int_broadcast_distinct_500000": 41633,
        "strjoin_int_broadcast_plain_1000": 18023,
        "strjoin_int_broadcast_plain_10000": 20326,
        "strjoin_int_broadcast_plain_100000": 26149,
        "strjoin_int_broadcast_plain_1000000": 113132,
        "strjoin_int_broadcast_plain_3000000": 276880,
        "strjoin_int_broadcast_plain_40000": 24311,
        "strjoin_int_broadcast_plain_500000": 70420,
        "strjoin_int_shuffled_hash_distinct_1000": 24539,
        "strjoin_int_shuffled_hash_distinct_10000": 26362,
        "strjoin_int_shuffled_hash_distinct_100000": 30591,
        "strjoin_int_shuffled_hash_distinct_1000000": 61289,
        "strjoin_int_shuffled_hash_distinct_3000000": 127651,
        "strjoin_int_shuffled_hash_distinct_40000": 27904,
        "strjoin_int_shuffled_hash_distinct_500000": 42418,
        "strjoin_int_shuffled_hash_plain_1000": 24419,
        "strjoin_int_shuffled_hash_plain_10000": 23571,
        "strjoin_int_shuffled_hash_plain_100000": 25270,
        "strjoin_int_shuffled_hash_plain_1000000": 43062,
        "strjoin_int_shuffled_hash_plain_3000000": 82247,
        "strjoin_int_shuffled_hash_plain_40000": 23678,
        "strjoin_int_shuffled_hash_plain_500000": 34353,
        "strjoin_int_sort_merge_distinct_1000": 32589,
        "strjoin_int_sort_merge_distinct_10000": 32867,
        "strjoin_int_sort_merge_distinct_100000": 38613,
        "strjoin_int_sort_merge_distinct_1000000": 70608,
        "strjoin_int_sort_merge_distinct_3000000": 139104,
        "strjoin_int_sort_merge_distinct_40000": 34851,
        "strjoin_int_sort_merge_distinct_500000": 54726,
        "strjoin_int_sort_merge_plain_1000": 32528,
        "strjoin_int_sort_merge_plain_10000": 32388,
        "strjoin_int_sort_merge_plain_100000": 34566,
        "strjoin_int_sort_merge_plain_1000000": 58718,
        "strjoin_int_sort_merge_plain_3000000": 93287,
        "strjoin_int_sort_merge_plain_40000": 32766,
        "strjoin_int_sort_merge_plain_500000": 43278,
        "strjoin_long_broadcast_distinct_1000": 22659,
        "strjoin_long_broadcast_distinct_10000": 23822,
        "strjoin_long_broadcast_distinct_100000": 33198,
        "strjoin_long_broadcast_distinct_1000000": 139798,
        "strjoin_long_broadcast_distinct_3000000": 357011,
        "strjoin_long_broadcast_distinct_40000": 28326,
        "strjoin_long_broadcast_distinct_500000": 80990,
        "strjoin_long_broadcast_plain_1000": 20829,
        "strjoin_long_broadcast_plain_10000": 20580,
        "strjoin_long_broadcast_plain_100000": 27922,
        "strjoin_long_broadcast_plain_1000000": 113848,
        "strjoin_long_broadcast_plain_3000000": 273201,
        "strjoin_long_broadcast_plain_40000": 22990,
        "strjoin_long_broadcast_plain_500000": 67140,
        "strjoin_long_shuffled_hash_distinct_1000": 34018,
        "strjoin_long_shuffled_hash_distinct_10000": 33396,
        "strjoin_long_shuffled_hash_distinct_100000": 39520,
        "strjoin_long_shuffled_hash_distinct_1000000": 91211,
        "strjoin_long_shuffled_hash_distinct_3000000": 176617,
        "strjoin_long_shuffled_hash_distinct_40000": 34701,
        "strjoin_long_shuffled_hash_distinct_500000": 53520,
        "strjoin_long_shuffled_hash_plain_1000": 31516,
        "strjoin_long_shuffled_hash_plain_10000": 31240,
        "strjoin_long_shuffled_hash_plain_100000": 32858,
        "strjoin_long_shuffled_hash_plain_1000000": 51144,
        "strjoin_long_shuffled_hash_plain_3000000": 95634,
        "strjoin_long_shuffled_hash_plain_40000": 31966,
        "strjoin_long_shuffled_hash_plain_500000": 41829,
        "strjoin_long_sort_merge_distinct_1000": 42792,
        "strjoin_long_sort_merge_distinct_10000": 42784,
        "strjoin_long_sort_merge_distinct_100000": 48919,
        "strjoin_long_sort_merge_distinct_1000000": 97453,
        "strjoin_long_sort_merge_distinct_3000000": 184156,
        "strjoin_long_sort_merge_distinct_40000": 43136,
        "strjoin_long_sort_merge_distinct_500000": 68373,
        "strjoin_long_sort_merge_plain_1000": 40100,
        "strjoin_long_sort_merge_plain_10000": 40989,
        "strjoin_long_sort_merge_plain_100000": 43923,
        "strjoin_long_sort_merge_plain_1000000": 63120,
        "strjoin_long_sort_merge_plain_3000000": 103536,
        "strjoin_long_sort_merge_plain_40000": 42556,
        "strjoin_long_sort_merge_plain_500000": 54368,
        "strjoin_str_broadcast_distinct_1000": 25424,
        "strjoin_str_broadcast_distinct_10000": 28761,
        "strjoin_str_broadcast_distinct_100000": 41538,
        "strjoin_str_broadcast_distinct_1000000": 218827,
        "strjoin_str_broadcast_distinct_3000000": 163974,
        "strjoin_str_broadcast_distinct_40000": 32620,
        "strjoin_str_broadcast_distinct_500000": 110945,
        "strjoin_str_broadcast_plain_1000": 49419,
        "strjoin_str_broadcast_plain_10000": 48674,
        "strjoin_str_broadcast_plain_100000": 62655,
        "strjoin_str_broadcast_plain_1000000": 205966,
        "strjoin_str_broadcast_plain_3000000": 460385,
        "strjoin_str_broadcast_plain_40000": 53654,
        "strjoin_str_broadcast_plain_500000": 136520,
        "strjoin_str_shuffled_hash_distinct_1000": 42643,
        "strjoin_str_shuffled_hash_distinct_10000": 44503,
        "strjoin_str_shuffled_hash_distinct_100000": 49715,
        "strjoin_str_shuffled_hash_distinct_1000000": 119376,
        "strjoin_str_shuffled_hash_distinct_3000000": 236108,
        "strjoin_str_shuffled_hash_distinct_40000": 49078,
        "strjoin_str_shuffled_hash_distinct_500000": 82104,
        "strjoin_str_shuffled_hash_plain_1000": 42940,
        "strjoin_str_shuffled_hash_plain_10000": 43144,
        "strjoin_str_shuffled_hash_plain_100000": 48123,
        "strjoin_str_shuffled_hash_plain_1000000": 89582,
        "strjoin_str_shuffled_hash_plain_3000000": 175550,
        "strjoin_str_shuffled_hash_plain_40000": 44397,
        "strjoin_str_shuffled_hash_plain_500000": 64273,
        "strjoin_str_sort_merge_distinct_1000": 60846,
        "strjoin_str_sort_merge_distinct_10000": 62196,
        "strjoin_str_sort_merge_distinct_100000": 69843,
        "strjoin_str_sort_merge_distinct_1000000": 124388,
        "strjoin_str_sort_merge_distinct_3000000": 258394,
        "strjoin_str_sort_merge_distinct_40000": 68884,
        "strjoin_str_sort_merge_distinct_500000": 94156,
        "strjoin_str_sort_merge_plain_1000": 58768,
        "strjoin_str_sort_merge_plain_10000": 59319,
        "strjoin_str_sort_merge_plain_100000": 64110,
        "strjoin_str_sort_merge_plain_1000000": 102585,
        "strjoin_str_sort_merge_plain_3000000": 186458,
        "strjoin_str_sort_merge_plain_40000": 59736,
        "strjoin_str_sort_merge_plain_500000": 82980,
    },
}

# Williams design: each native variant occupies every position once and each ordered
# adjacent pair appears once. All timed variants warm every query in their own JVM.
ORDERS = [
    ["baseline_off", "baseline_on", "candidate_on", "candidate_off"],
    ["baseline_on", "candidate_off", "baseline_off", "candidate_on"],
    ["candidate_off", "candidate_on", "baseline_on", "baseline_off"],
    ["candidate_on", "baseline_off", "candidate_off", "baseline_on"],
]


def arguments(variant):
    args = ["spark-submit", "--master", "local[4]", "--driver-memory", "7g"]
    settings = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.bench": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.bench.type": "hadoop",
        "spark.sql.catalog.bench.warehouse": os.environ["WAREHOUSE"],
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.shuffle.partitions": "4",
        "spark.sql.iceberg.aggregate-push-down.enabled": "false",
        "spark.ui.enabled": "false",
        "spark.sql.session.timeZone": "UTC",
    }
    if variant == "spark":
        args.extend(["--jars", "jars/iceberg.jar"])
    else:
        side, mode = variant.split("_")
        jar = str(Path(f"jars/pruning-{side}/comet.jar").resolve())
        if not Path(jar).is_file():
            raise FileNotFoundError(jar)
        enabled = "true" if mode == "on" else "false"
        args.extend(["--jars", f"jars/iceberg.jar,{jar}"])
        settings.update(
            {
                "spark.driver.extraClassPath": jar,
                "spark.executor.extraClassPath": jar,
                "spark.plugins": "org.apache.spark.CometPlugin",
                "spark.shuffle.manager": "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager",
                "spark.comet.exec.shuffle.enabled": "true",
                "spark.memory.offHeap.enabled": "true",
                "spark.memory.offHeap.size": "5g",
                "spark.comet.scan.icebergNative.enabled": "true",
                "spark.comet.expression.Cast.allowIncompatible": "true",
                "spark.comet.explain.fallback.enabled": "true",
                # Keep the same Top-K execution algorithm when switching pruning off.
                "spark.comet.exec.topK.fusion.enabled": "true",
                "spark.comet.exec.join.dynamicFilter.enabled": enabled,
                "spark.comet.exec.topK.dynamicFilter.enabled": enabled,
                "spark.comet.exec.aggregate.dynamicFilter.enabled": enabled,
            }
        )
    for key, value in settings.items():
        args.extend(["--conf", f"{key}={value}"])
    suite = os.environ["BENCH_SUITE"]
    script = "run_tpch.py" if suite == "tpch" else "run_queries.py"
    args.append(f"benchmarks/iceberg-runtime-pruning-detailed/{script}")
    return args


def launch(variant, round_number):
    suite = os.environ["BENCH_SUITE"]
    child_env = {
        **os.environ,
        "BENCH_VARIANT": variant,
        "BENCH_ROUND": str(round_number),
        "BENCH_SEED": str(round_number + 1),
    }
    suffix = f"-shard-{os.environ['BENCH_SHARD']}" if "BENCH_SHARD" in os.environ else ""
    path = Path(f"run-{suite}{suffix}-{round_number}-{variant}.log")
    print(f"{suite}: round {round_number}, {variant}", flush=True)
    with path.open("w", encoding="utf-8") as out:
        result = subprocess.run(
            arguments(variant), env=child_env, stdout=out, stderr=subprocess.STDOUT
        )
    if result.returncode:
        print(f"Failed: {path}; exit={result.returncode}", flush=True)
        with path.open(encoding="utf-8") as source:
            tail = source.readlines()[-100:]
        print("".join(tail), flush=True)
    return result.returncode == 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment", type=Path)
    parser.add_argument("--data-manifest", type=Path)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    if args.environment:
        metadata = runner_environment()
        if "DATA_CACHE_KEY" in os.environ:
            metadata["data_cache_key"] = os.environ["DATA_CACHE_KEY"]
            metadata["data_cache_hit"] = os.environ["DATA_CACHE_HIT"] == "true"
        args.environment.write_text(json.dumps(metadata, indent=2) + "\n")
        return 0
    if args.data_manifest:
        entries = data_manifest(Path(os.environ["WAREHOUSE"]))
        if args.verify:
            if entries != json.loads(args.data_manifest.read_text()):
                raise ValueError("Iceberg snapshot changed: cached/generated file bytes differ")
        else:
            args.data_manifest.write_text(json.dumps(entries, indent=2) + "\n")
        return 0
    rounds = int(env("BENCH_ROUNDS", "4"))
    if rounds < len(ORDERS) or rounds % len(ORDERS):
        raise ValueError(
            "This benchmark requires a positive multiple of four balanced JVM rounds"
        )
    suite = os.environ["BENCH_SUITE"]
    passed = True
    for round_number in range(rounds):
        order = ORDERS[round_number % len(ORDERS)]
        if round_number == 0 or suite == "fuzz":
            passed &= launch("spark", round_number)
        for variant in order:
            passed &= launch(variant, round_number)
    return int(not passed)


if __name__ == "__main__":
    sys.exit(main())
