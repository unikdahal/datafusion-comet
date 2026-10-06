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

"""Run one suite of the detailed runtime pruning benchmark in one Spark JVM.

The variant is described by environment variables; the Comet jar and the Comet settings are
supplied by spark-submit. For the timed suites each query is warmed once, then timed BENCH_REPS
times in a rotated order; the fuzz suite runs every generated query once and exists to compare
results across variants. Planning and execution are timed separately.
"""

import random

from pyspark.sql import SparkSession

from lib import append, env, run_query, typed

SUITE = env("BENCH_SUITE", "join")
VARIANT = env("BENCH_VARIANT", "x")
ROUND = int(env("BENCH_ROUND", "0"))
REPS = int(env("BENCH_REPS", "2"))
OUTPUT = env("BENCH_OUTPUT", "results.jsonl")
ROWS = int(env("BENCH_ROWS", "16000000"))
FUZZ_ROWS = int(env("BENCH_FUZZ_ROWS", "4000000"))
FUZZ_QUERIES = int(env("BENCH_FUZZ_QUERIES", "400"))
SEED = int(env("BENCH_SEED", "1"))

T = "bench.db."
ALL_DIMS = [
    "dim_empty", "dim_1", "dim_128", "dim_128_spread", "dim_128_low", "dim_128_high", "dim_two_ranges",
    "dim_10k", "dim_10k_spread", "dim_1pct", "dim_10pct", "dim_50pct", "dim_all",
]
SOME_DIMS = ["dim_empty", "dim_128", "dim_128_spread", "dim_10k", "dim_1pct", "dim_10pct", "dim_all"]

# (name, sql, ordered, confs)


def dim_sql(dim, ktype):
    base = f"{T}{dim}"
    if ktype == "nan":
        return f"SELECT {typed('nan', 'id')} AS id FROM {base} UNION ALL SELECT double('NaN') AS id"
    if ktype in ("date", "str"):
        return f"SELECT DISTINCT {typed(ktype, 'id')} AS id FROM {base}"
    return f"SELECT {typed(ktype, 'id')} AS id FROM {base}"


def join(table, ktype, dim, form="inner"):
    d = f"({dim_sql(dim, ktype)}) d"
    f = f"{T}{table} f"
    sums = "count(*), sum(f.value), sum(length(f.payload))"
    sql = {
        "inner": f"SELECT /*+ BROADCAST(d) */ {sums} FROM {f} JOIN {d} ON f.id = d.id",
        "semi": f"SELECT /*+ BROADCAST(d) */ count(*), sum(f.value) FROM {f} LEFT SEMI JOIN {d} ON f.id = d.id",
        "anti": f"SELECT /*+ BROADCAST(d) */ count(*), sum(f.value) FROM {f} LEFT ANTI JOIN {d} ON f.id = d.id",
        "shuffled_hash": f"SELECT /*+ SHUFFLE_HASH(d) */ {sums} FROM {f} JOIN {d} ON f.id = d.id",
        "sort_merge": f"SELECT /*+ MERGE(d) */ {sums} FROM {f} JOIN {d} ON f.id = d.id",
        "grouped": (
            f"SELECT /*+ BROADCAST(d) */ f.k2 % 10 AS g, count(*), sum(f.value) FROM {f} JOIN {d} "
            "ON f.id = d.id GROUP BY f.k2 % 10 ORDER BY g"
        ),
        "extra_predicate": f"SELECT /*+ BROADCAST(d) */ {sums} FROM {f} JOIN {d} ON f.id = d.id AND f.value % 7 = 0",
        "star": (
            f"SELECT /*+ BROADCAST(d), BROADCAST(d2) */ {sums} FROM {f} JOIN {d} ON f.id = d.id "
            f"JOIN {T}dim_k2 d2 ON f.k2 = d2.k2"
        ),
    }[form]
    ordered = form == "grouped"
    return (f"join_{form}__{table}__{dim}", sql, ordered, None)


def topk(table, k, desc=False, nulls=None, extra="", cols="id, value, payload", offset=0, key="id"):
    direction = " DESC" if desc else ""
    placement = f" NULLS {nulls}" if nulls else ""
    tail = f" OFFSET {offset}" if offset else ""
    where = f" WHERE {extra}" if extra else ""
    label = f"topk{k}{'_desc' if desc else ''}{'_nulls_' + nulls.lower() if nulls else ''}"
    for tag, present in (
        ("by_" + "".join(c for c in key if c.isalnum()), key != "id"),
        ("filtered", bool(extra)),
        ("offset", bool(offset)),
        ("id_only", cols == "id"),
    ):
        if present:
            label += "_" + tag
    sql = f"SELECT {cols} FROM {T}{table}{where} ORDER BY {key}{direction}{placement}, value LIMIT {k}{tail}"
    return (f"{label}__{table}", sql, True, None)


def minmax(table, kind, extra=""):
    exprs = {"min": "min(id)", "max": "max(id)", "both": "min(id), max(id)"}[kind]
    where = f" WHERE {extra}" if extra else ""
    return (f"{kind}__{table}{'__filtered' if extra else ''}", f"SELECT {exprs} FROM {T}{table}{where}", True, None)


def join_suite():
    queries = []
    for table, dims in [("f_sorted", ALL_DIMS), ("f_unsorted", SOME_DIMS), ("f_pos_deletes", SOME_DIMS)]:
        queries += [join(table, "int", d) for d in dims]
    for form in ["semi", "anti", "shuffled_hash", "sort_merge", "grouped", "extra_predicate", "star"]:
        queries.append(join("f_sorted", "int", "dim_128", form))
    queries.append(join("f_pos_deletes", "int", "dim_128", "semi"))
    for ktype in ["long", "str", "date", "dec", "nulls", "nan"]:
        for dim in ["dim_128", "dim_10k", "dim_128_spread"]:
            queries.append(join(f"f_{ktype}", ktype, dim))
    return queries


def topk_minmax_suite():
    queries = []
    for table in ["f_sorted", "f_unsorted", "f_pos_deletes", "f_reversed_files", "f_overlap"]:
        queries += [topk(table, k) for k in (1, 10, 1000, 100000)]
        queries.append(topk(table, 10, desc=True))
        queries += [minmax(table, kind) for kind in ("min", "max", "both")]
    queries.append(topk("f_sorted", 1000, desc=True))
    for ktype in ["long", "str", "date", "nulls", "nan"]:
        table = f"f_{ktype}"
        queries += [topk(table, 10), topk(table, 10, desc=True), minmax(table, "both")]
    queries += [topk("f_nulls", 10, nulls="LAST"), topk("f_nulls", 10, desc=True, nulls="FIRST")]
    queries += [
        topk("f_sorted", 100, cols="id"),
        topk("f_sorted", 10, offset=1000),
        topk("f_sorted", 100, extra="value % 3 = 0"),
        topk("f_unsorted", 100, extra="value % 3 = 0"),
        topk("f_sorted", 100, key="k2, id"),
        topk("f_sorted", 10, key="value"),
        # No pruning possible: the sort key is an expression or an unordered string.
        topk("f_sorted", 10, key="id + 1"),
        topk("f_unsorted", 10, key="payload"),
        minmax("f_sorted", "min", "value % 3 = 0"),
        minmax("f_unsorted", "max", "value % 3 = 0"),
        (
            "min_grouped__f_sorted",
            f"SELECT k2 % 10 AS g, min(id) FROM {T}f_sorted GROUP BY k2 % 10 ORDER BY g",
            True,
            None,
        ),
        ("count_all__f_sorted", f"SELECT count(*), sum(value) FROM {T}f_sorted", True, None),
    ]
    return queries


def layouts_suite():
    queries = []
    start = ROWS * 3 // 5
    for table in ["f_sorted", "f_updated", "f_snapshots", "f_evolved", "f_part_evolved", "f_small_rg",
                  "f_many_files", "f_partitioned", "f_wide", "f_skewed", "f_big"]:
        scale = 4 if table == "f_big" else 1
        queries += [
            join(table, "int", "dim_128"),
            join(table, "int", "dim_10k"),
            topk(table, 10),
            topk(table, 1000, desc=True),
            minmax(table, "min"),
            minmax(table, "max"),
            (
                f"static_range__{table}",
                f"SELECT count(*), sum(value) FROM {T}{table} WHERE id >= {start * scale} AND id < {(start + ROWS // 100) * scale}",
                True,
                None,
            ),
            (f"count_all__{table}", f"SELECT count(*), sum(value) FROM {T}{table}", True, None),
        ]
    queries.append(
        (
            "join_wide_columns__f_wide__dim_128",
            f"SELECT /*+ BROADCAST(d) */ count(*), sum(length(f.w0)), sum(length(f.w4)) "
            f"FROM {T}f_wide f JOIN {T}dim_128 d ON f.id = d.id",
            True,
            None,
        )
    )
    return queries


def fuzz_queries():
    rng = random.Random(SEED)
    tables = {
        "fz_sorted": "int", "fz_unsorted": "int", "fz_pos_deletes": "int", "fz_updated": "int",
        "fz_long": "long", "fz_str": "str", "fz_date": "date", "fz_dec": "dec", "fz_nulls": "nulls",
        "fz_nan": "nan", "fz_snapshots": "int", "fz_evolved": "int", "fz_part_evolved": "int",
        "fz_small_rg": "int", "fz_overlap": "int", "fz_reversed_files": "int", "fz_skewed": "int",
    }
    names = sorted(tables)
    rows = FUZZ_ROWS

    def dim():
        kind = rng.choice(["range", "range", "modulo", "points", "empty", "narrow"])
        if kind == "range":
            width = int(rows * 10 ** rng.uniform(-4, 0))
            low = rng.randrange(0, max(rows - width, 1))
            return f"id >= {low} AND id < {low + max(width, 1)}"
        if kind == "narrow":
            low = rng.randrange(0, rows - 300)
            return f"id >= {low} AND id < {low + 256} AND id % 2 = 0"
        if kind == "modulo":
            m = rng.choice([7, 1000, 100000, 1000003])
            return f"id % {m} = {rng.randrange(m)}"
        if kind == "points":
            points = ", ".join(str(rng.randrange(rows)) for _ in range(rng.randint(1, 50)))
            return f"id IN ({points})"
        return "id < 0"

    def dim_from(ktype):
        base = f"SELECT {typed(ktype, 'id')} AS id FROM (SELECT id FROM range({rows}) WHERE {dim()})"
        if ktype == "nan":
            base += " UNION ALL SELECT double('NaN') AS id"
        if ktype in ("date", "str"):
            base = base.replace("SELECT ", "SELECT DISTINCT ", 1)
        return base

    def bound(ktype, value):
        return typed(ktype, str(value))

    queries = []
    for index in range(FUZZ_QUERIES):
        table = rng.choice(names)
        ktype = tables[table]
        kind = rng.choice(["join", "join", "semi", "topk", "topk", "minmax", "join_topk", "join_min", "filtered_topk"])
        t = f"{T}{table}"
        name = f"fuzz{SEED}_{index:04d}_{kind}__{table}"
        if kind == "join":
            sql = f"SELECT /*+ BROADCAST(d) */ count(*), sum(f.value) FROM {t} f JOIN ({dim_from(ktype)}) d ON f.id = d.id"
            queries.append((name, sql, False, None))
        elif kind == "semi":
            sql = f"SELECT /*+ BROADCAST(d) */ count(*), sum(f.value) FROM {t} f LEFT SEMI JOIN ({dim_from(ktype)}) d ON f.id = d.id"
            queries.append((name, sql, False, None))
        elif kind == "topk":
            k = rng.choice([1, 5, 10, 100, 1000, 20000])
            direction = rng.choice(["", " DESC"])
            placement = rng.choice(["", " NULLS FIRST", " NULLS LAST"])
            cols = rng.choice(["id, value", "id"])
            order = f"id{direction}{placement}" + (", value" if cols != "id" else "")
            queries.append((name, f"SELECT {cols} FROM {t} ORDER BY {order} LIMIT {k}", True, None))
        elif kind == "minmax":
            exprs = rng.choice(["min(id)", "max(id)", "min(id), max(id), count(*)"])
            where = rng.choice(["", f" WHERE value % {rng.choice([2, 3, 5])} = 0",
                                f" WHERE id >= {bound(ktype, rng.randrange(rows))}"])
            queries.append((name, f"SELECT {exprs} FROM {t}{where}", True, None))
        elif kind == "join_topk":
            k = rng.choice([1, 10, 100, 1000])
            direction = rng.choice(["", " DESC"])
            sql = (f"SELECT /*+ BROADCAST(d) */ f.id, f.value FROM {t} f JOIN ({dim_from(ktype)}) d ON f.id = d.id "
                   f"ORDER BY f.id{direction}, f.value LIMIT {k}")
            queries.append((name, sql, True, None))
        elif kind == "join_min":
            sql = (f"SELECT /*+ BROADCAST(d) */ min(f.id), max(f.id), count(*) FROM {t} f "
                   f"JOIN ({dim_from(ktype)}) d ON f.id = d.id")
            queries.append((name, sql, True, None))
        else:
            low = rng.randrange(rows)
            k = rng.choice([1, 10, 500])
            direction = rng.choice(["", " DESC"])
            queries.append((
                name,
                f"SELECT id, value FROM {t} WHERE id >= {bound(ktype, low)} ORDER BY id{direction}, value LIMIT {k}",
                True,
                None,
            ))
    return queries


def queries_for(suite):
    return {
        "join": join_suite,
        "topk_minmax": topk_minmax_suite,
        "layouts": layouts_suite,
        "fuzz": fuzz_queries,
    }[suite]()


def main():
    spark = SparkSession.builder.appName(f"detailed-pruning-{SUITE}-{VARIANT}").getOrCreate()
    queries = queries_for(SUITE)
    names = [q[0] for q in queries]
    assert len(names) == len(set(names)), "duplicate query names"
    if SUITE == "fuzz":
        for name, sql, ordered, confs in queries:
            record = run_query(spark, name, sql, ordered, confs)
            record.update({"suite": SUITE, "variant": VARIANT, "round": ROUND, "rep": 0})
            append(OUTPUT, record)
        spark.stop()
        return
    for name, sql, ordered, confs in queries:
        run_query(spark, name, sql, ordered, confs)
    for rep in range(REPS):
        shift = (ROUND * REPS + rep) % len(queries)
        for name, sql, ordered, confs in queries[shift:] + queries[:shift]:
            record = run_query(spark, name, sql, ordered, confs)
            record.update({"suite": SUITE, "variant": VARIANT, "round": ROUND, "rep": rep})
            append(OUTPUT, record)
    spark.stop()


if __name__ == "__main__":
    main()
