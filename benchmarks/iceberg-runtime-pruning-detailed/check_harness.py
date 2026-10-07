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

"""Check the benchmark's correctness gate, coverage gate and statistical unit."""

import json
import decimal
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import lib
import run_suite
import run_tpch
import summarize
import resolve_revisions


class HarnessChecks(unittest.TestCase):
    def test_runtime_diagnostic_unknown_counters_stay_unknown(self):
        self.assertEqual(
            lib.diagnostic_delta(
                {"gc": 3, "jit": None, "reset": 8}, {"gc": 5, "jit": 1, "reset": 2}
            ),
            {"gc": 2, "jit": None, "reset": None},
        )

    def test_query_order_is_shared_reproducible_and_varies_between_passes(self):
        values = list(range(50))
        self.assertEqual(lib.query_order(values, 2, 1), lib.query_order(values, 2, 1))
        self.assertCountEqual(lib.query_order(values, 2, 1), values)
        self.assertNotEqual(
            lib.query_order(values, 2, 1), lib.query_order(values, 2, 2)
        )
        self.assertEqual(values, list(range(50)))

    def test_latest_main_resolver_refuses_a_fork_baseline(self):
        with self.assertRaises(ValueError), patch.object(
            resolve_revisions.subprocess, "run"
        ) as fetch:
            resolve_revisions.resolve({"baseline": {"comet": "a" * 40}})
        fetch.assert_not_called()

    def test_provenance_gate_rejects_missing_changed_and_fork_baselines(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.assertTrue(summarize.validate_provenance(root, ["join"])[0])
            resolved = {
                "baseline": {
                    "repository": resolve_revisions.MAIN_REPOSITORY,
                    "ref": "refs/heads/main",
                    "comet": "a" * 40,
                    "iceberg_dependency": {"name": "iceberg", "source": "main"},
                },
                "candidate": {
                    "comet": "b" * 40,
                    "iceberg_dependency": {"name": "iceberg", "source": "candidate"},
                },
            }
            environment = {
                "resolved_revisions": resolved,
                "builds": {
                    side: {
                        "comet": resolved[side]["comet"],
                        "resolved_revisions": resolved,
                        "dependencies": [resolved[side]["iceberg_dependency"]],
                    }
                    for side in ("baseline", "candidate")
                },
            }
            path = root / "environment-join.json"
            path.write_text(json.dumps(environment))
            self.assertEqual(summarize.validate_provenance(root, ["join"])[0], [])
            changed = json.loads(json.dumps(environment))
            changed["builds"]["baseline"]["comet"] = "c" * 40
            path.write_text(json.dumps(changed))
            self.assertTrue(summarize.validate_provenance(root, ["join"])[0])
            changed = json.loads(json.dumps(environment))
            changed["resolved_revisions"]["baseline"]["repository"] = "fork"
            path.write_text(json.dumps(changed))
            self.assertTrue(summarize.validate_provenance(root, ["join"])[0])

    def test_main_advancement_is_resolved_each_run_with_its_own_dependency(self):
        import subprocess

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def command(*args):
                return subprocess.check_output(
                    ["git", "-C", directory, *args], text=True
                ).strip()

            command("init", "-b", "main")
            command("config", "user.email", "benchmark-test@example.invalid")
            command("config", "user.name", "Benchmark Test")
            (root / "native").mkdir()
            lock = root / "native/Cargo.lock"

            def commit(dependency):
                lock.write_text(
                    f'[[package]]\nname = "iceberg"\nversion = "0.10.0"\nsource = "git+https://example.invalid/iceberg#{dependency}"\n'
                )
                command("add", "native/Cargo.lock")
                command("commit", "-m", dependency)
                return command("rev-parse", "HEAD")

            candidate = commit("a" * 40)
            first_main = commit("b" * 40)
            configuration = {
                "baseline": {
                    "repository": resolve_revisions.MAIN_REPOSITORY,
                    "ref": "refs/heads/main",
                },
                "candidate": {"comet": candidate, "iceberg": "a" * 40},
            }
            previous = os.getcwd()
            try:
                os.chdir(directory)
                first = resolve_revisions.resolve(
                    configuration, main_repository=directory
                )
                second_main = commit("c" * 40)
                second = resolve_revisions.resolve(
                    configuration, main_repository=directory
                )
            finally:
                os.chdir(previous)
            self.assertEqual(first["baseline"]["comet"], first_main)
            self.assertEqual(second["baseline"]["comet"], second_main)
            self.assertEqual(first["candidate"], second["candidate"])
            self.assertTrue(
                second["baseline"]["iceberg_dependency"]["source"].endswith(
                    "#" + "c" * 40
                )
            )
            self.assertTrue(
                second["candidate"]["iceberg_dependency"]["source"].endswith(
                    "#" + "a" * 40
                )
            )
            with patch.object(
                resolve_revisions, "git", wraps=command
            ), self.assertRaises(ValueError):
                resolve_revisions.fixed_sha("main")

    def test_lossless_result_comparison(self):
        self.assertFalse(
            lib.equivalent(lib.canonical(1.23456701), lib.canonical(1.23456709))
        )
        self.assertTrue(
            lib.equivalent(lib.canonical(float("nan")), lib.canonical(float("nan")))
        )
        self.assertTrue(lib.equivalent(lib.canonical(-0.0), lib.canonical(0.0)))
        self.assertTrue(
            lib.equivalent(lib.canonical(1.0), lib.canonical(1.0 + 1e-10), True)
        )
        self.assertFalse(lib.equivalent(1000000000, 1000000001, True))
        self.assertFalse(lib.equivalent(None, "None", True))

    def test_unordered_signed_zero_and_decimal_rows_use_semantic_sorting(self):
        expected = [(0.0, 1), (-0.0, 2)]
        actual = [(0.0, 2), (-0.0, 1)]
        expected_rows = lib.result_rows(expected, ordered=False)
        actual_rows = lib.result_rows(actual, ordered=False)
        self.assertTrue(lib.equivalent(actual_rows, expected_rows))
        self.assertNotEqual(lib.digest(actual_rows), lib.digest(expected_rows))
        self.assertFalse(
            lib.equivalent(
                lib.result_rows(actual, ordered=True),
                lib.result_rows(expected, ordered=True),
            )
        )
        self.assertFalse(
            lib.equivalent(
                lib.result_rows([(0.0, 1), (-0.0, 1)], ordered=False), expected_rows
            )
        )
        # Different decimal encodings must not reorder their associated payloads either.
        large = "12345678901234567890123456789012345678"
        decimal_expected = [
            (decimal.Decimal(large + ".0"), 1),
            (decimal.Decimal(large + ".00"), 2),
        ]
        decimal_actual = [
            (decimal.Decimal(large + ".0"), 2),
            (decimal.Decimal(large + ".000"), 1),
        ]
        with decimal.localcontext() as context:
            context.prec = 4
            self.assertTrue(
                lib.equivalent(
                    lib.result_rows(decimal_actual, False),
                    lib.result_rows(decimal_expected, False),
                )
            )
            self.assertNotEqual(
                lib.semantic_sort_value(lib.canonical(decimal.Decimal(large))),
                lib.semantic_sort_value(lib.canonical(decimal.Decimal(large + "1"))),
            )
        with tempfile.TemporaryDirectory() as directory:
            variables = {
                "BENCH_EXPECTED": directory,
                "BENCH_VARIANT": "spark",
                "BENCH_SUITE": "join",
            }
            with patch.dict(os.environ, variables):
                oracle = lib.check_result(
                    "zero", "SELECT zero", None, expected, False, "same schema"
                )
                self.assertEqual(oracle["correctness"], "oracle")
                os.environ["BENCH_VARIANT"] = "candidate_on"
                comparison = lib.check_result(
                    "zero", "SELECT zero", None, actual, False, "same schema"
                )
                self.assertEqual(comparison["correctness"], "tolerance")
                self.assertNotEqual(
                    comparison["checksum"], comparison["oracle_checksum"]
                )

    def test_oracle_detects_schema_and_value_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            variables = {
                "BENCH_EXPECTED": directory,
                "BENCH_VARIANT": "spark",
                "BENCH_SUITE": "join",
            }
            with patch.dict(os.environ, variables):
                reference = lib.check_result(
                    "q", "SELECT 1", None, [(1,)], True, "integer schema"
                )
                self.assertEqual(reference["correctness"], "oracle")
                os.environ["BENCH_VARIANT"] = "candidate_on"
                exact = lib.check_result(
                    "q", "SELECT 1", None, [(1,)], True, "integer schema"
                )
                self.assertEqual(exact["correctness"], "exact")
                wrong_value = lib.check_result(
                    "q", "SELECT 1", None, [(2,)], True, "integer schema"
                )
                self.assertEqual(wrong_value["correctness"], "mismatch")
                wrong_schema = lib.check_result(
                    "q", "SELECT 1", None, [(1,)], True, "string schema"
                )
                self.assertEqual(wrong_schema["correctness"], "mismatch")
                missing = lib.check_result(
                    "q", "SELECT 2", None, [(2,)], True, "integer schema"
                )
                self.assertEqual(missing["correctness"], "missing oracle")

    def test_balanced_variant_positions_and_carryover(self):
        for row in run_suite.ORDERS:
            self.assertEqual(len(set(row)), 4)
        for position in range(4):
            self.assertEqual(len({row[position] for row in run_suite.ORDERS}), 4)
        pairs = [(a, b) for row in run_suite.ORDERS for a, b in zip(row, row[1:])]
        self.assertEqual(len(set(pairs)), 12)

    def test_incomplete_duplicate_failed_and_shared_error_results_fail(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory / "manifests").mkdir()
            records = []
            for variant in lib.VARIANTS + ["spark"]:
                for round_number in range(4) if variant != "spark" else [0]:
                    reps = 2 if variant != "spark" else 1
                    manifest = {
                        "suite": "join",
                        "variant": variant,
                        "round": round_number,
                        "reps": reps,
                        "queries": [{"query": "q", "sql_sha256": "x"}],
                    }
                    (
                        directory / "manifests" / f"join-{variant}-{round_number}.json"
                    ).write_text(json.dumps(manifest))
                    for rep in range(reps):
                        records.append(
                            {
                                "suite": "join",
                                "variant": variant,
                                "round": round_number,
                                "rep": rep,
                                "query": "q",
                                "sql_sha256": "x",
                                "checksum": "ok",
                                "correctness": (
                                    "oracle" if variant == "spark" else "exact"
                                ),
                                "total_ms": 10 if variant.startswith("baseline") else 5,
                            }
                        )
            validate = lambda values: summarize.validate(values, directory, ["join"], 4)
            self.assertEqual(validate(records), [])
            self.assertTrue(validate(records[:-1]))
            self.assertTrue(validate(records + records[:1]))
            broken = [dict(r) for r in records]
            broken[0]["correctness"] = "mismatch"
            self.assertTrue(validate(broken))
            self.assertTrue(
                validate([dict(r, error="shared failure") for r in records])
            )
            baseline = [r for r in records if r["variant"] == "baseline_on"]
            candidate = [r for r in records if r["variant"] == "candidate_on"]
            self.assertEqual(
                summarize.paired_ratio(baseline, candidate), (2.0, 2.0, 2.0, 4)
            )
            self.assertEqual(
                summarize.paired_ratio(baseline[:2], candidate[:2])[1:3], (None, None)
            )
            self.assertIsNone(
                summarize.paired_ratio(
                    baseline, [dict(r, correctness="mismatch") for r in candidate]
                )
            )
            self.assertIsNone(
                summarize.paired_ratio(
                    baseline, [dict(r, error="failure") for r in candidate]
                )
            )
            self.assertIsNone(summarize.paired_ratio(baseline, candidate[:-1]))
            self.assertIsNone(
                summarize.paired_ratio(baseline, candidate + candidate[:1])
            )
            self.assertIsNone(
                summarize.paired_ratio(
                    baseline, [dict(r, sql_sha256="different") for r in candidate]
                )
            )

    def test_tpch_namespace_preserves_column_aliases(self):
        sql = "SELECT nation FROM (SELECT n2.n_name AS nation FROM nation n2) s GROUP BY nation"
        natural = run_tpch.qualify(sql, "tpch_nat")
        clustered = run_tpch.qualify(sql, "tpch_clu")
        self.assertEqual(natural.split("\n", 1)[1], sql)
        self.assertEqual(clustered.split("\n", 1)[1], sql)
        self.assertNotEqual(natural, clustered)

    def test_all_workloads_have_unique_names(self):
        # PySpark is available in the workflow before this preflight runs.
        import run_queries

        for suite in ("join", "topk_minmax", "layouts", "fuzz", "strjoin"):
            queries = run_queries.queries_for(suite)
            self.assertTrue(queries)
            names = [q[0] for q in queries]
            self.assertEqual(len(names), len(set(names)), suite)


if __name__ == "__main__":
    unittest.main()
