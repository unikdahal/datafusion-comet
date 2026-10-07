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


class HarnessChecks(unittest.TestCase):
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
