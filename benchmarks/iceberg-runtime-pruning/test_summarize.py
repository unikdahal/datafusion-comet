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


"""Synthetic fixtures; run with unittest in GitHub Actions, without Spark."""

import contextlib
import copy
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import summarize
from workloads import EXTENDED_QUERIES, QUERIES, experiment


def fixture():
    manifest = experiment(2, 2)
    records = [
        {
            "query": query["query"], "sql_sha256": query["sql_sha256"],
            "variant": variant, "round": round_id, "rep": rep,
            "checksum": "a" * 16, "rows": 1,
            "schema_json": '{"type":"struct","fields":[]}',
            "plan_ms": 1.0, "exec_ms": 10.0, "total_ms": 11.0,
            "bytes_scanned": 100,
        }
        for query in manifest["queries"]
        for variant in manifest["variants"]
        for round_id in range(manifest["rounds"])
        for rep in range(manifest["reps"])
    ]
    return manifest, records


class CompletenessTests(unittest.TestCase):
    def setUp(self):
        self.manifest, self.records = fixture()

    def reject(self, records):
        with self.assertRaises(summarize.ValidationError):
            summarize.validate_records(records, self.manifest)

    def test_complete_matrix_in_any_order(self):
        queries, groups = summarize.validate_records(list(reversed(self.records)), self.manifest)
        self.assertEqual(queries, [q["query"] for q in self.manifest["queries"]])
        self.assertTrue(all(len(runs) == 4 for runs in groups.values()))

    def test_identical_main_on_without_off_fails(self):
        self.reject([r for r in self.records if r["variant"] != "off"])

    def test_missing_query_variant_round_and_rep(self):
        for field, value in (("query", "scan_control"), ("variant", "on"), ("round", 1), ("rep", 1)):
            with self.subTest(field=field):
                self.reject([r for r in self.records if r[field] != value])
        self.reject(self.records[:-1])
        self.reject([])

    def test_duplicate_does_not_replace_missing_identity(self):
        self.reject(self.records[:-1] + [self.records[0]])
        self.reject(self.records + [self.records[0]])

    def test_results_and_counts_must_agree(self):
        for field, value in (("checksum", "b" * 16), ("rows", 2), ("schema_json", '{"type":"long"}')):
            with self.subTest(field=field):
                records = copy.deepcopy(self.records)
                records[0][field] = value
                self.reject(records)

    def test_invalid_and_missing_fields(self):
        invalid = {
            "query": ["unknown", [], None], "variant": ["candidate", {}, None],
            "round": [-1, 2, True, "0"], "rep": [-1, 2, False, 0.5],
            "checksum": ["", "z" * 16, 123], "rows": [-1, True, 1.5, "1"],
            "sql_sha256": ["b" * 64, None],
            "schema_json": [None, "", {}],
            "plan_ms": [-1, True, "1", float("nan"), float("inf"), 10 ** 400],
            "exec_ms": [0, -1, False, "10", float("nan"), float("inf")],
            "total_ms": [-1, 12, True, "11", float("nan"), float("inf")],
            "bytes_scanned": [-1, True, 0.5, "100"],
            "error": ["failed"], "status": ["failed", None],
        }
        for field, values in invalid.items():
            for value in values:
                with self.subTest(field=field, value=value):
                    records = copy.deepcopy(self.records)
                    records[0][field] = value
                    self.reject(records)
        for field in ("query", "variant", "round", "rep", "checksum", "rows", "sql_sha256", "plan_ms", "exec_ms", "total_ms"):
            with self.subTest(missing=field):
                records = copy.deepcopy(self.records)
                del records[0][field]
                self.reject(records)
        for record in (None, [], "invalid", 1):
            self.reject([record] + self.records[1:])

    def test_invalid_manifest(self):
        for field, value in (("rounds", 0), ("reps", True), ("queries", []), ("variants", ["main", "on"]), ("protocol_version", 2)):
            with self.subTest(field=field):
                manifest = copy.deepcopy(self.manifest)
                manifest[field] = value
                with self.assertRaises(summarize.ValidationError):
                    summarize.validate_records(self.records, manifest)
        manifest = copy.deepcopy(self.manifest)
        manifest["queries"].append(manifest["queries"][0])
        with self.assertRaises(summarize.ValidationError):
            summarize.validate_records(self.records, manifest)

    def test_jsonl_rejects_invalid_json_and_duplicate_keys(self):
        for text in ('{', '{"round": 0, "round": 1}', '{"exec_ms": NaN}', '{"exec_ms": Infinity}'):
            with self.subTest(text=text), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "results.jsonl"
                path.write_text(text + "\n", encoding="utf-8")
                with self.assertRaises(summarize.ValidationError):
                    summarize.load_records(path)

    def test_cli_produces_no_comparisons_on_invalid_fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            results = Path(directory) / "results.jsonl"
            manifest = Path(directory) / "manifest.json"
            manifest.write_text(json.dumps(self.manifest), encoding="utf-8")
            cases = (
                [r for r in self.records if r["variant"] != "off"],
                self.records[:-1],
                self.records + [self.records[0]],
            )
            for records in cases:
                results.write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")
                stdout, stderr = io.StringIO(), io.StringIO()
                with patch("sys.argv", ["summarize.py", str(results), "--manifest", str(manifest), "--oracle", str(results)]), contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    self.assertEqual(summarize.main(), 1)
                self.assertEqual(stdout.getvalue(), "")
                self.assertIn("validation failed", stderr.getvalue())

    def test_complete_jsonl_cli(self):
        with tempfile.TemporaryDirectory() as directory:
            results, manifest = Path(directory) / "results.jsonl", Path(directory) / "manifest.json"
            results.write_text("".join(json.dumps(r) + "\n" for r in self.records), encoding="utf-8")
            manifest.write_text(json.dumps(self.manifest), encoding="utf-8")
            oracle = Path(directory) / "oracle.jsonl"
            oracle.write_text("".join(json.dumps(r) + "\n" for r in self.oracle()), encoding="utf-8")
            stdout = io.StringIO()
            with patch("sys.argv", ["summarize.py", str(results), "--manifest", str(manifest), "--oracle", str(oracle)]), contextlib.redirect_stdout(stdout):
                self.assertEqual(summarize.main(), 0)
            self.assertIn("Speedup", stdout.getvalue())
            self.assertIn("n/a", stdout.getvalue())

    def oracle(self):
        return [dict(r, variant="spark") for r in self.records if r["variant"] == "main" and r["round"] == 0 and r["rep"] == 0]

    def test_oracle_missing_duplicate_mismatch_and_invalid_records(self):
        queries, groups = summarize.validate_records(self.records, self.manifest)
        oracle = self.oracle()
        summarize.validate_oracle(oracle, queries, groups)
        for records in ([], oracle[:-1], oracle + [oracle[0]], [None] + oracle[1:]):
            with self.assertRaises(summarize.ValidationError):
                summarize.validate_oracle(records, queries, groups)
        for field, value in (("checksum", "b" * 16), ("rows", 2), ("schema_json", "different"), ("variant", "main"), ("query", "unknown"), ("error", "failed")):
            records = copy.deepcopy(oracle)
            records[0][field] = value
            with self.subTest(field=field), self.assertRaises(summarize.ValidationError):
                summarize.validate_oracle(records, queries, groups)


class RoundBootstrapTests(unittest.TestCase):
    def test_profiles_have_unique_names_and_retain_legacy_workloads(self):
        self.assertEqual(len(QUERIES), 9)
        self.assertEqual(len(EXTENDED_QUERIES), 18)
        names = [name for name, _ in EXTENDED_QUERIES]
        self.assertEqual(len(names), len(set(names)))
        self.assertEqual(EXTENDED_QUERIES[:9], QUERIES)

    def test_scope_requires_matching_nonempty_metadata_with_multiplicity(self):
        scope = [{"native_scan_metadata": ["fact", "dim"]}]
        self.assertTrue(summarize.same_native_scope(scope, scope))
        for other in ([{}], [{"native_scan_metadata": []}], [{"native_scan_metadata": ["dim"]}], [{"native_scan_metadata": ["fact", "dim", "dim"]}]):
            self.assertFalse(summarize.same_native_scope(scope, other))

    def test_shared_round_noise_is_paired_and_repetitions_are_clustered(self):
        import random

        base = [{"round": r, "exec_ms": value} for r, value in enumerate((10, 100, 1000)) for _ in range(3)]
        candidate = [dict(record, exec_ms=record["exec_ms"] / 2) for record in base]
        self.assertEqual(summarize.round_ratio_ci(base, candidate, random.Random(1)), (2.0, 2.0))
        low, high = summarize.ratio_ci([r["exec_ms"] for r in base], [r["exec_ms"] for r in candidate], random.Random(1))
        self.assertLess(low, 2.0)
        self.assertGreater(high, 2.0)

    def test_one_round_or_mismatched_rounds_has_no_paired_interval(self):
        import random

        records = [{"round": 0, "exec_ms": 1}]
        self.assertIsNone(summarize.round_ratio_ci(records, records, random.Random(1)))
        self.assertIsNone(summarize.round_ratio_ci(records, [{"round": 1, "exec_ms": 1}], random.Random(1)))


if __name__ == "__main__":
    unittest.main()
