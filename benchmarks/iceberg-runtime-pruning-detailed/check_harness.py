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
    def test_campaign_shards_cover_full_catalog_and_preserve_all_fuzz_seeds(self):
        campaign = run_suite.make_campaign({})
        self.assertFalse(campaign["partial"])
        self.assertEqual(len(campaign["matrix"]), 18)
        for suite in campaign["suites"]:
            for round_number, catalog in campaign["catalog"][suite].items():
                assigned = [q for shard in campaign["assignments"][suite][round_number] for q in shard]
                self.assertCountEqual(assigned, [q["query"] for q in catalog])
                self.assertEqual(len(assigned), len(set(assigned)))
                if suite == "fuzz":
                    self.assertEqual(len(catalog), 500)
                    self.assertTrue(all(q["query"].startswith(f"fuzz{int(round_number) + 1}_") for q in catalog))

    def test_targeted_campaign_refuses_unknown_names_and_only_limits_query_sets(self):
        name = run_suite.query_catalog("join", 0)[0]["query"]
        campaign = run_suite.make_campaign({"suites": ["join"], "queries": {"join": [name]}})
        self.assertTrue(campaign["partial"])
        self.assertEqual(len(campaign["matrix"]), 1)
        self.assertEqual(campaign["parameters"], run_suite.PARAMETERS)
        self.assertEqual(campaign["query_sets"]["join"], {str(r): [name] for r in range(8)})
        for configuration in ({"suites": ["unknown"]}, {"suites": []},
                              {"queries": {"join": ["missing"]}},
                              {"suites": ["join"], "queries": {"tpch": ["missing"]}}):
            with self.assertRaises(ValueError):
                run_suite.make_campaign(configuration)

    def test_targeted_mode_uses_the_same_shard_independent_data_cache_key(self):
        full = run_suite.make_campaign({"suites": ["join"]})
        name = full["catalog"]["join"]["0"][0]["query"]
        selected = run_suite.make_campaign({"suites": ["join"], "queries": {"join": [name]}})
        self.assertEqual(full["data_matrix"][0]["data_key"], selected["data_matrix"][0]["data_key"])

    def test_tpch_sharding_retains_q15_select_and_file_setup_cleanup(self):
        catalog = run_tpch.queries_for()
        q15 = [q[0] for q in catalog if "__q15" in q[0]]
        self.assertEqual(len(q15), 2)
        path = next(p for p in run_tpch.query_files() if Path(p).stem == "q15")
        statements = run_tpch.statements(path)
        self.assertTrue(any(not run_tpch.is_select(sql) for sql in statements))
        for database in run_tpch.DATABASES:
            selected = [run_tpch.query_name(database, path, i) for i, sql in enumerate(statements)
                        if run_tpch.is_select(sql)]
            self.assertEqual(len(selected), 1)
            self.assertIn(selected[0], q15)

    def test_report_rejects_a_query_omitted_by_every_shard_and_missing_shard(self):
        # Valid synthetic artifacts use a real full-suite canonical catalog. An omitted
        # query must fail even if all variants/manifests agree on that incomplete set.
        campaign = run_suite.make_campaign({"suites": ["join"]})
        resolved = {"campaign": campaign, "baseline": {"repository": resolve_revisions.MAIN_REPOSITORY,
                    "ref": "refs/heads/main", "comet": "a" * 40, "iceberg_dependency": {"name": "iceberg"}},
                    "candidate": {"comet": "b" * 40, "iceberg_dependency": {"name": "iceberg"}}}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for shard in range(3):
                directory = root / f"matched-join-shard-{shard}"
                (directory / "manifests").mkdir(parents=True)
                suffix = f"join-shard-{shard}"
                environment = {"resolved_revisions": resolved, "suite": "join", "shard": shard, "shards": 3,
                               "runner": {"cpu_model": "fixture", "cores": 4, "memory_mib": 16000,
                                          "cpuinfo": "fixture", "free_m": "fixture"},
                               "builds": {side: {"comet": resolved[side]["comet"], "resolved_revisions": resolved,
                                          "dependencies": [resolved[side]["iceberg_dependency"]]}
                                          for side in ("baseline", "candidate")}}
                (directory / f"environment-{suffix}.json").write_text(json.dumps(environment))
                (directory / f"data-{suffix}.json").write_text(json.dumps([{"file": "data", "sha256": "x", "bytes": 1}]))
                for prefix in ("results", "warmup-validation"):
                    (directory / f"{prefix}-{suffix}.jsonl").write_text("")
                for variant in lib.VARIANTS + ["spark"]:
                    for r in (range(8) if variant != "spark" else [0]):
                        names = campaign["assignments"]["join"][str(r)][shard]
                        manifest = {"suite": "join", "variant": variant, "round": r, "shard": shard, "shards": 3,
                                    "reps": 1 if variant == "spark" else 2,
                                    "warmups": 0 if variant == "spark" else 4,
                                    "queries": [q for q in campaign["catalog"]["join"][str(r)] if q["query"] in names]}
                        (directory / "manifests" / f"join-{variant}-{r}-shard-{shard}.json").write_text(json.dumps(manifest))
            self.assertEqual(summarize.merge_shards(root, resolved)[1], [])
            for path in (root / "matched-join-shard-0/manifests").glob("*.json"):
                manifest = json.loads(path.read_text())
                manifest["queries"].pop(0)
                path.write_text(json.dumps(manifest))
            errors = summarize.merge_shards(root, resolved)[1]
            self.assertTrue(any("Union of shard query sets" in error for error in errors))
            import shutil
            shutil.rmtree(root / "matched-join-shard-1")
            self.assertTrue(any("missing=" in error for error in summarize.merge_shards(root, resolved)[1]))

    def test_stage_metric_preserves_missing_and_sums_zero_and_nonzero(self):
        self.assertIsNone(summarize.stage_metric({}, "rows"))
        self.assertIsNone(summarize.stage_metric({"stages": [{"metrics": {}}]}, "rows"))
        self.assertEqual(
            summarize.stage_metric(
                {"stages": [{"metrics": {"rows": 0}}, {"metrics": {"rows": 12}}]},
                "rows",
            ),
            12,
        )
        self.assertEqual(
            summarize.stage_metric({"stages": [{"metrics": {"rows": 0}}]}, "rows"),
            0,
        )

    def test_native_scan_coverage_distinguishes_partial_fallback_and_repeated_tables(
        self,
    ):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "plans/layouts/candidate_on/digest.txt"
            path.parent.mkdir(parents=True)
            dimension = "CometIcebergNativeScan [id#5], /warehouse/dim/metadata/v1.metadata.json, dim, 4\n"
            fact = "CometIcebergNativeScan [id#1], /warehouse/fact/metadata/v1.metadata.json, fact, 256\n"
            self.assertIsNone(
                summarize.native_scan_coverage(
                    root, "layouts", "candidate_on", "digest"
                )
            )
            path.write_text("BatchScan fact\n" + dimension)
            partial = summarize.native_scan_coverage(
                root, "layouts", "candidate_on", "digest"
            )
            path.write_text(fact + dimension)
            full = summarize.native_scan_coverage(
                root, "layouts", "candidate_on", "digest"
            )
            self.assertNotEqual(partial, full)
            path.write_text(dimension + fact)
            self.assertEqual(
                summarize.native_scan_coverage(
                    root, "layouts", "candidate_on", "digest"
                ),
                full,
            )
            path.write_text(fact + fact + dimension)
            self.assertNotEqual(
                summarize.native_scan_coverage(
                    root, "layouts", "candidate_on", "digest"
                ),
                full,
            )
            path.write_text("BatchScan fact\n")
            self.assertEqual(
                summarize.native_scan_coverage(
                    root, "layouts", "candidate_on", "digest"
                ),
                (),
            )
            path.write_text("CometIcebergNativeScan [id#1], unfamiliar format\n")
            self.assertIsNone(
                summarize.native_scan_coverage(
                    root, "layouts", "candidate_on", "digest"
                )
            )

    def test_warmup_gate_rejects_failures_missing_duplicates_and_different_sql(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "manifests").mkdir()
            manifest = {
                "variant": "candidate_on",
                "round": 0,
                "warmups": 1,
                "queries": [{"query": "q", "sql_sha256": "digest"}],
            }
            (root / "manifests/join-candidate_on-0.json").write_text(
                json.dumps(manifest)
            )
            path = root / "warmup-validation-join.jsonl"
            good = {
                "suite": "join",
                "variant": "candidate_on",
                "round": 0,
                "iteration": 0,
                "query": "q",
                "sql_sha256": "digest",
                "correctness": "exact",
            }
            self.assertEqual(
                summarize.validate_warmups(root, ["join"], True)[1], {("join", "q")}
            )
            for runs in [
                [good],
                [dict(good, correctness="mismatch")],
                [good, good],
                [dict(good, sql_sha256="changed")],
                [dict(good, error="warmup failed")],
            ]:
                path.write_text("".join(json.dumps(r) + "\n" for r in runs))
                failures, invalid = summarize.validate_warmups(root, ["join"], True)
                if runs == [good]:
                    self.assertEqual((failures, invalid), ([], set()))
                else:
                    self.assertTrue(failures)
                    self.assertEqual(invalid, {("join", "q")})
            manifest.pop("warmups")
            (root / "manifests/join-candidate_on-0.json").write_text(
                json.dumps(manifest)
            )
            self.assertTrue(summarize.validate_warmups(root, ["join"], True)[0])

    def test_warmup_record_retains_correctness_and_omits_timing(self):
        with patch.object(lib, "append") as append:
            lib.record_warmup(
                {
                    "query": "q",
                    "sql_sha256": "digest",
                    "correctness": "mismatch",
                    "checksum": "bad",
                    "oracle_checksum": "good",
                    "total_ms": 1,
                    "stages": [{}],
                },
                "join",
                "baseline_on",
                2,
                0,
            )
        path, record = append.call_args.args
        self.assertEqual(path, "warmup-validation-join.jsonl")
        self.assertEqual(record["correctness"], "mismatch")
        self.assertEqual(record["iteration"], 0)
        self.assertNotIn("total_ms", record)
        self.assertNotIn("stages", record)

    def test_ratio_gate_rejects_invalid_comparison_and_missing_sql_digest(self):
        good = [
            {
                "round": 0,
                "rep": 0,
                "total_ms": 10,
                "sql_sha256": "same",
                "correctness": "exact",
            }
        ]
        self.assertIsNone(
            summarize.paired_ratio(good, [dict(good[0], comparison_validated=False)])
        )
        missing = [{k: v for k, v in good[0].items() if k != "sql_sha256"}]
        self.assertIsNone(summarize.paired_ratio(missing, missing))

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
