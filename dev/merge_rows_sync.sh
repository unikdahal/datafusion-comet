#!/usr/bin/env bash
set -euo pipefail

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git fetch origin merge-rows-merge-work
git checkout -B merge-rows-working origin/merge-rows-merge-work

python3 - <<'PY'
from pathlib import Path

# Current main owns operator tag 120 for IcebergWrite. MergeRows uses the next unused core tag.
proto = Path("native/proto/src/proto/operator.proto")
text = proto.read_text()
old = "    IcebergWrite iceberg_write = 120;\n    MergeRows merge_rows = 120;"
new = "    IcebergWrite iceberg_write = 120;\n    MergeRows merge_rows = 121;"
if old not in text:
    raise SystemExit("expected MergeRows operator-tag collision was not found")
proto.write_text(text.replace(old, new, 1))

# Validate the protobuf instruction contract before compiling any expressions. The declared
# output_types width is authoritative at this boundary; deriving a schema from malformed output
# projections first would let the malformed payload define its own expected width.
planner = Path("native/core/src/execution/planner.rs")
text = planner.read_text()
anchor = """                let compile_instructions = |instrs: &[spark_operator::MergeInstruction]| -> Result<
                    Vec<MergeInstructionExec>,
                    ExecutionError,
                > {
                    instrs
                        .iter()
                        .map(|instr| {
                            let condition = self.create_expr(
"""
replacement = """                let output_width = merge.output_types.len();
                let compile_instructions = |instrs: &[spark_operator::MergeInstruction]| -> Result<
                    Vec<MergeInstructionExec>,
                    ExecutionError,
                > {
                    instrs
                        .iter()
                        .enumerate()
                        .map(|(instruction_idx, instr)| {
                            if instr.outputs.len() > 2 {
                                return Err(ExecutionError::GeneralError(format!(
                                    \"MergeRows instruction {instruction_idx} has {} output rows; \\
                                     expected at most 2\",
                                    instr.outputs.len()
                                )));
                            }
                            for (output_idx, row) in instr.outputs.iter().enumerate() {
                                if row.exprs.len() != output_width {
                                    return Err(ExecutionError::GeneralError(format!(
                                        \"MergeRows instruction {instruction_idx} output \\
                                         {output_idx} has {} expressions; expected {output_width}\",
                                        row.exprs.len()
                                    )));
                                }
                            }

                            let condition = self.create_expr(
"""
if anchor not in text:
    raise SystemExit("expected MergeRows planner instruction compiler was not found")
planner.write_text(text.replace(anchor, replacement, 1))

# Spark 3.4 has no core MergeRowsExec, so its versioned suite should assert the shim contract
# directly instead of compiling a large Spark-3.5 test suite whose every test immediately skips.
suite34 = Path("spark/src/test/spark-3.4/org/apache/comet/exec/CometMergeRowsSuite.scala")
suite34.write_text('''/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet.exec

import org.apache.spark.sql.CometTestBase

import org.apache.comet.rules.shims.ShimCometMergeRows

class CometMergeRowsSuite extends CometTestBase {
  test("Spark 3.4 keeps core MergeRows execution unsupported") {
    assert(ShimCometMergeRows.nativeExecs.isEmpty)
  }
}
''')

# Versioned Spark 3.5 / 4.0 source roots already encode compatibility; runtime assumes only add
# dead skipped branches. Also keep the new code self-contained without external tracker IDs.
for path in [
    Path("spark/src/test/spark-3.5/org/apache/comet/exec/CometMergeRowsSuite.scala"),
    Path("spark/src/test/spark-4.0/org/apache/comet/exec/CometMergeRowsSuite.scala"),
]:
    text = path.read_text()
    text = text.replace("import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus\n", "")
    text = text.replace(
        '  private def assumeMerge(): Unit = assume(isSpark35Plus, "MergeRowsExec requires Spark 3.5+")\n\n',
        "",
    )
    text = text.replace("    assumeMerge()\n", "")
    text = text.replace(
        " * baseline. Broader native-write acceleration is tracked by umbrella issue #5122. See\n"
        " * `CometIcebergWriteActionSuite` for MERGE INTO coverage against real Iceberg tables.\n",
        " * baseline. See `CometIcebergWriteActionSuite` for MERGE INTO coverage against real\n"
        " * Iceberg tables.\n",
    )
    path.write_text(text)

# Keep the Spark 3.4 shim explanation version-focused without an external tracker reference.
shim34 = Path("spark/src/main/spark-3.4/org/apache/comet/rules/shims/ShimCometMergeRows.scala")
text = shim34.read_text()
text = text.replace(
    " * Spark 3.4 predates `MergeRowsExec` (it was moved from Iceberg extensions into Spark core in\n"
    " * Iceberg 1.4.0 / SPARK-52403, first shipping in Spark 3.5). Nothing to register here; CoW MERGE\n"
    " * on 3.4 continues to run via Iceberg's own extension-provided operator, unconverted.\n",
    " * Spark 3.4 predates the core `MergeRowsExec`. Nothing to register here; row-level MERGE on\n"
    " * 3.4 continues to use the connector-provided execution path, unconverted.\n",
)
shim34.write_text(text)

# Use the repository's canonical Spark-version helper rather than parsing SPARK_VERSION locally.
shim4 = Path("spark/src/main/spark-4.x/org/apache/comet/rules/shims/ShimCometMergeRows.scala")
text = shim4.read_text()
text = text.replace("import org.apache.spark.SPARK_VERSION\n", "")
text = text.replace(
    "import org.apache.comet.serde.CometOperatorSerde\n",
    "import org.apache.comet.CometSparkSessionExtensions.isSpark41Plus\n"
    "import org.apache.comet.serde.CometOperatorSerde\n",
)
text = text.replace('    if (SPARK_VERSION.startsWith("4.0.")) {', "    if (!isSpark41Plus) {")
shim4.write_text(text)
PY

# Preserve the current-main workflow bodies in this staging branch. The GitHub connector will add
# the MergeRows suite entries after validation so the final tree can still contain workflow edits.
git checkout merge-rows-main-base -- \
  .github/workflows/pr_build_linux.yml \
  .github/workflows/pr_build_macos.yml

git add -A
git commit -m "tmp: apply MergeRows sync review fixes"

# Formatting and source-set compilation across every Spark profile touched by the feature.
for profile in \
  "spark-3.4,scala-2.12" \
  "spark-3.5,scala-2.12" \
  "spark-4.0,scala-2.13" \
  "spark-4.1,scala-2.13" \
  "spark-4.2,scala-2.13"
do
  ./mvnw -B -P"$profile" -DskipTests spotless:apply
  ./mvnw -B -P"$profile" -DskipTests spotless:check
  ./mvnw -B -pl spark -am -P"$profile" -DskipTests test-compile
done

(
  cd native
  cargo fmt --all
  cargo fmt --all -- --check
  cargo test -p datafusion-comet merge_rows --lib
)

git diff --check

# New MergeRows code must not carry external issue/PR identifiers on the fork.
if git diff --unified=0 merge-rows-main-base...HEAD | \
    grep '^+' | grep -E '(SPARK-[0-9]+|issue #[0-9]+|PR #[0-9]+|pull/[0-9]+)'; then
  echo "external tracker identifier found in added MergeRows content" >&2
  exit 1
fi

# Include formatter output, then collapse the staging branch to one commit whose sole parent is
# the exact current-main snapshot.
git add -A
git reset --soft merge-rows-main-base
git commit -m "feat: add native support for MergeRowsExec"
test "$(git rev-list --count merge-rows-main-base..HEAD)" = "1"
test "$(git rev-list --count HEAD..merge-rows-main-base)" = "0"
git push --force origin HEAD:merge-rows-final-staging
