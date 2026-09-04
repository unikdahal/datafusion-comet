#!/usr/bin/env bash
set -euo pipefail

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

# Materialize both refs locally. The validator needs the exact base for workflow restoration,
# full-diff checks, the final squash, and the one-commit ancestry assertions.
git fetch origin merge-rows-merge-work merge-rows-main-base
git branch -f merge-rows-main-base origin/merge-rows-main-base
git checkout -B merge-rows-working origin/merge-rows-merge-work

python3 - <<'PY'
import re
from pathlib import Path

# Current main owns operator tag 120 for IcebergWrite. MergeRows uses the next unused core tag.
proto = Path("native/proto/src/proto/operator.proto")
text = proto.read_text()
old = "    IcebergWrite iceberg_write = 120;\n    MergeRows merge_rows = 120;"
new = "    IcebergWrite iceberg_write = 120;\n    MergeRows merge_rows = 121;"
if old not in text:
    raise SystemExit("expected MergeRows operator-tag collision was not found")
proto.write_text(text.replace(old, new, 1))

# Validate the protobuf instruction contract before compiling expressions. output_types is the
# declared JVM/native wire width; malformed output rows must not define their own expected width.
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

# Spark 3.4 has no core MergeRowsExec. Keep the suite name because CI invokes it on every Spark
# profile, but make the version-specific contract test small and meaningful instead of skipping a
# large suite copied from a later source set.
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

# Spark 3.5 and 4.0 have dedicated test source roots, so the source set itself is the compatibility
# gate. Remove every runtime assumption as a complete line; substring deletion is deliberately
# avoided because it can leave indentation debris in nested test bodies.
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
    text = re.sub(r"^[ \t]*assumeMerge\(\)\n", "", text, flags=re.MULTILINE)
    text = text.replace(
        " * baseline. Broader native-write acceleration is tracked by umbrella issue #5122. See\n"
        " * `CometIcebergWriteActionSuite` for MERGE INTO coverage against real Iceberg tables.\n",
        " * baseline. See `CometIcebergWriteActionSuite` for MERGE INTO coverage against real\n"
        " * Iceberg tables.\n",
    )
    path.write_text(text)

# Keep the Spark 3.4 shim explanation version-focused and self-contained.
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

# This source root is Spark 4.x-only, so the repository's version helper expresses the 4.0 vs
# 4.1+ boundary without local string parsing.
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

# Ensure the source-set cleanup really happened before spending time compiling.
if grep -R -n 'assumeMerge()' \
  spark/src/test/spark-3.5/org/apache/comet/exec/CometMergeRowsSuite.scala \
  spark/src/test/spark-4.0/org/apache/comet/exec/CometMergeRowsSuite.scala; then
  echo "runtime MergeRows version assumption remains in a version-specific test source root" >&2
  exit 1
fi

# Preserve current-main workflow bodies while Actions performs validation. The workflow suite
# entries are added to the validated final tree through the repository API afterwards.
git checkout merge-rows-main-base -- \
  .github/workflows/pr_build_linux.yml \
  .github/workflows/pr_build_macos.yml

git add -A
git commit -m "tmp: apply MergeRows sync review fixes"

# Native formatting/unit coverage first. Build one release library for the Scala execution suites.
(
  cd native
  cargo fmt --all
  cargo fmt --all -- --check
  cargo test -p datafusion-comet merge_rows --lib
  cargo build --release -p datafusion-comet
)

export SPARK_LOCAL_HOSTNAME=localhost
export SPARK_LOCAL_IP=127.0.0.1
export JAVA_TOOL_OPTIONS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED"

# Each version is cleaned before compilation so generated protobuf/classes from the previous
# profile cannot make a later profile pass accidentally. Spotless runs for every source-set view,
# then the dedicated MergeRows suite executes under the same profile.
for profile in \
  "spark-3.4,scala-2.12" \
  "spark-3.5,scala-2.12" \
  "spark-4.0,scala-2.13" \
  "spark-4.1,scala-2.13" \
  "spark-4.2,scala-2.13"
do
  ./mvnw -B -P"$profile" clean
  ./mvnw -B -P"$profile" -DskipTests spotless:apply
  ./mvnw -B -P"$profile" -DskipTests spotless:check
  ./mvnw -B -pl spark -am -P"$profile" -DskipTests test-compile

  MAVEN_OPTS="-Xmx4G -Xms2G -DwildcardSuites=org.apache.comet.exec.CometMergeRowsSuite" \
    SPARK_HOME="$(pwd)" \
    ./mvnw -B -Prelease -P"$profile" install

done

# Check the complete candidate relative to the frozen current-main snapshot, including formatter
# edits still in the working tree. A plain `git diff --check` here would only inspect unstaged
# formatter changes and could miss whitespace defects already committed by the transformation.
git diff --check merge-rows-main-base

# New fork-facing MergeRows content must remain self-contained and carry no external tracker IDs.
if git diff --unified=0 merge-rows-main-base | \
    grep '^+' | grep -E '(SPARK-[0-9]+|issue #[0-9]+|PR #[0-9]+|pull/[0-9]+)'; then
  echo "external tracker identifier found in added MergeRows content" >&2
  exit 1
fi

# Include formatter output, then collapse the candidate to one commit whose sole parent is the
# exact current-main snapshot used throughout validation.
git add -A
git reset --soft merge-rows-main-base
git commit -m "feat: add native support for MergeRowsExec"
test "$(git rev-list --count merge-rows-main-base..HEAD)" = "1"
test "$(git rev-list --count HEAD..merge-rows-main-base)" = "0"
test "$(git rev-parse HEAD^)" = "$(git rev-parse merge-rows-main-base)"
git push --force origin HEAD:merge-rows-final-staging
