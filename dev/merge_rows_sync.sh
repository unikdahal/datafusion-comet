#!/usr/bin/env bash
set -euo pipefail

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git fetch origin merge-rows-merge-work
git checkout -B merge-rows-working origin/merge-rows-merge-work

python3 - <<'PY'
from pathlib import Path

# Apply the already-reviewed protobuf collision repair so Scala compilation can reach the
# MergeRows source/test source failure we are isolating.
proto = Path("native/proto/src/proto/operator.proto")
text = proto.read_text()
old = "    IcebergWrite iceberg_write = 120;\n    MergeRows merge_rows = 120;"
new = "    IcebergWrite iceberg_write = 120;\n    MergeRows merge_rows = 121;"
if old not in text:
    raise SystemExit("expected MergeRows operator-tag collision was not found")
proto.write_text(text.replace(old, new, 1))

# The Spark-3.5 source root is itself the compatibility gate; remove the redundant runtime
# assumption exactly as the full validation candidate does.
suite = Path("spark/src/test/spark-3.5/org/apache/comet/exec/CometMergeRowsSuite.scala")
text = suite.read_text()
text = text.replace("import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus\n", "")
text = text.replace(
    '  private def assumeMerge(): Unit = assume(isSpark35Plus, "MergeRowsExec requires Spark 3.5+")\n\n',
    "",
)
text = text.replace("    assumeMerge()\n", "")
suite.write_text(text)
PY

log=/tmp/spark-3.5-test-compile.log
if ! ./mvnw -B -pl spark -am -Pspark-3.5,scala-2.12 -DskipTests test-compile 2>&1 | tee "$log"; then
  mkdir -p .ci-diagnostics
  cp "$log" .ci-diagnostics/spark-test-compile.log
  printf '%s\n' 'spark-3.5,scala-2.12' > .ci-diagnostics/profile.txt
  git add -A
  git commit -m "tmp: capture Spark 3.5 MergeRows compile diagnostics"
  git push --force origin HEAD:merge-rows-diagnostic
  exit 1
fi

echo 'Spark 3.5 test-compile passed in isolated reproduction.'
