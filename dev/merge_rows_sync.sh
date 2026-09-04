#!/usr/bin/env bash
set -euo pipefail

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git remote add upstream https://github.com/apache/datafusion-comet.git
git fetch upstream main
git fetch origin refs/pull/2/head:refs/remotes/origin/merge-rows-pr
git checkout -B merge-rows-working refs/remotes/origin/merge-rows-pr

if ! git merge --no-ff --no-commit upstream/main; then
  echo "::group::Unmerged files"
  git diff --name-only --diff-filter=U
  echo "::endgroup::"
  echo "::group::Conflict hunks"
  git diff --cc
  echo "::endgroup::"
  exit 1
fi

python3 - <<'PY'
from pathlib import Path

proto = Path("native/proto/src/proto/operator.proto")
text = proto.read_text()
old = "    IcebergWrite iceberg_write = 120;\n    MergeRows merge_rows = 120;"
new = "    IcebergWrite iceberg_write = 120;\n    MergeRows merge_rows = 121;"
if old not in text:
    raise SystemExit("expected MergeRows operator-tag collision was not found")
proto.write_text(text.replace(old, new, 1))

merge_rows = Path("native/core/src/execution/operators/merge_rows.rs")
text = merge_rows.read_text()

anchor = """impl MergeConfig {
    /// Validate the optional row ID against the current child schema.
    fn validate(&self, child: &Arc<dyn ExecutionPlan>) -> Result<(), DataFusionError> {
"""
replacement = """impl MergeConfig {
    fn validate_instruction_shapes(&self, output_width: usize) -> Result<(), DataFusionError> {
        for (group_name, instructions) in [
            (\"matched\", &self.matched_instructions),
            (\"not matched\", &self.not_matched_instructions),
            (
                \"not matched by source\",
                &self.not_matched_by_source_instructions,
            ),
        ] {
            for (instruction_idx, instruction) in instructions.iter().enumerate() {
                if instruction.outputs.len() > 2 {
                    return Err(DataFusionError::Internal(format!(
                        \"MergeRows: {group_name} instruction {instruction_idx} has {} output rows; \\
                         expected at most 2\",
                        instruction.outputs.len()
                    )));
                }
                for (output_idx, output) in instruction.outputs.iter().enumerate() {
                    if output.len() != output_width {
                        return Err(DataFusionError::Internal(format!(
                            \"MergeRows: {group_name} instruction {instruction_idx} output \\
                             {output_idx} has {} expressions; expected {output_width}\",
                            output.len()
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// Validate the instruction wire contract and optional row ID against the current schemas.
    fn validate(
        &self,
        child: &Arc<dyn ExecutionPlan>,
        output_width: usize,
    ) -> Result<(), DataFusionError> {
"""
if anchor not in text:
    raise SystemExit("expected MergeConfig validation anchor was not found")
text = text.replace(anchor, replacement, 1)

if "        config.validate(&child)?;" not in text:
    raise SystemExit("expected constructor validation call was not found")
text = text.replace(
    "        config.validate(&child)?;",
    "        config.validate(&child, schema.fields().len())?;",
    1,
)
if "        self.config.validate(&child)?;" not in text:
    raise SystemExit("expected replacement-child validation call was not found")
text = text.replace(
    "        self.config.validate(&child)?;",
    "        self.config.validate(&child, self.schema.fields().len())?;",
    1,
)

helper = """    fn test_config(
        matched_instructions: Vec<MergeInstructionExec>,
        not_matched_instructions: Vec<MergeInstructionExec>,
        not_matched_by_source_instructions: Vec<MergeInstructionExec>,
        row_id_ordinal: Option<usize>,
    ) -> MergeConfig {
        MergeConfig {
            is_source_row_present: col(\"source_present\", &test_schema()).unwrap(),
            is_target_row_present: col(\"target_present\", &test_schema()).unwrap(),
            matched_instructions,
            not_matched_instructions,
            not_matched_by_source_instructions,
            row_id_ordinal,
        }
    }
"""
tests = helper + """
    #[test]
    fn rejects_invalid_instruction_shapes() {
        let too_many_outputs = MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![lit(1i32)], vec![lit(2i32)], vec![lit(3i32)]],
        };
        let err = test_config(vec![too_many_outputs], vec![], vec![], None)
            .validate_instruction_shapes(1)
            .unwrap_err();
        assert!(err.to_string().contains(\"has 3 output rows; expected at most 2\"));

        let wrong_width = MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![lit(1i32), lit(2i32)]],
        };
        let err = test_config(vec![], vec![wrong_width], vec![], None)
            .validate_instruction_shapes(1)
            .unwrap_err();
        assert!(err.to_string().contains(\"has 2 expressions; expected 1\"));
    }
"""
if helper not in text:
    raise SystemExit("expected MergeRows unit-test helper was not found")
merge_rows.write_text(text.replace(helper, tests, 1))
PY

git add -A
git commit -m "tmp: merge main and apply review fixes"

./mvnw -B -Pspark-3.5,scala-2.12 -DskipTests spotless:apply
./mvnw -B -Pspark-4.1,scala-2.13 -DskipTests spotless:apply
./mvnw -B -Pspark-3.5,scala-2.12 -DskipTests spotless:check
./mvnw -B -Pspark-4.1,scala-2.13 -DskipTests spotless:check

./mvnw -B -pl spark -am -Pspark-3.5,scala-2.12 -DskipTests test-compile
./mvnw -B -pl spark -am -Pspark-4.1,scala-2.13 -DskipTests test-compile

(
  cd native
  cargo fmt --all
  cargo fmt --all -- --check
  cargo test -p datafusion-comet merge_rows --lib
)

git diff --check

# The Actions token cannot update workflow files. Keep the staging branch on the current
# main workflow versions; the GitHub connector adds the two suite-list entries afterward.
git checkout upstream/main -- \
  .github/workflows/pr_build_linux.yml \
  .github/workflows/pr_build_macos.yml

git add -A
git reset --soft upstream/main
git commit -m "feat: add native support for MergeRowsExec"
test "$(git rev-list --count upstream/main..HEAD)" = "1"
test "$(git rev-list --count HEAD..upstream/main)" = "0"
git push --force origin HEAD:merge-rows-sync-work
