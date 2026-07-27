// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, RecordBatchOptions};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::boolean::{and, not};
use arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::{
    execution::TaskContext,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        RecordBatchStream, SendableRecordBatchStream,
    },
};
use futures::{Stream, StreamExt};
use std::collections::HashSet;
use std::{
    any::Any,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// One `MergeRows.Instruction` (Keep / Discard / Split), expressed uniformly as a gating
/// condition plus zero, one, or two output row projections -- matching Spark's real
/// `condition: Expression, outputs: Seq[Seq[Expression]]` shape (Discard has zero output
/// projections, Keep has one, Split has two).
#[derive(Debug, Clone)]
pub struct MergeInstructionExec {
    pub condition: Arc<dyn PhysicalExpr>,
    pub outputs: Vec<Vec<Arc<dyn PhysicalExpr>>>,
}

/// A Comet native operator that reproduces Spark's `MergeRowsExec` (the row-level MERGE
/// dispatch operator introduced to Spark core in Iceberg 1.4.0 / SPARK-52403). Sits between the
/// target/source join and the write, deciding per row whether it becomes a kept row, is
/// discarded (a copy-on-write delete), or is split into two output rows.
#[derive(Debug)]
pub struct MergeRowsExec {
    is_source_row_present: Arc<dyn PhysicalExpr>,
    is_target_row_present: Arc<dyn PhysicalExpr>,
    matched_instructions: Vec<MergeInstructionExec>,
    not_matched_instructions: Vec<MergeInstructionExec>,
    not_matched_by_source_instructions: Vec<MergeInstructionExec>,
    check_cardinality: bool,
    row_id_ordinal: usize,
    child: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl MergeRowsExec {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        is_source_row_present: Arc<dyn PhysicalExpr>,
        is_target_row_present: Arc<dyn PhysicalExpr>,
        matched_instructions: Vec<MergeInstructionExec>,
        not_matched_instructions: Vec<MergeInstructionExec>,
        not_matched_by_source_instructions: Vec<MergeInstructionExec>,
        check_cardinality: bool,
        row_id_ordinal: usize,
        child: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> Result<Self, DataFusionError> {
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));

        Ok(Self {
            is_source_row_present,
            is_target_row_present,
            matched_instructions,
            not_matched_instructions,
            not_matched_by_source_instructions,
            check_cardinality,
            row_id_ordinal,
            child,
            schema,
            cache,
        })
    }
}

impl DisplayAs for MergeRowsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CometMergeRowsExec")
            }
            DisplayFormatType::TreeRender => unimplemented!(),
        }
    }
}

impl ExecutionPlan for MergeRowsExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MergeRowsExec {
            is_source_row_present: Arc::clone(&self.is_source_row_present),
            is_target_row_present: Arc::clone(&self.is_target_row_present),
            matched_instructions: self.matched_instructions.clone(),
            not_matched_instructions: self.not_matched_instructions.clone(),
            not_matched_by_source_instructions: self.not_matched_by_source_instructions.clone(),
            check_cardinality: self.check_cardinality,
            row_id_ordinal: self.row_id_ordinal,
            child: Arc::clone(&children[0]),
            schema: Arc::clone(&self.schema),
            cache: Arc::clone(&self.cache),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let child_stream = self.child.execute(partition, context)?;
        Ok(Box::pin(MergeRowsStream {
            is_source_row_present: Arc::clone(&self.is_source_row_present),
            is_target_row_present: Arc::clone(&self.is_target_row_present),
            matched_instructions: self.matched_instructions.clone(),
            not_matched_instructions: self.not_matched_instructions.clone(),
            not_matched_by_source_instructions: self.not_matched_by_source_instructions.clone(),
            check_cardinality: self.check_cardinality,
            row_id_ordinal: self.row_id_ordinal,
            child_stream,
            schema: Arc::clone(&self.schema),
            // One `seen` set per partition, created here and threaded through every batch this
            // stream polls -- see the field doc on `MergeRowsStream::seen` for why it must not
            // be reset per batch.
            seen: HashSet::new(),
        }))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn name(&self) -> &str {
        "CometMergeRowsExec"
    }
}

pub struct MergeRowsStream {
    is_source_row_present: Arc<dyn PhysicalExpr>,
    is_target_row_present: Arc<dyn PhysicalExpr>,
    matched_instructions: Vec<MergeInstructionExec>,
    not_matched_instructions: Vec<MergeInstructionExec>,
    not_matched_by_source_instructions: Vec<MergeInstructionExec>,
    check_cardinality: bool,
    row_id_ordinal: usize,
    child_stream: SendableRecordBatchStream,
    schema: SchemaRef,
    /// Target row ids already seen in a matched pair. Accumulated across *every* batch polled
    /// from this stream (i.e. for the lifetime of the partition), not reset per batch -- a
    /// cardinality violation where the two matching source rows land in different Arrow batches
    /// must still be caught. Mirrors Spark's `MergeRowsExec.BitmapCardinalityValidator`, which is
    /// task-scoped, not batch-scoped.
    seen: HashSet<i64>,
}

/// Rewrites NULL slots to `false`, producing an all-valid boolean array.
///
/// Every boolean in this operator -- the row-presence flags and each instruction condition --
/// goes through Spark's `BasePredicate.eval(InternalRow): Boolean`, which collapses a NULL
/// predicate result to plain `false`. Arrow's `and`/`not` kernels instead propagate NULL, so a
/// NULL must be flattened *before* it enters any mask arithmetic; otherwise a NULL condition
/// poisons `run_group`'s shrinking `remaining` mask and the row is silently dropped from every
/// later instruction in the group -- including the catch-all `Keep(TrueLiteral, target.output)`
/// that Spark's `RewriteMergeIntoTable` appends to the matched and not-matched-by-source groups.
/// For a copy-on-write MERGE that means the target row is never written to the rewritten data
/// file, i.e. silent data loss.
fn null_to_false(array: &BooleanArray) -> BooleanArray {
    match array.nulls() {
        Some(nulls) => BooleanArray::new(array.values() & nulls.inner(), None),
        None => array.clone(),
    }
}

fn eval_bool(
    expr: &Arc<dyn PhysicalExpr>,
    batch: &RecordBatch,
) -> Result<BooleanArray, DataFusionError> {
    let array: ArrayRef = expr.evaluate(batch)?.into_array(batch.num_rows())?;
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .map(null_to_false)
        .ok_or_else(|| DataFusionError::Internal("MergeRows: expected boolean array".to_string()))
}

fn project(
    batch: &RecordBatch,
    exprs: &[Arc<dyn PhysicalExpr>],
    schema: &SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let mut columns = Vec::with_capacity(exprs.len());
    for expr in exprs {
        columns.push(expr.evaluate(batch)?.into_array(batch.num_rows())?);
    }
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options).map_err(|e| e.into())
}

/// Runs one instruction group (matched / not_matched / not_matched_by_source) over the rows
/// selected by `group_mask`, producing zero or more output batches. Reproduces Spark's ordered,
/// first-match-wins clause evaluation (`MergeRows`: "the first matching expression is used")
/// via a shrinking `remaining` mask.
fn run_group(
    batch: &RecordBatch,
    group_mask: &BooleanArray,
    instructions: &[MergeInstructionExec],
    schema: &SchemaRef,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    let mut remaining = group_mask.clone();
    let mut out = Vec::new();

    for instr in instructions {
        let cond = eval_bool(&instr.condition, batch)?;
        let fire = and(&remaining, &cond)?;

        if fire.true_count() > 0 {
            let filtered = filter_record_batch(batch, &fire)?;
            for output_exprs in &instr.outputs {
                out.push(project(&filtered, output_exprs, schema)?);
            }
        }

        remaining = and(&remaining, &not(&fire)?)?;
    }

    Ok(out)
}

/// Detects a target row matched by more than one source row (Spark's
/// `MERGE_CARDINALITY_VIOLATION`), mirroring `MergeRowsExec.BitmapCardinalityValidator`: track
/// row ids seen within the matched group and fail on the first repeat.
fn check_cardinality(
    batch: &RecordBatch,
    matched_mask: &BooleanArray,
    row_id_ordinal: usize,
    seen: &mut HashSet<i64>,
) -> Result<(), DataFusionError> {
    let filtered = filter_record_batch(batch, matched_mask)?;
    let row_ids = filtered
        .column(row_id_ordinal)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("MergeRows: row id column must be Int64".to_string())
        })?;

    for i in 0..row_ids.len() {
        if row_ids.is_valid(i) {
            let id = row_ids.value(i);
            if !seen.insert(id) {
                return Err(DataFusionError::Execution(
                    "[MERGE_CARDINALITY_VIOLATION] The ON search condition of the MERGE \
                     statement matched a single row from the target table with multiple rows \
                     of the source table."
                        .to_string(),
                ));
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn process_batch(
    batch: RecordBatch,
    is_source_row_present: &Arc<dyn PhysicalExpr>,
    is_target_row_present: &Arc<dyn PhysicalExpr>,
    matched_instructions: &[MergeInstructionExec],
    not_matched_instructions: &[MergeInstructionExec],
    not_matched_by_source_instructions: &[MergeInstructionExec],
    check_cardinality_flag: bool,
    row_id_ordinal: usize,
    // Caller-owned and threaded across every batch of the partition -- must NOT be created
    // fresh per call, or a cardinality violation split across two batches goes undetected.
    seen: &mut HashSet<i64>,
    schema: &SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let source_present = eval_bool(is_source_row_present, &batch)?;
    let target_present = eval_bool(is_target_row_present, &batch)?;

    let matched_mask = and(&target_present, &source_present)?;
    let not_matched_mask = and(&not(&target_present)?, &source_present)?;
    let not_matched_by_source_mask = and(&target_present, &not(&source_present)?)?;

    if check_cardinality_flag {
        check_cardinality(&batch, &matched_mask, row_id_ordinal, seen)?;
    }

    let mut batches = Vec::new();
    batches.extend(run_group(&batch, &matched_mask, matched_instructions, schema)?);
    batches.extend(run_group(
        &batch,
        &not_matched_mask,
        not_matched_instructions,
        schema,
    )?);
    batches.extend(run_group(
        &batch,
        &not_matched_by_source_mask,
        not_matched_by_source_instructions,
        schema,
    )?);

    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }

    arrow::compute::concat_batches(schema, &batches).map_err(|e| e.into())
}

impl Stream for MergeRowsStream {
    type Item = datafusion::common::Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // `MergeRowsStream` is structurally `Unpin` (every field is), so projecting a plain
        // `&mut Self` out of the `Pin` is sound. Doing so lets us split disjoint field borrows --
        // `&mut this.seen` alongside the other `&this.*` borrows -- so cardinality state
        // accumulates across every batch polled from this stream instead of resetting per batch.
        let this = self.get_mut();
        match this.child_stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(process_batch(
                batch,
                &this.is_source_row_present,
                &this.is_target_row_present,
                &this.matched_instructions,
                &this.not_matched_instructions,
                &this.not_matched_by_source_instructions,
                this.check_cardinality,
                this.row_id_ordinal,
                &mut this.seen,
                &this.schema,
            ))),
            other => other,
        }
    }
}

impl RecordBatchStream for MergeRowsStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::{binary, col, lit};
    use datafusion::logical_expr::Operator as DFOperator;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int64, true),
            Field::new("val", DataType::Int32, true),
            Field::new("target_present", DataType::Boolean, false),
            Field::new("source_present", DataType::Boolean, false),
        ]))
    }

    fn test_batch(
        row_ids: Vec<i64>,
        vals: Vec<i32>,
        target: Vec<bool>,
        source: Vec<bool>,
    ) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(row_ids)),
                Arc::new(Int32Array::from(vals)),
                Arc::new(BooleanArray::from(target)),
                Arc::new(BooleanArray::from(source)),
            ],
        )
        .unwrap()
    }

    fn out_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, true)]))
    }

    fn keep_all() -> MergeInstructionExec {
        MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![col("val", &test_schema()).unwrap()]],
        }
    }

    fn discard_all() -> MergeInstructionExec {
        MergeInstructionExec {
            condition: lit(true),
            outputs: vec![],
        }
    }

    #[test]
    fn keep_matched_discard_rest() {
        // 3 rows: matched, not-matched (insert-only), not-matched-by-source.
        let batch = test_batch(
            vec![1, 2, 3],
            vec![10, 20, 30],
            vec![true, false, true],
            vec![true, true, false],
        );
        let out = process_batch(
            batch,
            &col("source_present", &test_schema()).unwrap(),
            &col("target_present", &test_schema()).unwrap(),
            &[keep_all()],
            &[keep_all()],
            &[discard_all()],
            false,
            0,
            &mut HashSet::new(),
            &out_schema(),
        )
        .unwrap();
        let vals = out
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let mut got: Vec<i32> = vals.iter().flatten().collect();
        got.sort();
        // row 1 (matched, kept) and row 2 (not-matched, inserted); row 3 discarded.
        assert_eq!(got, vec![10, 20]);
    }

    #[test]
    fn first_match_wins_ordering() {
        let batch = test_batch(vec![1], vec![5], vec![true], vec![true]);
        let cond_false = MergeInstructionExec {
            condition: binary(
                col("val", &test_schema()).unwrap(),
                DFOperator::Gt,
                lit(100i32),
                &test_schema(),
            )
            .unwrap(),
            outputs: vec![vec![lit(1i32)]],
        };
        let cond_true = MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![lit(2i32)]],
        };
        let out = process_batch(
            batch,
            &col("source_present", &test_schema()).unwrap(),
            &col("target_present", &test_schema()).unwrap(),
            &[cond_false, cond_true],
            &[],
            &[],
            false,
            0,
            &mut HashSet::new(),
            &out_schema(),
        )
        .unwrap();
        let vals = out
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(vals.value(0), 2);
    }

    #[test]
    fn null_condition_falls_through_to_next_instruction() {
        // Regression test for the NULL-propagation data-loss bug. Models the plan
        // `RewriteMergeIntoTable` actually builds for
        //   MERGE ... WHEN MATCHED AND s.val > 100 THEN UPDATE ...
        // namely a two-instruction matched group whose second entry is the appended catch-all
        // `Keep(TrueLiteral, target.output)`. With `val` NULL the first condition evaluates to
        // NULL, which Spark treats as `false` and falls through to the catch-all; before the
        // `null_to_false` normalization Arrow's NULL-propagating `and`/`not` poisoned the
        // `remaining` mask and the row vanished from the output entirely.
        let batch = RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(Int32Array::from(vec![None::<i32>])),
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(BooleanArray::from(vec![true])),
            ],
        )
        .unwrap();
        let cond_null = MergeInstructionExec {
            condition: binary(
                col("val", &test_schema()).unwrap(),
                DFOperator::Gt,
                lit(100i32),
                &test_schema(),
            )
            .unwrap(),
            outputs: vec![vec![lit(1i32)]],
        };
        let keep_catch_all = MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![lit(2i32)]],
        };
        let out = process_batch(
            batch,
            &col("source_present", &test_schema()).unwrap(),
            &col("target_present", &test_schema()).unwrap(),
            &[cond_null, keep_catch_all],
            &[],
            &[],
            false,
            0,
            &mut HashSet::new(),
            &out_schema(),
        )
        .unwrap();
        assert_eq!(
            out.num_rows(),
            1,
            "row with a NULL clause condition must fall through to the catch-all Keep, not \
             disappear from the rewritten data file"
        );
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(vals.value(0), 2);
    }

    #[test]
    fn null_row_presence_flag_treated_as_false() {
        // The presence flags feed the same mask arithmetic as clause conditions, so a nullable
        // `__row_from_source` / `__row_from_target` must also collapse to `false` rather than
        // NULL -- otherwise the row falls out of all three group masks and is dropped.
        let schema = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int64, true),
            Field::new("val", DataType::Int32, true),
            Field::new("target_present", DataType::Boolean, true),
            Field::new("source_present", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(Int32Array::from(vec![10])),
                Arc::new(BooleanArray::from(vec![Some(true)])),
                Arc::new(BooleanArray::from(vec![None::<bool>])),
            ],
        )
        .unwrap();
        // source NULL -> false, target true => not-matched-by-source group.
        let out = process_batch(
            batch,
            &col("source_present", &schema).unwrap(),
            &col("target_present", &schema).unwrap(),
            &[],
            &[],
            &[MergeInstructionExec {
                condition: lit(true),
                outputs: vec![vec![col("val", &schema).unwrap()]],
            }],
            false,
            0,
            &mut HashSet::new(),
            &out_schema(),
        )
        .unwrap();
        assert_eq!(out.num_rows(), 1);
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(vals.value(0), 10);
    }

    #[test]
    fn cardinality_violation_detected() {
        // Same target row_id (1) matched twice -> must error.
        let batch = test_batch(vec![1, 1], vec![10, 20], vec![true, true], vec![true, true]);
        let matched_mask = BooleanArray::from(vec![true, true]);
        let mut seen = HashSet::new();
        let result = check_cardinality(&batch, &matched_mask, 0, &mut seen);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("MERGE_CARDINALITY_VIOLATION"));
    }

    #[test]
    fn split_produces_two_rows() {
        let batch = test_batch(vec![1], vec![7], vec![true], vec![true]);
        let split = MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![lit(1i32)], vec![lit(2i32)]],
        };
        let out = process_batch(
            batch,
            &col("source_present", &test_schema()).unwrap(),
            &col("target_present", &test_schema()).unwrap(),
            &[split],
            &[],
            &[],
            false,
            0,
            &mut HashSet::new(),
            &out_schema(),
        )
        .unwrap();
        assert_eq!(out.num_rows(), 2);
    }

    #[test]
    fn cardinality_violation_detected_across_batches() {
        // Regression test for the per-batch `seen` reset bug: `process_batch` must accept the
        // caller's `seen` set and mutate it in place so state accumulates across the whole
        // partition. Two separate batches each carry one of two source rows matching the same
        // target row_id=1 -- exactly the case a fresh-per-batch `HashSet` would miss.
        let mut seen = HashSet::new();
        let batch1 = test_batch(vec![1], vec![10], vec![true], vec![true]);
        let batch2 = test_batch(vec![1], vec![20], vec![true], vec![true]);

        let first = process_batch(
            batch1,
            &col("source_present", &test_schema()).unwrap(),
            &col("target_present", &test_schema()).unwrap(),
            &[keep_all()],
            &[],
            &[],
            true,
            0,
            &mut seen,
            &out_schema(),
        );
        assert!(
            first.is_ok(),
            "first batch introduces row_id=1 and should not trip the cardinality check"
        );

        let second = process_batch(
            batch2,
            &col("source_present", &test_schema()).unwrap(),
            &col("target_present", &test_schema()).unwrap(),
            &[keep_all()],
            &[],
            &[],
            true,
            0,
            &mut seen,
            &out_schema(),
        );
        assert!(
            second.is_err(),
            "second batch reuses row_id=1 from a *different* Arrow batch and must trip \
             MERGE_CARDINALITY_VIOLATION"
        );
        assert!(second
            .unwrap_err()
            .to_string()
            .contains("MERGE_CARDINALITY_VIOLATION"));
    }
}
