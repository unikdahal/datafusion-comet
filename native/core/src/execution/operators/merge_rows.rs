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

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, RecordBatch};
use arrow::compute::kernels::boolean::{and, and_not, not};
use arrow::compute::{filter_record_batch, prep_null_mask_filter};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::utils::memory::estimate_memory_size;
use datafusion::common::{DataFusionError, HashMap, ScalarValue};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::{
    execution::TaskContext,
    physical_plan::{
        apply_expression_roots, ChildrenPropertiesMode, DisplayAs, DisplayFormatType,
        ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream, ReplaceChildrenOptions,
        SendableRecordBatchStream,
    },
};
use datafusion_comet_common::{cast_and_stamp_schema, SparkError};
use futures::{Stream, StreamExt};
use roaring::RoaringBitmap;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeActionContext {
    Copy,
    Delete,
    Insert,
    Update,
}

/// A MergeRows instruction: condition plus zero (Discard), one (Keep), or two (Split)
/// output row projections.
#[derive(Debug, Clone)]
pub struct MergeInstructionExec {
    pub condition: Arc<dyn PhysicalExpr>,
    pub outputs: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    pub context: Option<MergeActionContext>,
}

#[derive(Debug, Default)]
struct MergeSemanticMetrics {
    copied: Count,
    inserted: Count,
    deleted: Count,
    updated: Count,
    matched_updated: Count,
    matched_deleted: Count,
    not_matched_by_source_updated: Count,
    not_matched_by_source_deleted: Count,
}

impl MergeSemanticMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            copied: MetricBuilder::new(metrics).counter("numTargetRowsCopied", partition),
            inserted: MetricBuilder::new(metrics).counter("numTargetRowsInserted", partition),
            deleted: MetricBuilder::new(metrics).counter("numTargetRowsDeleted", partition),
            updated: MetricBuilder::new(metrics).counter("numTargetRowsUpdated", partition),
            matched_updated: MetricBuilder::new(metrics)
                .counter("numTargetRowsMatchedUpdated", partition),
            matched_deleted: MetricBuilder::new(metrics)
                .counter("numTargetRowsMatchedDeleted", partition),
            not_matched_by_source_updated: MetricBuilder::new(metrics)
                .counter("numTargetRowsNotMatchedBySourceUpdated", partition),
            not_matched_by_source_deleted: MetricBuilder::new(metrics)
                .counter("numTargetRowsNotMatchedBySourceDeleted", partition),
        }
    }

    fn record_delete(&self, count: usize, source_present: bool) {
        self.deleted.add(count);
        if source_present {
            self.matched_deleted.add(count);
        } else {
            self.not_matched_by_source_deleted.add(count);
        }
    }

    fn record_update(&self, count: usize, source_present: bool) {
        self.updated.add(count);
        if source_present {
            self.matched_updated.add(count);
        } else {
            self.not_matched_by_source_updated.add(count);
        }
    }

    fn record_instruction(
        &self,
        instruction: &MergeInstructionExec,
        count: usize,
        source_present: bool,
    ) -> Result<(), DataFusionError> {
        match (instruction.outputs.len(), instruction.context) {
            (0, None) => self.record_delete(count, source_present),
            (1, Some(MergeActionContext::Copy)) => self.copied.add(count),
            (1, Some(MergeActionContext::Delete)) => self.record_delete(count, source_present),
            (1, Some(MergeActionContext::Insert)) => self.inserted.add(count),
            (1, Some(MergeActionContext::Update)) | (2, None) => {
                self.record_update(count, source_present)
            }
            (1, None) => (),
            (0 | 2, Some(_)) => {
                return Err(DataFusionError::Internal(
                    "MergeRows: action context is invalid for Discard or Split".to_string(),
                ));
            }
            (outputs, _) => {
                return Err(DataFusionError::Internal(format!(
                    "MergeRows: unsupported instruction output count {outputs}"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct MergeConfig {
    is_source_row_present: Arc<dyn PhysicalExpr>,
    is_target_row_present: Arc<dyn PhysicalExpr>,
    matched_instructions: Vec<MergeInstructionExec>,
    not_matched_instructions: Vec<MergeInstructionExec>,
    not_matched_by_source_instructions: Vec<MergeInstructionExec>,
    row_id_ordinal: Option<usize>,
    semantic_metrics_required: bool,
}

impl MergeConfig {
    fn validate(
        &self,
        child: &Arc<dyn ExecutionPlan>,
        output_schema: &SchemaRef,
    ) -> Result<(), DataFusionError> {
        if let Some(ordinal) = self.row_id_ordinal {
            let child_schema = child.schema();
            let child_fields = child_schema.fields().len();
            if ordinal >= child_fields {
                return Err(DataFusionError::Internal(format!(
                    "MergeRows: row id ordinal {ordinal} is out of range for a child with \
                     {child_fields} columns"
                )));
            }
            let data_type = child_schema.field(ordinal).data_type();
            if data_type != &DataType::Int64 {
                return Err(DataFusionError::Internal(format!(
                    "MergeRows: row id column at ordinal {ordinal} must be Int64, got {data_type}"
                )));
            }
        }

        let output_width = output_schema.fields().len();
        for (group, instructions) in [
            ("matched", &self.matched_instructions),
            ("not matched", &self.not_matched_instructions),
            (
                "not matched by source",
                &self.not_matched_by_source_instructions,
            ),
        ] {
            for (instruction_index, instruction) in instructions.iter().enumerate() {
                if instruction.outputs.len() > 2 {
                    return Err(DataFusionError::Internal(format!(
                        "MergeRows: {group} instruction {instruction_index} has {} output rows; expected at most 2",
                        instruction.outputs.len()
                    )));
                }
                let valid_context = match instruction.outputs.len() {
                    0 | 2 => instruction.context.is_none(),
                    1 => instruction.context.is_some() || !self.semantic_metrics_required,
                    _ => false,
                };
                if !valid_context {
                    return Err(DataFusionError::Internal(format!(
                        "MergeRows: {group} instruction {instruction_index} has invalid action context for {} output rows",
                        instruction.outputs.len()
                    )));
                }
                for (output_index, output) in instruction.outputs.iter().enumerate() {
                    if output.len() != output_width {
                        return Err(DataFusionError::Internal(format!(
                            "MergeRows: {group} instruction {instruction_index} output {output_index} has {} expressions; expected {output_width}",
                            output.len()
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct MergeRowsExec {
    config: Arc<MergeConfig>,
    child: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl MergeRowsExec {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        is_source_row_present: Arc<dyn PhysicalExpr>,
        is_target_row_present: Arc<dyn PhysicalExpr>,
        matched_instructions: Vec<MergeInstructionExec>,
        not_matched_instructions: Vec<MergeInstructionExec>,
        not_matched_by_source_instructions: Vec<MergeInstructionExec>,
        row_id_ordinal: Option<usize>,
        child: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> Result<Self, DataFusionError> {
        Self::try_new_with_semantic_metrics(
            is_source_row_present,
            is_target_row_present,
            matched_instructions,
            not_matched_instructions,
            not_matched_by_source_instructions,
            row_id_ordinal,
            false,
            child,
            schema,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_new_with_semantic_metrics(
        is_source_row_present: Arc<dyn PhysicalExpr>,
        is_target_row_present: Arc<dyn PhysicalExpr>,
        matched_instructions: Vec<MergeInstructionExec>,
        not_matched_instructions: Vec<MergeInstructionExec>,
        not_matched_by_source_instructions: Vec<MergeInstructionExec>,
        row_id_ordinal: Option<usize>,
        semantic_metrics_required: bool,
        child: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
    ) -> Result<Self, DataFusionError> {
        let config = Arc::new(MergeConfig {
            is_source_row_present,
            is_target_row_present,
            matched_instructions,
            not_matched_instructions,
            not_matched_by_source_instructions,
            row_id_ordinal,
            semantic_metrics_required,
        });
        config.validate(&child, &schema)?;

        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            config,
            child,
            schema,
            cache,
            metrics: ExecutionPlanMetricsSet::new(),
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
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let [child] = children.as_slice() else {
            return Err(DataFusionError::Internal(format!(
                "MergeRows expects exactly one child, got {}",
                children.len()
            )));
        };
        let child = Arc::clone(child);
        self.config.validate(&child, &self.schema)?;
        Ok(Arc::new(MergeRowsExec {
            config: Arc::clone(&self.config),
            child,
            schema: Arc::clone(&self.schema),
            cache: Arc::clone(&self.cache),
            metrics: self.metrics.clone(),
        }))
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> datafusion::common::Result<TreeNodeRecursion>,
    ) -> datafusion::common::Result<TreeNodeRecursion> {
        let instructions = self
            .config
            .matched_instructions
            .iter()
            .chain(self.config.not_matched_instructions.iter())
            .chain(self.config.not_matched_by_source_instructions.iter());
        let instruction_expressions = instructions.flat_map(|instruction| {
            std::iter::once(&instruction.condition)
                .chain(instruction.outputs.iter().flat_map(|output| output.iter()))
        });

        apply_expression_roots(
            [
                &self.config.is_source_row_present,
                &self.config.is_target_row_present,
            ]
            .into_iter()
            .chain(instruction_expressions),
            f,
        )
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let reservation = self.config.row_id_ordinal.map(|_| {
            MemoryConsumer::new(format!("CometMergeRowsExec[{partition}]"))
                .register(&context.runtime_env().memory_pool)
        });
        let child_stream = self.child.execute(partition, Arc::clone(&context))?;
        let semantic_metrics = self
            .config
            .semantic_metrics_required
            .then(|| MergeSemanticMetrics::new(&self.metrics, partition));
        Ok(Box::pin(MergeRowsStream {
            config: Arc::clone(&self.config),
            child_stream,
            schema: Arc::clone(&self.schema),
            seen: MatchedRowIds::default(),
            reservation,
            baseline: BaselineMetrics::new(&self.metrics, partition),
            semantic_metrics,
        }))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn name(&self) -> &str {
        "CometMergeRowsExec"
    }
}

pub struct MergeRowsStream {
    config: Arc<MergeConfig>,
    child_stream: SendableRecordBatchStream,
    schema: SchemaRef,
    // Partition-scoped so duplicate matches across Arrow batches are still detected.
    seen: MatchedRowIds,
    reservation: Option<MemoryReservation>,
    baseline: BaselineMetrics,
    semantic_metrics: Option<MergeSemanticMetrics>,
}

/// Target row ids already matched in this partition, kept as Spark keeps them: 32-bit roaring
/// bitmaps keyed by the high 32 bits of each id. This is a map of bitmaps rather than one
/// `RoaringTreemap` because admission prices a batch from each touched container's current
/// cardinality before inserting it, which needs a lookup by high bits that the treemap lacks.
#[derive(Default)]
struct MatchedRowIds {
    partitions: HashMap<u32, RoaringBitmap>,
    /// Upper bound of the bytes the bitmaps hold: `PARTITION_FIXED_BYTES` per bitmap plus
    /// `container_bytes` of every container's cardinality.
    bitmap_bytes: usize,
    /// The current batch's ids, sorted. Reused across batches.
    batch: Vec<u64>,
}

// Priced from the roaring format, which every implementation shares, rather than from roaring-rs
// internals. A container holds the values that share their high 16 bits, as a sorted array of u16
// up to 4,096 values and as a 65,536-bit bitset beyond that. Array slots are charged double for Vec
// growth, and each container and bitmap a generous fixed cost for its record and allocations.
// Insertion never creates run containers: roaring-rs only builds them for ranges and `optimize`.
const ARRAY_CONTAINER_LIMIT: u64 = 4096;
const ARRAY_VALUE_BYTES: u64 = 4;
const BITSET_CONTAINER_BYTES: u64 = 8 * 1024;
const CONTAINER_FIXED_BYTES: u64 = 128;
const PARTITION_FIXED_BYTES: u64 = 512;

fn container_bytes(cardinality: u64) -> u64 {
    match cardinality {
        0 => 0,
        c if c <= ARRAY_CONTAINER_LIMIT => CONTAINER_FIXED_BYTES + c * ARRAY_VALUE_BYTES,
        _ => CONTAINER_FIXED_BYTES + BITSET_CONTAINER_BYTES,
    }
}

fn cardinality_memory_overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted(
        "MergeRows: cardinality memory estimate overflow".to_string(),
    )
}

fn to_usize(bytes: u64) -> Result<usize, DataFusionError> {
    usize::try_from(bytes).map_err(|_| cardinality_memory_overflow())
}

/// What inserting a sorted, duplicate-free batch adds to `MatchedRowIds`.
struct BatchGrowth {
    /// Net change in the bytes the bitmaps keep. Negative when a full array becomes a smaller bitset.
    bitmap_delta: i64,
    /// What to admit: the sum of the growing containers, ignoring the ones that shrink, since a
    /// store is allocated before the one it replaces is freed.
    bitmap_growth: u64,
    /// The largest single store rebuilt while inserting, briefly held next to its old copy.
    transient_bytes: u64,
    new_partitions: usize,
}

impl MatchedRowIds {
    fn contains(&self, id: u64) -> bool {
        self.partitions
            .get(&((id >> 32) as u32))
            .is_some_and(|bitmap| bitmap.contains(id as u32))
    }

    fn map_bytes(partitions: usize) -> Result<usize, DataFusionError> {
        estimate_memory_size::<(u32, RoaringBitmap)>(partitions, 0)
    }

    /// Bytes to reserve for this state.
    fn memory_size(&self) -> Result<usize, DataFusionError> {
        Self::map_bytes(self.partitions.len())?
            .checked_add(std::mem::size_of::<Self>())
            .and_then(|bytes| bytes.checked_add(self.bitmap_bytes))
            .and_then(|bytes| bytes.checked_add(self.batch.capacity() * std::mem::size_of::<u64>()))
            .ok_or_else(cardinality_memory_overflow)
    }

    /// Prices inserting `self.batch` from the cardinality of each container it touches.
    fn price_batch(&self) -> BatchGrowth {
        let ids = &self.batch;
        let mut growth = BatchGrowth {
            bitmap_delta: 0,
            bitmap_growth: 0,
            transient_bytes: 0,
            new_partitions: 0,
        };
        let mut index = 0;
        while index < ids.len() {
            let high = (ids[index] >> 32) as u32;
            let bitmap = self.partitions.get(&high);
            if bitmap.is_none() {
                growth.new_partitions += 1;
                growth.bitmap_delta += PARTITION_FIXED_BYTES as i64;
                growth.bitmap_growth += PARTITION_FIXED_BYTES;
            }
            while index < ids.len() && (ids[index] >> 32) as u32 == high {
                let container = ids[index] >> 16;
                let mut end = index + 1;
                while end < ids.len() && ids[end] >> 16 == container {
                    end += 1;
                }
                let start = (container << 16) as u32;
                let existing =
                    bitmap.map_or(0, |bitmap| bitmap.range_cardinality(start..=start | 0xFFFF));
                let projected = existing + (end - index) as u64;
                let delta = container_bytes(projected) as i64 - container_bytes(existing) as i64;
                growth.bitmap_delta += delta;
                growth.bitmap_growth += delta.max(0) as u64;
                growth.transient_bytes = growth.transient_bytes.max(container_bytes(projected));
                index = end;
            }
        }
        growth
    }

    /// Adds `ids` (`count` of them), or fails without changing what has been matched. A duplicate
    /// is reported before any memory is requested, as Spark reports it, so the sort buffer (eight
    /// bytes per row of one batch) is allocated before it is admitted. The bitmaps' growth is
    /// reserved before they change, and the reservation then shrinks to what they keep.
    fn insert_batch(
        &mut self,
        ids: impl Iterator<Item = i64>,
        count: usize,
        reservation: &mut MemoryReservation,
    ) -> Result<(), DataFusionError> {
        if count == 0 {
            return Ok(());
        }
        self.batch.clear();
        self.batch.reserve_exact(count);
        // Casting is a bijection over the i64 bit patterns, so negative ids stay distinct.
        self.batch.extend(ids.map(|id| id as u64));
        self.batch.sort_unstable();
        if self.batch.windows(2).any(|pair| pair[0] == pair[1])
            || self.batch.iter().any(|&id| self.contains(id))
        {
            return Err(cardinality_violation());
        }

        let growth = self.price_batch();
        let partitions = self.partitions.len();
        let grown_partitions = partitions + growth.new_partitions;
        let map_growth = Self::map_bytes(grown_partitions)? - Self::map_bytes(partitions)?;
        // Growing the map rehashes into a new table while the old one is still allocated.
        let map_transient = if grown_partitions > self.partitions.capacity() {
            Self::map_bytes(partitions)?
        } else {
            0
        };
        let admitted = self
            .memory_size()?
            .checked_add(to_usize(growth.bitmap_growth)?)
            .and_then(|bytes| bytes.checked_add(map_growth))
            .and_then(|bytes| bytes.checked_add(map_transient))
            .and_then(|bytes| bytes.checked_add(to_usize(growth.transient_bytes).ok()?))
            .ok_or_else(cardinality_memory_overflow)?;
        reservation.try_resize(admitted)?;

        let mut index = 0;
        while index < self.batch.len() {
            let high = (self.batch[index] >> 32) as u32;
            let bitmap = self.partitions.entry(high).or_default();
            while index < self.batch.len() && (self.batch[index] >> 32) as u32 == high {
                let inserted = bitmap.insert(self.batch[index] as u32);
                debug_assert!(inserted, "duplicates were rejected before insertion");
                index += 1;
            }
        }
        self.bitmap_bytes = usize::try_from(self.bitmap_bytes as i64 + growth.bitmap_delta)
            .expect("the bitmaps never hold a negative number of bytes");
        reservation.resize(self.memory_size()?);
        Ok(())
    }

    #[cfg(test)]
    fn len(&self) -> u64 {
        self.partitions.values().map(RoaringBitmap::len).sum()
    }
}

/// Spark predicates treat NULL as false; Arrow boolean kernels preserve NULL.
fn null_to_false(array: &BooleanArray) -> BooleanArray {
    if array.null_count() == 0 {
        array.clone()
    } else {
        prep_null_mask_filter(array)
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
    // Different instructions can infer different nested nullability for the same output column.
    cast_and_stamp_schema("MergeRows", schema, columns, batch.num_rows())
}

fn filter_or_pass_through(
    batch: &RecordBatch,
    mask: &BooleanArray,
) -> Result<RecordBatch, DataFusionError> {
    if mask.true_count() == batch.num_rows() {
        Ok(batch.clone())
    } else {
        filter_record_batch(batch, mask).map_err(|e| e.into())
    }
}

/// Applies an ordered instruction group with Spark's first-match-wins semantics.
/// Output is grouped by the producing instruction; no physical row ordering is advertised.
fn run_group(
    batch: &RecordBatch,
    group_mask: &BooleanArray,
    instructions: &[MergeInstructionExec],
    schema: &SchemaRef,
    source_present: bool,
    metrics: Option<&MergeSemanticMetrics>,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    if instructions.is_empty() || group_mask.true_count() == 0 {
        return Ok(vec![]);
    }

    // Only rows routed to this group may evaluate its clause predicates.
    let mut current = filter_or_pass_through(batch, group_mask)?;
    let mut out = Vec::new();
    let last = instructions.len() - 1;

    for (idx, instr) in instructions.iter().enumerate() {
        if current.num_rows() == 0 {
            break;
        }

        // Remove claimed rows before evaluating later clauses. This matters for ANSI errors in
        // predicates that Spark would never evaluate after an earlier clause matched.
        let fire = match instr.condition.evaluate(&current)? {
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => {
                BooleanArray::from(vec![true; current.num_rows()])
            }
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(false) | None)) => continue,
            value => value
                .into_array(current.num_rows())?
                .as_any()
                .downcast_ref::<BooleanArray>()
                .map(null_to_false)
                .ok_or_else(|| {
                    DataFusionError::Internal("MergeRows: expected boolean array".to_string())
                })?,
        };

        if fire.true_count() == 0 {
            continue;
        }

        if let Some(metrics) = metrics {
            metrics.record_instruction(instr, fire.true_count(), source_present)?;
        }

        let filtered = filter_or_pass_through(&current, &fire)?;
        for output_exprs in &instr.outputs {
            out.push(project(&filtered, output_exprs, schema)?);
        }

        if idx != last {
            current = if fire.true_count() == current.num_rows() {
                current.slice(0, 0)
            } else {
                filter_record_batch(&current, &not(&fire)?)?
            };
        }
    }

    Ok(out)
}

fn cardinality_violation() -> DataFusionError {
    DataFusionError::External(Box::new(SparkError::MergeCardinalityViolation))
}

/// Detects a target row matched by more than one source row.
fn check_cardinality(
    batch: &RecordBatch,
    matched_mask: &BooleanArray,
    row_id_ordinal: usize,
    seen: &mut MatchedRowIds,
    reservation: &mut MemoryReservation,
) -> Result<(), DataFusionError> {
    let row_ids = batch
        .column(row_id_ordinal)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("MergeRows: row id column must be Int64".to_string())
        })?;
    let matched = matched_mask.values();
    // Spark's row-id read treats a null long slot as zero.
    let ids = matched.set_indices().map(|i| {
        if row_ids.is_null(i) {
            0
        } else {
            row_ids.value(i)
        }
    });
    seen.insert_batch(ids, matched.count_set_bits(), reservation)
}

fn process_batch_with_metrics(
    batch: RecordBatch,
    config: &MergeConfig,
    seen: &mut MatchedRowIds,
    reservation: Option<&mut MemoryReservation>,
    schema: &SchemaRef,
    metrics: Option<&MergeSemanticMetrics>,
) -> Result<RecordBatch, DataFusionError> {
    let source_present = eval_bool(&config.is_source_row_present, &batch)?;
    let target_present = eval_bool(&config.is_target_row_present, &batch)?;

    let matched_mask = and(&target_present, &source_present)?;
    let not_matched_mask = and_not(&source_present, &target_present)?;
    let not_matched_by_source_mask = and_not(&target_present, &source_present)?;

    // Vectorized cardinality validation can surface before an unrelated per-row expression error;
    // both paths fail the query, but the error selected can differ from Spark in that rare case.
    if let Some(row_id_ordinal) = config.row_id_ordinal {
        let reservation = reservation.ok_or_else(|| {
            DataFusionError::Internal(
                "MergeRows: cardinality checking requires a memory reservation".to_string(),
            )
        })?;
        check_cardinality(&batch, &matched_mask, row_id_ordinal, seen, reservation)?;
    }

    let mut batches = Vec::new();
    for (mask, instructions, source_present) in [
        (&matched_mask, &config.matched_instructions, true),
        (&not_matched_mask, &config.not_matched_instructions, true),
        (
            &not_matched_by_source_mask,
            &config.not_matched_by_source_instructions,
            false,
        ),
    ] {
        batches.extend(run_group(
            &batch,
            mask,
            instructions,
            schema,
            source_present,
            metrics,
        )?);
    }

    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }

    arrow::compute::concat_batches(schema, &batches).map_err(|e| e.into())
}

#[cfg(test)]
fn process_batch(
    batch: RecordBatch,
    config: &MergeConfig,
    seen: &mut MatchedRowIds,
    reservation: Option<&mut MemoryReservation>,
    schema: &SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    process_batch_with_metrics(batch, config, seen, reservation, schema, None)
}

// Bound synchronous all-discard processing so a delete-heavy stream yields cooperatively.
const MAX_DISCARDED_BATCHES_PER_POLL: u32 = 128;

impl Stream for MergeRowsStream {
    type Item = datafusion::common::Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut discarded_budget = MAX_DISCARDED_BATCHES_PER_POLL;
        loop {
            let poll = match this.child_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    // Keep elapsed_compute scoped to this operator, not the upstream poll.
                    let _timer = this.baseline.elapsed_compute().timer();
                    let result = process_batch_with_metrics(
                        batch,
                        &this.config,
                        &mut this.seen,
                        this.reservation.as_mut(),
                        &this.schema,
                        this.semantic_metrics.as_ref(),
                    );
                    match result {
                        Ok(batch) if batch.num_rows() == 0 => {
                            discarded_budget -= 1;
                            if discarded_budget == 0 {
                                cx.waker().wake_by_ref();
                                return Poll::Pending;
                            }
                            continue;
                        }
                        other => Poll::Ready(Some(other)),
                    }
                }
                other => other,
            };
            return this.baseline.record_poll(poll);
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
    use arrow::array::{Int32Array, StructArray};
    use arrow::datatypes::{Field, Schema};
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool, UnboundedMemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::logical_expr::Operator as DFOperator;
    use datafusion::physical_expr::expressions::{binary, col, lit};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug, Default)]
    struct RegistrationCountingPool {
        inner: UnboundedMemoryPool,
        merge_rows_registrations: AtomicUsize,
    }

    impl std::fmt::Display for RegistrationCountingPool {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RegistrationCountingPool")
        }
    }

    impl MemoryPool for RegistrationCountingPool {
        fn name(&self) -> &str {
            "RegistrationCountingPool"
        }

        fn register(&self, consumer: &MemoryConsumer) {
            if consumer.name().starts_with("CometMergeRowsExec[") {
                self.merge_rows_registrations.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.register(consumer);
        }

        fn unregister(&self, consumer: &MemoryConsumer) {
            self.inner.unregister(consumer);
            if consumer.name().starts_with("CometMergeRowsExec[") {
                self.merge_rows_registrations.fetch_sub(1, Ordering::SeqCst);
            }
        }

        fn grow(&self, reservation: &MemoryReservation, additional: usize) {
            self.inner.grow(reservation, additional);
        }

        fn shrink(&self, reservation: &MemoryReservation, subtractive: usize) {
            self.inner.shrink(reservation, subtractive);
        }

        fn try_grow(
            &self,
            reservation: &MemoryReservation,
            additional: usize,
        ) -> Result<(), DataFusionError> {
            self.inner.try_grow(reservation, additional)
        }

        fn reserved(&self) -> usize {
            self.inner.reserved()
        }
    }

    fn task_context_with_registration_counter(
        pool: &Arc<RegistrationCountingPool>,
    ) -> Arc<TaskContext> {
        let memory_pool = Arc::clone(pool) as Arc<dyn MemoryPool>;
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(memory_pool)
            .build_arc()
            .unwrap();
        Arc::new(TaskContext::default().with_runtime(runtime))
    }

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

    fn test_reservation() -> MemoryReservation {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        MemoryConsumer::new("test").register(&pool)
    }

    fn out_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, true)]))
    }

    fn keep_all() -> MergeInstructionExec {
        MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![col("val", &test_schema()).unwrap()]],
            context: None,
        }
    }

    fn discard_all() -> MergeInstructionExec {
        MergeInstructionExec {
            condition: lit(true),
            outputs: vec![],
            context: None,
        }
    }

    fn test_config(
        matched_instructions: Vec<MergeInstructionExec>,
        not_matched_instructions: Vec<MergeInstructionExec>,
        not_matched_by_source_instructions: Vec<MergeInstructionExec>,
        row_id_ordinal: Option<usize>,
    ) -> MergeConfig {
        MergeConfig {
            is_source_row_present: col("source_present", &test_schema()).unwrap(),
            is_target_row_present: col("target_present", &test_schema()).unwrap(),
            matched_instructions,
            not_matched_instructions,
            not_matched_by_source_instructions,
            row_id_ordinal,
            semantic_metrics_required: false,
        }
    }

    #[test]
    fn keep_matched_discard_rest() {
        let batch = test_batch(
            vec![1, 2, 3],
            vec![10, 20, 30],
            vec![true, false, true],
            vec![true, true, false],
        );
        let config = test_config(
            vec![keep_all()],
            vec![keep_all()],
            vec![discard_all()],
            None,
        );
        let out = process_batch(
            batch,
            &config,
            &mut MatchedRowIds::default(),
            Some(&mut test_reservation()),
            &out_schema(),
        )
        .unwrap();
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let mut got: Vec<i32> = vals.iter().flatten().collect();
        got.sort();
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
            context: None,
        };
        let cond_true = MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![lit(2i32)]],
            context: None,
        };
        let config = test_config(vec![cond_false, cond_true], vec![], vec![], None);
        let out = process_batch(
            batch,
            &config,
            &mut MatchedRowIds::default(),
            Some(&mut test_reservation()),
            &out_schema(),
        )
        .unwrap();
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(vals.value(0), 2);
    }

    #[test]
    fn later_condition_is_not_evaluated_on_an_already_claimed_row() {
        let batch = test_batch(vec![1, 2], vec![0, 2], vec![true, true], vec![true, true]);
        let claims_zero = MergeInstructionExec {
            condition: binary(
                col("val", &test_schema()).unwrap(),
                DFOperator::Eq,
                lit(0i32),
                &test_schema(),
            )
            .unwrap(),
            outputs: vec![vec![lit(111i32)]],
            context: None,
        };
        let divides_by_val = MergeInstructionExec {
            condition: binary(
                binary(
                    lit(2i32),
                    DFOperator::Divide,
                    col("val", &test_schema()).unwrap(),
                    &test_schema(),
                )
                .unwrap(),
                DFOperator::Gt,
                lit(0i32),
                &test_schema(),
            )
            .unwrap(),
            outputs: vec![vec![lit(222i32)]],
            context: None,
        };
        let config = test_config(vec![claims_zero, divides_by_val], vec![], vec![], None);
        let out = process_batch(
            batch,
            &config,
            &mut MatchedRowIds::default(),
            Some(&mut test_reservation()),
            &out_schema(),
        )
        .unwrap();
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let mut got: Vec<i32> = vals.iter().flatten().collect();
        got.sort();
        assert_eq!(got, vec![111, 222]);
    }

    #[test]
    fn null_condition_falls_through_to_next_instruction() {
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
            context: None,
        };
        let keep_catch_all = MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![lit(2i32)]],
            context: None,
        };
        let config = test_config(vec![cond_null, keep_catch_all], vec![], vec![], None);
        let out = process_batch(
            batch,
            &config,
            &mut MatchedRowIds::default(),
            Some(&mut test_reservation()),
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
        let config = MergeConfig {
            is_source_row_present: col("source_present", &schema).unwrap(),
            is_target_row_present: col("target_present", &schema).unwrap(),
            matched_instructions: vec![],
            not_matched_instructions: vec![],
            not_matched_by_source_instructions: vec![MergeInstructionExec {
                condition: lit(true),
                outputs: vec![vec![col("val", &schema).unwrap()]],
                context: None,
            }],
            row_id_ordinal: None,
            semantic_metrics_required: false,
        };
        let out = process_batch(
            batch,
            &config,
            &mut MatchedRowIds::default(),
            Some(&mut test_reservation()),
            &out_schema(),
        )
        .unwrap();
        assert_eq!(out.num_rows(), 1);
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(vals.value(0), 10);
    }

    #[test]
    fn condition_not_evaluated_outside_its_group() {
        let batch = test_batch(vec![1, 2], vec![0, 5], vec![true, false], vec![true, true]);
        let div_cond = MergeInstructionExec {
            condition: binary(
                binary(
                    lit(10i32),
                    DFOperator::Divide,
                    col("val", &test_schema()).unwrap(),
                    &test_schema(),
                )
                .unwrap(),
                DFOperator::Gt,
                lit(1i32),
                &test_schema(),
            )
            .unwrap(),
            outputs: vec![vec![col("val", &test_schema()).unwrap()]],
            context: None,
        };
        assert!(
            eval_bool(&div_cond.condition, &batch).is_err(),
            "test is only meaningful if batch-wide evaluation of this condition errors"
        );
        let config = test_config(vec![keep_all()], vec![div_cond], vec![], None);
        let out = process_batch(
            batch,
            &config,
            &mut MatchedRowIds::default(),
            Some(&mut test_reservation()),
            &out_schema(),
        )
        .expect("not-matched condition must not be evaluated against the matched row");
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let mut got: Vec<i32> = vals.iter().flatten().collect();
        got.sort();
        assert_eq!(got, vec![0, 5]);
    }

    fn bounded_reservation(limit: usize) -> MemoryReservation {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(limit));
        MemoryConsumer::new("test").register(&pool)
    }

    fn run_cardinality_ids(
        ids: &[i64],
        batch_rows: usize,
        reservation: &mut MemoryReservation,
    ) -> Result<MatchedRowIds, DataFusionError> {
        let mut seen = MatchedRowIds::default();
        for chunk in ids.chunks(batch_rows) {
            let len = chunk.len();
            let batch = test_batch(
                chunk.to_vec(),
                vec![0; len],
                vec![true; len],
                vec![true; len],
            );
            let mask = BooleanArray::from(vec![true; len]);
            check_cardinality(&batch, &mask, 0, &mut seen, reservation)?;
        }
        Ok(seen)
    }

    fn run_cardinality(
        n: usize,
        batch_rows: usize,
        reservation: &mut MemoryReservation,
    ) -> Result<MatchedRowIds, DataFusionError> {
        let ids: Vec<i64> = (0..n as i64).collect();
        run_cardinality_ids(&ids, batch_rows, reservation)
    }

    fn assert_seen_fully_reserved(seen: &MatchedRowIds, reservation: &MemoryReservation) {
        assert_eq!(
            reservation.size(),
            seen.memory_size().unwrap(),
            "the reservation must match the measured cardinality state"
        );
    }

    #[test]
    fn insert_only_merge_does_not_register_memory_consumer() {
        use datafusion::datasource::memory::MemorySourceConfig;

        let source = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let exec = MergeRowsExec::try_new(
            col("source_present", &test_schema()).unwrap(),
            col("target_present", &test_schema()).unwrap(),
            vec![],
            vec![keep_all()],
            vec![],
            None,
            source,
            out_schema(),
        )
        .unwrap();
        let pool = Arc::new(RegistrationCountingPool::default());
        let stream = exec
            .execute(0, task_context_with_registration_counter(&pool))
            .unwrap();

        assert_eq!(
            pool.merge_rows_registrations.load(Ordering::SeqCst),
            0,
            "insert-only MERGE must not register an unused memory consumer"
        );
        drop(stream);
        assert_eq!(pool.merge_rows_registrations.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn cardinality_merge_registers_memory_consumer() {
        use datafusion::datasource::memory::MemorySourceConfig;

        let source = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let exec = MergeRowsExec::try_new(
            col("source_present", &test_schema()).unwrap(),
            col("target_present", &test_schema()).unwrap(),
            vec![keep_all()],
            vec![],
            vec![],
            Some(0),
            source,
            out_schema(),
        )
        .unwrap();
        let pool = Arc::new(RegistrationCountingPool::default());
        let stream = exec
            .execute(0, task_context_with_registration_counter(&pool))
            .unwrap();

        assert_eq!(pool.merge_rows_registrations.load(Ordering::SeqCst), 1);
        drop(stream);
        assert_eq!(
            pool.merge_rows_registrations.load(Ordering::SeqCst),
            0,
            "dropping the stream must unregister the memory consumer"
        );
    }

    #[test]
    fn cardinality_state_is_accounted_to_the_memory_pool() {
        let mut reservation = test_reservation();
        let seen = run_cardinality(9, 4, &mut reservation).unwrap();
        assert!(
            reservation.size() > 0,
            "`seen` must be visible to the memory pool"
        );
        assert_seen_fully_reserved(&seen, &reservation);
    }

    #[test]
    fn cardinality_reservation_tracks_dense_sparse_and_spark_layouts() {
        let dense: Vec<i64> = (0..70_000).collect();
        let sparse: Vec<i64> = (0..20_000).map(|i| i * 200).collect();
        // Spark's monotonically increasing ids: partition id << 33 plus a row counter.
        let spark = |partitions: i64| -> Vec<i64> {
            (0..20_000)
                .map(|i| ((i % partitions) << 33) + i / partitions)
                .collect()
        };
        let mut shuffled = dense.clone();
        // A fixed permutation, so the ids arrive out of order.
        shuffled.sort_by_key(|id| (id * 7_919) % 70_001);
        let signed = vec![i64::MIN, -1, 0, 1, i64::MAX];
        for (name, ids, batch_rows) in [
            ("dense-single-row", dense[..300].to_vec(), 1usize),
            ("dense", dense.clone(), 8192),
            ("sparse", sparse, 257),
            ("spark-200", spark(200), 8192),
            ("spark-16k", spark(16_384), 8192),
            ("shuffled", shuffled, 8192),
            ("signed", signed, 2),
        ] {
            let mut reservation = test_reservation();
            let seen = run_cardinality_ids(&ids, batch_rows, &mut reservation)
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(seen.len(), ids.len() as u64, "{name}");
            assert_seen_fully_reserved(&seen, &reservation);
        }
    }

    #[test]
    fn cardinality_pricing_handles_an_array_becoming_a_bitset() {
        // A container priced as an array of 4,096 values costs more than the bitset it becomes at
        // 4,097, so growth across that boundary is negative and must not be treated as a size.
        assert!(
            container_bytes(ARRAY_CONTAINER_LIMIT) > container_bytes(ARRAY_CONTAINER_LIMIT + 1)
        );
        let ids: Vec<i64> = (0..8_192).collect();
        for batch_rows in [4_096, 4_097, 8_192, 1] {
            let mut reservation = test_reservation();
            let seen = run_cardinality_ids(&ids, batch_rows, &mut reservation).unwrap();
            assert_eq!(seen.len(), 8_192, "batches of {batch_rows}");
            assert_seen_fully_reserved(&seen, &reservation);
        }
    }

    #[test]
    fn cardinality_insertion_never_creates_run_containers() {
        // A full container and a long dense range are where run containers would pay off.
        let ids: Vec<i64> = (0..200_000).collect();
        let mut reservation = test_reservation();
        let seen = run_cardinality_ids(&ids, 8192, &mut reservation).unwrap();
        for bitmap in seen.partitions.values() {
            assert_eq!(bitmap.statistics().n_run_containers, 0);
        }
    }

    #[test]
    fn cardinality_reservation_covers_real_allocations() {
        use crate::alloc_accounting::current_balance;

        // The accounting allocator settles each thread's balance in steps of up to 64 KiB.
        const SLACK: usize = 512 * 1024;
        let spread = |n: i64, shift: u32| -> Vec<i64> { (0..n).map(|i| i << shift).collect() };
        let mut shuffled: Vec<i64> = (0..1_000_000).map(|i| i * 16).collect();
        shuffled.sort_by_key(|id| (id / 16 * 7_919) % 1_000_003);
        for (name, ids) in [
            ("one id per container", spread(60_000, 16)),
            ("one id per bitmap", spread(40_000, 32)),
            ("dense", (0..4_000_000).collect()),
            ("full arrays, shuffled", shuffled),
        ] {
            let base = current_balance();
            let mut reservation = test_reservation();
            let seen = run_cardinality_ids(&ids, 8192, &mut reservation).unwrap();
            let heap = current_balance().saturating_sub(base);
            assert!(
                reservation.size() + SLACK >= heap,
                "{name}: reserved {} bytes, but the state holds {heap}",
                reservation.size()
            );
            drop(seen);
        }
    }

    #[test]
    fn cardinality_admission_failure_leaves_the_state_unchanged() {
        let mut reservation = bounded_reservation(64 * 1024);
        let mut seen =
            run_cardinality_ids(&(0..1_000).collect::<Vec<i64>>(), 8192, &mut reservation).unwrap();
        let (len, bytes, partitions, reserved) = (
            seen.len(),
            seen.bitmap_bytes,
            seen.partitions.len(),
            reservation.size(),
        );

        // A thousand ids in containers of their own need far more than the pool has left.
        let ids: Vec<i64> = (1..1_001).map(|i| i << 16).collect();
        let batch = test_batch(ids, vec![0; 1_000], vec![true; 1_000], vec![true; 1_000]);
        let err = check_cardinality(
            &batch,
            &BooleanArray::from(vec![true; 1_000]),
            0,
            &mut seen,
            &mut reservation,
        )
        .unwrap_err();
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "{err}"
        );
        assert_eq!(seen.len(), len);
        assert_eq!(seen.bitmap_bytes, bytes);
        assert_eq!(seen.partitions.len(), partitions);
        assert_eq!(reservation.size(), reserved);
    }

    #[test]
    fn cardinality_state_is_far_smaller_than_a_hash_set() {
        let n = 1_000_000;
        let mut reservation = test_reservation();
        let seen = run_cardinality(n, 8192, &mut reservation).unwrap();
        // 16 bitset containers, against about 16 MiB for a hash set of a million i64s.
        assert!(
            reservation.size() < 256 * 1024,
            "dense ids reserved {} bytes",
            reservation.size()
        );
        assert_seen_fully_reserved(&seen, &reservation);
    }

    #[test]
    fn cardinality_state_cannot_exceed_a_bounded_pool() {
        // Sparse ids, one per container, need far more than 64 KiB.
        let ids: Vec<i64> = (0..10_000).map(|i| i << 16).collect();
        let mut reservation = bounded_reservation(64 * 1024);
        let err = run_cardinality_ids(&ids, 8192, &mut reservation)
            .err()
            .expect("the pool must reject the state");
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "expected the pool to reject the cardinality state, got {err}"
        );

        let mut probe = test_reservation();
        let needed = run_cardinality_ids(&ids, 8192, &mut probe)
            .unwrap()
            .memory_size()
            .unwrap();
        // Room for the store rebuilt while inserting, briefly held next to its old copy.
        let mut ok_reservation = bounded_reservation(needed + 64 * 1024);
        let seen = run_cardinality_ids(&ids, 8192, &mut ok_reservation).unwrap();
        assert_seen_fully_reserved(&seen, &ok_reservation);
    }

    #[test]
    fn cardinality_violation_wins_over_memory_exhaustion() {
        let mut seen = MatchedRowIds::default();
        let first = test_batch(vec![1], vec![0], vec![true], vec![true]);
        check_cardinality(
            &first,
            &BooleanArray::from(vec![true]),
            0,
            &mut seen,
            &mut test_reservation(),
        )
        .unwrap();

        let mut zero_budget = bounded_reservation(0);
        for (ids, what) in [
            (vec![1], "across batches"),
            (vec![2, 2], "within a batch"),
            (vec![(7 << 32) + 5, (7 << 32) + 5], "in a new partition"),
        ] {
            let len = ids.len();
            let batch = test_batch(ids, vec![0; len], vec![true; len], vec![true; len]);
            let err = check_cardinality(
                &batch,
                &BooleanArray::from(vec![true; len]),
                0,
                &mut seen,
                &mut zero_budget,
            )
            .unwrap_err();
            assert!(
                err.to_string().contains("MERGE_CARDINALITY_VIOLATION"),
                "expected a cardinality violation {what} before pool admission, got {err}"
            );
        }

        let new_id = test_batch(vec![3], vec![0], vec![true], vec![true]);
        let memory_err = check_cardinality(
            &new_id,
            &BooleanArray::from(vec![true]),
            0,
            &mut seen,
            &mut zero_budget,
        )
        .unwrap_err();
        assert!(
            matches!(memory_err, DataFusionError::ResourcesExhausted(_)),
            "expected memory exhaustion for a new id, got {memory_err}"
        );
    }

    #[test]
    fn cardinality_violation_detected() {
        let batch = test_batch(vec![1, 1], vec![10, 20], vec![true, true], vec![true, true]);
        let matched_mask = BooleanArray::from(vec![true, true]);
        let mut seen = MatchedRowIds::default();
        let result =
            check_cardinality(&batch, &matched_mask, 0, &mut seen, &mut test_reservation());
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
            context: None,
        };
        let config = test_config(vec![split], vec![], vec![], None);
        let out = process_batch(
            batch,
            &config,
            &mut MatchedRowIds::default(),
            Some(&mut test_reservation()),
            &out_schema(),
        )
        .unwrap();
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let got: Vec<i32> = vals.iter().flatten().collect();
        assert_eq!(got, vec![1, 2]);
    }

    #[test]
    fn semantic_metrics_count_all_merge_actions_once_per_input_row() {
        let batch = test_batch(
            vec![1, 2, 3, 4, 5, 6],
            vec![10, 11, 12, 13, 14, 15],
            vec![true, true, true, true, false, true],
            vec![true, true, true, true, true, false],
        );
        let clause =
            |value: i32, outputs: Vec<Vec<Arc<dyn PhysicalExpr>>>, context| MergeInstructionExec {
                condition: binary(
                    col("val", &test_schema()).unwrap(),
                    DFOperator::Eq,
                    lit(value),
                    &test_schema(),
                )
                .unwrap(),
                outputs,
                context,
            };
        let value_projection = || vec![col("val", &test_schema()).unwrap()];
        let config = MergeConfig {
            is_source_row_present: col("source_present", &test_schema()).unwrap(),
            is_target_row_present: col("target_present", &test_schema()).unwrap(),
            matched_instructions: vec![
                clause(10, vec![value_projection()], Some(MergeActionContext::Copy)),
                clause(11, vec![], None),
                clause(
                    12,
                    vec![value_projection()],
                    Some(MergeActionContext::Update),
                ),
                clause(13, vec![vec![lit(13i32)], vec![lit(13i32)]], None),
            ],
            not_matched_instructions: vec![clause(
                14,
                vec![value_projection()],
                Some(MergeActionContext::Insert),
            )],
            not_matched_by_source_instructions: vec![clause(
                15,
                vec![value_projection()],
                Some(MergeActionContext::Delete),
            )],
            row_id_ordinal: None,
            semantic_metrics_required: true,
        };
        let metrics = MergeSemanticMetrics::default();
        let output = process_batch_with_metrics(
            batch,
            &config,
            &mut MatchedRowIds::default(),
            Some(&mut test_reservation()),
            &out_schema(),
            Some(&metrics),
        )
        .unwrap();

        assert_eq!(output.num_rows(), 6);
        assert_eq!(metrics.copied.value(), 1);
        assert_eq!(metrics.inserted.value(), 1);
        assert_eq!(metrics.deleted.value(), 2);
        assert_eq!(metrics.updated.value(), 2);
        assert_eq!(metrics.matched_updated.value(), 2);
        assert_eq!(metrics.matched_deleted.value(), 1);
        assert_eq!(metrics.not_matched_by_source_updated.value(), 0);
        assert_eq!(metrics.not_matched_by_source_deleted.value(), 1);
    }

    #[test]
    fn cardinality_violation_detected_for_null_row_ids() {
        let batch = RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![None, None])),
                Arc::new(Int32Array::from(vec![10, 20])),
                Arc::new(BooleanArray::from(vec![true, true])),
                Arc::new(BooleanArray::from(vec![true, true])),
            ],
        )
        .unwrap();
        let matched_mask = BooleanArray::from(vec![true, true]);
        let result = check_cardinality(
            &batch,
            &matched_mask,
            0,
            &mut MatchedRowIds::default(),
            &mut test_reservation(),
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("MERGE_CARDINALITY_VIOLATION"));
    }

    #[tokio::test]
    async fn all_discarded_batch_is_not_emitted() {
        use datafusion::datasource::memory::MemorySourceConfig;
        use datafusion::prelude::SessionContext;

        let discarded = test_batch(vec![1], vec![10], vec![true], vec![true]);
        let kept = test_batch(vec![2], vec![20], vec![false], vec![true]);
        let source =
            MemorySourceConfig::try_new_exec(&[vec![discarded, kept]], test_schema(), None)
                .unwrap();

        let exec = MergeRowsExec::try_new(
            col("source_present", &test_schema()).unwrap(),
            col("target_present", &test_schema()).unwrap(),
            vec![discard_all()],
            vec![keep_all()],
            vec![],
            None,
            source,
            out_schema(),
        )
        .unwrap();

        let ctx = SessionContext::new();
        let mut stream = exec.execute(0, ctx.task_ctx()).unwrap();
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch.unwrap());
        }
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn instruction_with_more_than_two_outputs_is_rejected() {
        use datafusion::datasource::memory::MemorySourceConfig;
        let source = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let invalid = MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![lit(1i32)], vec![lit(2i32)], vec![lit(3i32)]],
            context: None,
        };
        let err = MergeRowsExec::try_new(
            col("source_present", &test_schema()).unwrap(),
            col("target_present", &test_schema()).unwrap(),
            vec![invalid],
            vec![],
            vec![],
            None,
            source,
            out_schema(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("expected at most 2"));
    }

    #[test]
    fn instruction_output_width_must_match_operator_schema() {
        use datafusion::datasource::memory::MemorySourceConfig;
        let source = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let invalid = MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![lit(1i32), lit(2i32)]],
            context: None,
        };
        let err = MergeRowsExec::try_new(
            col("source_present", &test_schema()).unwrap(),
            col("target_present", &test_schema()).unwrap(),
            vec![invalid],
            vec![],
            vec![],
            None,
            source,
            out_schema(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("expected 1"));
    }

    #[test]
    fn out_of_range_row_id_ordinal_is_rejected() {
        use datafusion::datasource::memory::MemorySourceConfig;
        let source = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let err = MergeRowsExec::try_new(
            col("source_present", &test_schema()).unwrap(),
            col("target_present", &test_schema()).unwrap(),
            vec![keep_all()],
            vec![],
            vec![],
            Some(99),
            source,
            out_schema(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("row id ordinal"));
    }

    #[test]
    fn non_int64_row_id_is_rejected_at_plan_construction() {
        use datafusion::datasource::memory::MemorySourceConfig;
        let wrong_schema = Arc::new(Schema::new(vec![
            Field::new("row_id", DataType::Int32, true),
            Field::new("val", DataType::Int32, true),
            Field::new("target_present", DataType::Boolean, false),
            Field::new("source_present", DataType::Boolean, false),
        ]));
        let source =
            MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(&wrong_schema), None).unwrap();
        let err = MergeRowsExec::try_new(
            col("source_present", &wrong_schema).unwrap(),
            col("target_present", &wrong_schema).unwrap(),
            vec![],
            vec![],
            vec![],
            Some(0),
            source,
            out_schema(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("must be Int64"));
    }

    #[test]
    fn replace_children_rejects_ordinal_out_of_range_for_new_child() {
        use datafusion::datasource::memory::MemorySourceConfig;
        let original_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("row_id", DataType::Int64, true),
        ]));
        let source =
            MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(&original_schema), None)
                .unwrap();
        let exec = Arc::new(
            MergeRowsExec::try_new(
                lit(true),
                lit(true),
                vec![],
                vec![],
                vec![],
                Some(3),
                source,
                out_schema(),
            )
            .unwrap(),
        );

        let narrow_schema = Arc::new(Schema::new(vec![Field::new(
            "row_id",
            DataType::Int64,
            true,
        )]));
        let narrow_child =
            MemorySourceConfig::try_new_exec(&[vec![]], narrow_schema, None).unwrap();
        let err = exec
            .replace_children(
                vec![narrow_child],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("out of range"),
            "expected an out-of-range ordinal error, got: {err}"
        );
    }

    #[test]
    fn replace_children_rejects_wrong_arity() {
        use datafusion::datasource::memory::MemorySourceConfig;
        let source = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let exec = Arc::new(
            MergeRowsExec::try_new(
                col("source_present", &test_schema()).unwrap(),
                col("target_present", &test_schema()).unwrap(),
                vec![keep_all()],
                vec![],
                vec![],
                Some(0),
                source,
                out_schema(),
            )
            .unwrap(),
        );
        let no_children = Arc::clone(&exec)
            .replace_children(
                vec![],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .unwrap_err();
        assert!(no_children.to_string().contains("exactly one child"));

        let child_a = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let child_b = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let two_children = exec
            .replace_children(
                vec![child_a, child_b],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .unwrap_err();
        assert!(two_children.to_string().contains("exactly one child"));
    }

    #[test]
    fn replace_children_revalidates_row_id_schema() {
        use datafusion::datasource::memory::MemorySourceConfig;
        let source = MemorySourceConfig::try_new_exec(&[vec![]], test_schema(), None).unwrap();
        let exec = Arc::new(
            MergeRowsExec::try_new(
                col("source_present", &test_schema()).unwrap(),
                col("target_present", &test_schema()).unwrap(),
                vec![keep_all()],
                vec![],
                vec![],
                Some(0),
                source,
                out_schema(),
            )
            .unwrap(),
        );

        let narrow_schema = Arc::new(Schema::new(vec![Field::new(
            "only_col",
            DataType::Int32,
            true,
        )]));
        let narrow_child =
            MemorySourceConfig::try_new_exec(&[vec![]], narrow_schema, None).unwrap();
        let err = exec
            .replace_children(
                vec![narrow_child],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
            .unwrap_err();
        assert!(err.to_string().contains("must be Int64"));
    }

    #[test]
    fn cardinality_violation_detected_across_batches() {
        let mut seen = MatchedRowIds::default();
        let batch1 = test_batch(vec![1], vec![10], vec![true], vec![true]);
        let batch2 = test_batch(vec![1], vec![20], vec![true], vec![true]);
        let config = test_config(vec![keep_all()], vec![], vec![], Some(0));

        let first = process_batch(
            batch1,
            &config,
            &mut seen,
            Some(&mut test_reservation()),
            &out_schema(),
        );
        assert!(first.is_ok());

        let second = process_batch(
            batch2,
            &config,
            &mut seen,
            Some(&mut test_reservation()),
            &out_schema(),
        );
        assert!(second.is_err());
        assert!(second
            .unwrap_err()
            .to_string()
            .contains("MERGE_CARDINALITY_VIOLATION"));
    }

    #[test]
    fn project_reconciles_nested_struct_nullability_with_declared_schema() {
        let source_field = Field::new("n", DataType::Boolean, false);
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(vec![source_field.clone()].into()),
            true,
        )]));
        let inner_values: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
        let payload_array: ArrayRef = Arc::new(StructArray::new(
            vec![source_field].into(),
            vec![inner_values],
            None,
        ));
        let batch = RecordBatch::try_new(Arc::clone(&source_schema), vec![payload_array]).unwrap();

        let target_field = Field::new("n", DataType::Boolean, true);
        let out_schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(vec![target_field].into()),
            true,
        )]));

        let out = project(
            &batch,
            &[col("payload", &source_schema).unwrap()],
            &out_schema,
        )
        .expect("project must reconcile projected nested nullability with the declared schema");
        assert_eq!(
            out.schema().field(0).data_type(),
            out_schema.field(0).data_type()
        );
    }
}
