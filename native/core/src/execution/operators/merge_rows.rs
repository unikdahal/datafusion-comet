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
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
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
use roaring::RoaringTreemap;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// A MergeRows instruction: condition plus zero (Discard), one (Keep), or two (Split)
/// output row projections.
#[derive(Debug, Clone)]
pub struct MergeInstructionExec {
    pub condition: Arc<dyn PhysicalExpr>,
    pub outputs: Vec<Vec<Arc<dyn PhysicalExpr>>>,
}

#[derive(Debug)]
struct MergeConfig {
    is_source_row_present: Arc<dyn PhysicalExpr>,
    is_target_row_present: Arc<dyn PhysicalExpr>,
    matched_instructions: Vec<MergeInstructionExec>,
    not_matched_instructions: Vec<MergeInstructionExec>,
    not_matched_by_source_instructions: Vec<MergeInstructionExec>,
    row_id_ordinal: Option<usize>,
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
        let config = Arc::new(MergeConfig {
            is_source_row_present,
            is_target_row_present,
            matched_instructions,
            not_matched_instructions,
            not_matched_by_source_instructions,
            row_id_ordinal,
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
        Ok(Box::pin(MergeRowsStream {
            config: Arc::clone(&self.config),
            child_stream,
            schema: Arc::clone(&self.schema),
            seen: RoaringTreemap::new(),
            reservation,
            baseline: BaselineMetrics::new(&self.metrics, partition),
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
    seen: RoaringTreemap,
    reservation: Option<MemoryReservation>,
    baseline: BaselineMetrics,
}

const SEEN_FIXED_BYTES: usize = std::mem::size_of::<RoaringTreemap>();
// RoaringBitmap::statistics reports backing-store capacities, but not container metadata or the
// BTreeMap nodes used by RoaringTreemap. These constants deliberately over-account that metadata.
// They are accounting estimates, not allocator-exact measurements.
const SEEN_TREEMAP_PARTITION_OVERHEAD_BYTES: usize = 256;
const SEEN_CONTAINER_OVERHEAD_BYTES: usize = 64;

// Admission happens before mutating the roaring state. Array containers may reallocate while the
// old allocation is still live, so use a deliberately loose per-slot bound. A bitmap container has
// fixed backing storage after the array-to-bitmap transition, so only that transition needs payload
// headroom on later batches.
const SEEN_ARRAY_MIN_CAPACITY: u64 = 4;
const SEEN_ARRAY_SLOT_UPPER_BYTES: u64 = 8;
const SEEN_ARRAY_LIMIT: u64 = 4096;
const SEEN_BITMAP_TRANSITION_HEADROOM_BYTES: u64 = 32 * 1024;

fn seen_memory_overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted(
        "MergeRows: cardinality memory estimate overflow".to_string(),
    )
}

fn checked_seen_add(total: usize, additional: usize) -> Result<usize, DataFusionError> {
    total
        .checked_add(additional)
        .ok_or_else(seen_memory_overflow)
}

fn estimate_seen_memory_size(seen: &RoaringTreemap) -> Result<usize, DataFusionError> {
    let mut total = SEEN_FIXED_BYTES;
    for (_, bitmap) in seen.bitmaps() {
        let statistics = bitmap.statistics();
        let payload_bytes = statistics
            .n_bytes_array_containers
            .checked_add(statistics.n_bytes_run_containers)
            .and_then(|bytes| bytes.checked_add(statistics.n_bytes_bitset_containers))
            .ok_or_else(seen_memory_overflow)?;
        let payload_bytes = usize::try_from(payload_bytes).map_err(|_| seen_memory_overflow())?;
        let container_overhead = (statistics.n_containers as usize)
            .checked_mul(SEEN_CONTAINER_OVERHEAD_BYTES)
            .ok_or_else(seen_memory_overflow)?;

        total = checked_seen_add(total, SEEN_TREEMAP_PARTITION_OVERHEAD_BYTES)?;
        total = checked_seen_add(total, container_overhead)?;
        total = checked_seen_add(total, payload_bytes)?;
    }
    Ok(total)
}

fn projected_container_payload_headroom(
    existing_cardinality: u64,
    projected_cardinality: u64,
) -> Result<usize, DataFusionError> {
    let bytes = if existing_cardinality > SEEN_ARRAY_LIMIT {
        // BitmapStore has fixed backing storage. Inserting another value only flips a bit, so
        // repeatedly reserving another bitmap payload would create false memory-pool failures.
        0
    } else if projected_cardinality <= SEEN_ARRAY_LIMIT {
        let capacity = projected_cardinality
            .max(SEEN_ARRAY_MIN_CAPACITY)
            .checked_next_power_of_two()
            .ok_or_else(seen_memory_overflow)?;
        capacity
            .checked_mul(SEEN_ARRAY_SLOT_UPPER_BYTES)
            .ok_or_else(seen_memory_overflow)?
    } else {
        // Cover the one-time array growth plus array-to-bitmap conversion. In roaring 0.11.5 the
        // array stores u16 values and the bitmap payload is 8 KiB. A 4,097th insert can first grow
        // the array allocation and then allocate the bitmap before the old array is dropped, so
        // 32 KiB remains conservative for that transient overlap.
        SEEN_BITMAP_TRANSITION_HEADROOM_BYTES
    };
    usize::try_from(bytes).map_err(|_| seen_memory_overflow())
}

fn estimate_batch_roaring_peak(ids: &[u64]) -> Result<usize, DataFusionError> {
    debug_assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));

    let mut total = SEEN_FIXED_BYTES;
    let mut last_partition = None;
    let mut index = 0usize;

    while index < ids.len() {
        let container_key = ids[index] >> 16;
        let partition = (ids[index] >> 32) as u32;
        if last_partition != Some(partition) {
            total = checked_seen_add(total, SEEN_TREEMAP_PARTITION_OVERHEAD_BYTES)?;
            last_partition = Some(partition);
        }

        let mut end_index = index + 1;
        while end_index < ids.len() && (ids[end_index] >> 16) == container_key {
            end_index += 1;
        }

        let cardinality = u64::try_from(end_index - index).map_err(|_| seen_memory_overflow())?;
        total = checked_seen_add(total, SEEN_CONTAINER_OVERHEAD_BYTES)?;
        total = checked_seen_add(total, projected_container_payload_headroom(0, cardinality)?)?;
        index = end_index;
    }

    Ok(total)
}

fn seen_contains_any_sorted(seen: &RoaringTreemap, ids: &[u64]) -> bool {
    debug_assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));

    let mut bitmaps = seen.bitmaps();
    let mut current = bitmaps.next();
    let mut index = 0usize;

    while index < ids.len() {
        let partition = (ids[index] >> 32) as u32;
        let mut end_index = index + 1;
        while end_index < ids.len() && (ids[end_index] >> 32) as u32 == partition {
            end_index += 1;
        }

        while current
            .as_ref()
            .is_some_and(|(existing_partition, _)| *existing_partition < partition)
        {
            current = bitmaps.next();
        }

        if let Some((existing_partition, bitmap)) = current.as_ref() {
            if *existing_partition == partition
                && ids[index..end_index]
                    .iter()
                    .any(|id| bitmap.contains(*id as u32))
            {
                return true;
            }
        }

        index = end_index;
    }

    false
}

fn estimate_seen_batch_headroom(
    seen: &RoaringTreemap,
    ids: &[u64],
) -> Result<usize, DataFusionError> {
    debug_assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));

    let mut headroom = 0usize;
    let mut last_partition = None;
    let mut index = 0usize;

    while index < ids.len() {
        let container_key = ids[index] >> 16;
        let partition = (ids[index] >> 32) as u32;
        if last_partition != Some(partition) {
            headroom = checked_seen_add(headroom, SEEN_TREEMAP_PARTITION_OVERHEAD_BYTES)?;
            last_partition = Some(partition);
        }

        let mut end_index = index + 1;
        while end_index < ids.len() && (ids[end_index] >> 16) == container_key {
            end_index += 1;
        }

        let incoming = u64::try_from(end_index - index).map_err(|_| seen_memory_overflow())?;
        let start = container_key << 16;
        let end = start | u16::MAX as u64;
        let existing = seen.range_cardinality(start..=end);
        let projected = existing
            .checked_add(incoming)
            .ok_or_else(seen_memory_overflow)?;

        headroom = checked_seen_add(headroom, SEEN_CONTAINER_OVERHEAD_BYTES)?;
        headroom = checked_seen_add(
            headroom,
            projected_container_payload_headroom(existing, projected)?,
        )?;
        index = end_index;
    }

    Ok(headroom)
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

fn reserve_seen_batch(
    seen: &RoaringTreemap,
    ids: &[u64],
    ids_capacity: usize,
    reservation: &mut MemoryReservation,
) -> Result<(), DataFusionError> {
    if ids.is_empty() {
        return Ok(());
    }

    // sync_seen_reservation leaves the reservation equal to the retained roaring estimate after
    // every successful batch. Reuse that cached value instead of rescanning the entire treemap
    // before every admission. Only the first batch needs to size the empty state explicitly.
    let current_bytes = if reservation.size() == 0 {
        estimate_seen_memory_size(seen)?
    } else {
        reservation.size()
    };
    let growth_headroom = estimate_seen_batch_headroom(seen, ids)?;
    let batch_roaring_peak = estimate_batch_roaring_peak(ids)?;
    let ids_bytes = ids_capacity
        .checked_mul(std::mem::size_of::<u64>())
        .ok_or_else(seen_memory_overflow)?;
    let target = current_bytes
        .checked_add(growth_headroom)
        .and_then(|bytes| bytes.checked_add(batch_roaring_peak))
        .and_then(|bytes| bytes.checked_add(ids_bytes))
        .ok_or_else(seen_memory_overflow)?;
    if target > reservation.size() {
        reservation.try_grow(target - reservation.size())?;
    }
    Ok(())
}

fn sync_seen_reservation(
    seen: &RoaringTreemap,
    reservation: &mut MemoryReservation,
) -> Result<(), DataFusionError> {
    let projected_bytes = estimate_seen_memory_size(seen)?;
    if projected_bytes > reservation.size() {
        // This indicates that the pre-admission bound above was too small. The memory already
        // exists, so account it infallibly before surfacing an internal error rather than issuing a
        // post-allocation try_grow.
        let missing = projected_bytes - reservation.size();
        reservation.grow(missing);
        return Err(DataFusionError::Internal(format!(
            "MergeRows: cardinality admission underestimated roaring growth by {missing} bytes"
        )));
    }
    if projected_bytes < reservation.size() {
        reservation.shrink(reservation.size() - projected_bytes);
    }
    Ok(())
}

/// Detects a target row matched by more than one source row.
fn check_cardinality(
    batch: &RecordBatch,
    matched_mask: &BooleanArray,
    row_id_ordinal: usize,
    seen: &mut RoaringTreemap,
    reservation: &mut MemoryReservation,
) -> Result<(), DataFusionError> {
    let row_ids = batch
        .column(row_id_ordinal)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("MergeRows: row id column must be Int64".to_string())
        })?;

    let matched_count = matched_mask.true_count();
    if matched_count == 0 {
        return Ok(());
    }

    // Validate all duplicates before asking the memory pool for more space. This preserves Spark's
    // cardinality-error precedence. Sorting lets us detect batch-local duplicates and probe the
    // existing treemap partition-by-partition instead of doing one BTree lookup per row.
    let mut ids = Vec::with_capacity(matched_count);
    for i in matched_mask.values().set_indices() {
        // Spark's row-id read treats a null long slot as zero.
        let id = if row_ids.is_null(i) {
            0
        } else {
            row_ids.value(i)
        };

        // Casting is a bijection over i64 bit patterns, so negative row ids remain distinct.
        ids.push(id as u64);
    }
    ids.sort_unstable();
    if ids.windows(2).any(|pair| pair[0] == pair[1]) || seen_contains_any_sorted(seen, &ids) {
        return Err(cardinality_violation());
    }

    // Reserve the retained-set growth plus the temporary sorted-id buffer and batch-local roaring
    // state before constructing that state. The owned union then moves whole bitmap partitions
    // where possible instead of repeating a treemap lookup for every row.
    let ids_capacity = ids.capacity();
    reserve_seen_batch(seen, &ids, ids_capacity, reservation)?;
    let batch_seen = RoaringTreemap::from_sorted_iter(ids).map_err(|_| {
        DataFusionError::Internal(
            "MergeRows: sorted cardinality ids unexpectedly failed roaring construction"
                .to_string(),
        )
    })?;
    *seen |= batch_seen;
    sync_seen_reservation(seen, reservation)
}

fn process_batch(
    batch: RecordBatch,
    config: &MergeConfig,
    seen: &mut RoaringTreemap,
    reservation: Option<&mut MemoryReservation>,
    schema: &SchemaRef,
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
    for (mask, instructions) in [
        (&matched_mask, &config.matched_instructions),
        (&not_matched_mask, &config.not_matched_instructions),
        (
            &not_matched_by_source_mask,
            &config.not_matched_by_source_instructions,
        ),
    ] {
        batches.extend(run_group(&batch, mask, instructions, schema)?);
    }

    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }

    arrow::compute::concat_batches(schema, &batches).map_err(|e| e.into())
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
                    let result = process_batch(
                        batch,
                        &this.config,
                        &mut this.seen,
                        this.reservation.as_mut(),
                        &this.schema,
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
        }
    }

    fn discard_all() -> MergeInstructionExec {
        MergeInstructionExec {
            condition: lit(true),
            outputs: vec![],
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
            &mut RoaringTreemap::new(),
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
        };
        let cond_true = MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![lit(2i32)]],
        };
        let config = test_config(vec![cond_false, cond_true], vec![], vec![], None);
        let out = process_batch(
            batch,
            &config,
            &mut RoaringTreemap::new(),
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
        };
        let config = test_config(vec![claims_zero, divides_by_val], vec![], vec![], None);
        let out = process_batch(
            batch,
            &config,
            &mut RoaringTreemap::new(),
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
        };
        let keep_catch_all = MergeInstructionExec {
            condition: lit(true),
            outputs: vec![vec![lit(2i32)]],
        };
        let config = test_config(vec![cond_null, keep_catch_all], vec![], vec![], None);
        let out = process_batch(
            batch,
            &config,
            &mut RoaringTreemap::new(),
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
            }],
            row_id_ordinal: None,
        };
        let out = process_batch(
            batch,
            &config,
            &mut RoaringTreemap::new(),
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
        };
        assert!(
            eval_bool(&div_cond.condition, &batch).is_err(),
            "test is only meaningful if batch-wide evaluation of this condition errors"
        );
        let config = test_config(vec![keep_all()], vec![div_cond], vec![], None);
        let out = process_batch(
            batch,
            &config,
            &mut RoaringTreemap::new(),
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

    fn run_cardinality(
        n: usize,
        batch_rows: usize,
        reservation: &mut MemoryReservation,
    ) -> Result<RoaringTreemap, DataFusionError> {
        let mut seen = RoaringTreemap::new();
        let mut next = 0i64;
        while (next as usize) < n {
            let end = ((next as usize) + batch_rows).min(n) as i64;
            let ids: Vec<i64> = (next..end).collect();
            let len = ids.len();
            let batch = test_batch(ids, vec![0; len], vec![true; len], vec![true; len]);
            let mask = BooleanArray::from(vec![true; len]);
            check_cardinality(&batch, &mask, 0, &mut seen, reservation)?;
            next = end;
        }
        Ok(seen)
    }

    fn assert_seen_fully_reserved(seen: &RoaringTreemap, reservation: &MemoryReservation) {
        let expected = estimate_seen_memory_size(seen).unwrap();
        assert_eq!(
            reservation.size(),
            expected,
            "reservation must track the roaring backing-capacity estimate"
        );
    }

    fn run_cardinality_ids(
        ids: &[i64],
        batch_rows: usize,
        reservation: &mut MemoryReservation,
    ) -> Result<RoaringTreemap, DataFusionError> {
        let mut seen = RoaringTreemap::new();
        for chunk in ids.chunks(batch_rows) {
            let len = chunk.len();
            let batch = test_batch(
                chunk.to_vec(),
                vec![0; len],
                vec![true; len],
                vec![true; len],
            );
            check_cardinality(
                &batch,
                &BooleanArray::from(vec![true; len]),
                0,
                &mut seen,
                reservation,
            )?;
        }
        Ok(seen)
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
    fn cardinality_reservation_tracks_roaring_backing_capacity() {
        for &(n, batch_rows) in &[
            (1usize, 1usize),
            (2, 1),
            (3, 1),
            (7, 1),
            (8, 1),
            (9, 1),
            (17, 1),
            (64, 8),
            (200, 16),
        ] {
            let mut reservation = test_reservation();
            let seen = run_cardinality(n, batch_rows, &mut reservation).unwrap();
            assert_seen_fully_reserved(&seen, &reservation);
        }
    }

    #[test]
    fn cardinality_admission_covers_dense_sparse_and_spark_layouts() {
        let dense: Vec<i64> = (0..5_000).collect();
        let sparse: Vec<i64> = (0..20_000).map(|i| i * 200).collect();
        let spark_2k: Vec<i64> = (0..20_000)
            .map(|i| {
                let partition = i % 2_048;
                let row = i / 2_048;
                (partition << 33) + row
            })
            .collect();
        let spark_16k: Vec<i64> = (0..20_000)
            .map(|i| {
                let partition = i % 16_384;
                let row = i / 16_384;
                (partition << 33) + row
            })
            .collect();

        for (name, ids, batch_rows) in [
            ("dense-single-row", &dense, 1usize),
            ("dense-batched", &dense, 4096),
            ("sparse", &sparse, 257),
            ("spark-2k", &spark_2k, 4096),
            ("spark-16k", &spark_16k, 4096),
        ] {
            let mut reservation = test_reservation();
            let seen = run_cardinality_ids(ids, batch_rows, &mut reservation)
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(seen.len(), ids.len() as u64, "{name}");
            assert_seen_fully_reserved(&seen, &reservation);
        }
    }

    #[test]
    fn bitmap_payload_headroom_is_only_charged_for_the_transition() {
        assert_eq!(
            projected_container_payload_headroom(0, SEEN_ARRAY_LIMIT + 1).unwrap(),
            SEEN_BITMAP_TRANSITION_HEADROOM_BYTES as usize
        );
        assert_eq!(
            projected_container_payload_headroom(SEEN_ARRAY_LIMIT + 1, SEEN_ARRAY_LIMIT + 2)
                .unwrap(),
            0
        );
    }

    #[test]
    fn cardinality_state_is_not_mutated_when_pool_rejects_admission() {
        let mut seen = RoaringTreemap::new();
        let batch = test_batch(vec![1, 2, 3], vec![0; 3], vec![true; 3], vec![true; 3]);
        let mut reservation = bounded_reservation(1);
        let err = check_cardinality(
            &batch,
            &BooleanArray::from(vec![true; 3]),
            0,
            &mut seen,
            &mut reservation,
        )
        .unwrap_err();

        assert!(matches!(err, DataFusionError::ResourcesExhausted(_)));
        assert!(
            seen.is_empty(),
            "pool admission must happen before roaring allocation"
        );
        assert_eq!(reservation.size(), 0);
    }

    #[test]
    fn cardinality_state_respects_a_bounded_pool() {
        let n = 917_505;

        let mut probe_reservation = test_reservation();
        let probe = run_cardinality(n, 4096, &mut probe_reservation).unwrap();
        let needed = estimate_seen_memory_size(&probe).unwrap();
        assert!(
            needed < 16 * 1024 * 1024,
            "dense roaring cardinality state unexpectedly needs {needed} bytes"
        );

        // Allow enough transient admission headroom for the final batch as well as retained state.
        let mut ok_reservation = bounded_reservation(needed + 256 * 1024);
        let seen = run_cardinality(n, 4096, &mut ok_reservation).unwrap();
        assert_eq!(seen.len(), n as u64);
        assert_seen_fully_reserved(&seen, &ok_reservation);
    }

    #[test]
    fn cardinality_violation_wins_over_memory_exhaustion() {
        let mut seen = RoaringTreemap::new();
        let mut warmup_reservation = test_reservation();
        let first = test_batch(vec![1], vec![0], vec![true], vec![true]);
        check_cardinality(
            &first,
            &BooleanArray::from(vec![true]),
            0,
            &mut seen,
            &mut warmup_reservation,
        )
        .unwrap();

        let duplicate = test_batch(vec![1], vec![0], vec![true], vec![true]);
        let mut zero_budget = bounded_reservation(0);
        let duplicate_err = check_cardinality(
            &duplicate,
            &BooleanArray::from(vec![true]),
            0,
            &mut seen,
            &mut zero_budget,
        )
        .unwrap_err();
        assert!(
            duplicate_err
                .to_string()
                .contains("MERGE_CARDINALITY_VIOLATION"),
            "expected cardinality violation before pool admission, got {duplicate_err}"
        );

        let within_batch_duplicate =
            test_batch(vec![2, 2], vec![0, 0], vec![true, true], vec![true, true]);
        let duplicate_err = check_cardinality(
            &within_batch_duplicate,
            &BooleanArray::from(vec![true, true]),
            0,
            &mut seen,
            &mut zero_budget,
        )
        .unwrap_err();
        assert!(
            duplicate_err
                .to_string()
                .contains("MERGE_CARDINALITY_VIOLATION"),
            "expected within-batch cardinality violation before pool admission, got {duplicate_err}"
        );

        let new_id = test_batch(vec![2], vec![0], vec![true], vec![true]);
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
        assert!(
            !seen.contains(2),
            "failed admission must not mutate roaring state"
        );
    }

    #[test]
    fn cardinality_violation_detected() {
        let batch = test_batch(vec![1, 1], vec![10, 20], vec![true, true], vec![true, true]);
        let matched_mask = BooleanArray::from(vec![true, true]);
        let mut seen = RoaringTreemap::new();
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
        };
        let config = test_config(vec![split], vec![], vec![], None);
        let out = process_batch(
            batch,
            &config,
            &mut RoaringTreemap::new(),
            Some(&mut test_reservation()),
            &out_schema(),
        )
        .unwrap();
        let vals = out.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let got: Vec<i32> = vals.iter().flatten().collect();
        assert_eq!(got, vec![1, 2]);
    }

    #[test]
    fn signed_row_ids_remain_distinct_in_cardinality_state() {
        let batch = test_batch(
            vec![i64::MIN, -1, 0, 1, i64::MAX],
            vec![0; 5],
            vec![true; 5],
            vec![true; 5],
        );
        let mask = BooleanArray::from(vec![true; 5]);
        let mut seen = RoaringTreemap::new();
        check_cardinality(&batch, &mask, 0, &mut seen, &mut test_reservation()).unwrap();
        assert_eq!(seen.len(), 5);
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
            &mut RoaringTreemap::new(),
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
        let mut seen = RoaringTreemap::new();
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
