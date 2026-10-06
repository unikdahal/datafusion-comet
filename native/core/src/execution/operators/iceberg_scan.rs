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

//! Native Iceberg table scan operator using iceberg-rust

use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg::arrow::{RuntimePredicateProvider, ScanMetrics};
use iceberg::Error;
use iceberg::Runtime as IcebergRuntime;

use crate::cloud::s3::credential_bridge::AccessMode;
use crate::execution::jni_api::get_runtime;
use crate::execution::operators::iceberg_common::load_file_io;
use crate::execution::operators::ExecutionError;
use crate::parquet::parquet_support::SparkParquetOptions;
use crate::parquet::schema_adapter::SparkPhysicalExprAdapterFactory;
use datafusion_comet_spark_expr::EvalMode;
use datafusion_physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};
use iceberg::scan::FileScanTask;

/// Iceberg table scan operator that uses iceberg-rust to read Iceberg tables.
///
/// Executes pre-planned FileScanTasks for efficient parallel scanning.
pub struct IcebergScanExec {
    /// Iceberg table metadata location for FileIO initialization
    metadata_location: String,
    /// Output schema after projection
    output_schema: SchemaRef,
    /// Cached execution plan properties
    plan_properties: Arc<PlanProperties>,
    /// Catalog-specific configuration for FileIO. Holds the unfiltered FileIO property bag, which
    /// may contain OAuth tokens, REST `credentials.uri`, and other secrets the credential bridge
    /// needs. Redacted in `Debug` so plan dumps and tracing do not leak credentials.
    catalog_properties: HashMap<String, String>,
    /// Spark V2 catalog name; forwarded as dispatchKey to the credential bridge. Empty when the
    /// table has no catalog identity.
    catalog_name: String,
    /// Pre-planned file scan tasks
    tasks: Vec<FileScanTask>,
    /// Number of data files to read concurrently
    data_file_concurrency_limit: usize,
    /// Execution-time predicate source attached to this scan.
    runtime_predicate_provider: Option<Arc<dyn RuntimePredicateProvider>>,
    /// Field id and direction to order file tasks by, so the producer's bound tightens on the
    /// best files first and statistics reject the rest before they are opened.
    runtime_task_order: Option<(i32, RuntimeScanOrder)>,
    /// Metrics
    metrics: ExecutionPlanMetricsSet,
}

impl IcebergScanExec {
    pub fn new(
        metadata_location: String,
        schema: SchemaRef,
        catalog_properties: HashMap<String, String>,
        catalog_name: String,
        tasks: Vec<FileScanTask>,
        data_file_concurrency_limit: usize,
    ) -> Result<Self, ExecutionError> {
        let output_schema = schema;
        let plan_properties = Self::compute_properties(Arc::clone(&output_schema), 1);

        let metrics = ExecutionPlanMetricsSet::new();

        Ok(Self {
            metadata_location,
            output_schema,
            plan_properties,
            catalog_properties,
            catalog_name,
            tasks,
            data_file_concurrency_limit,
            runtime_predicate_provider: None,
            runtime_task_order: None,
            metrics,
        })
    }

    #[cfg(test)]
    pub(crate) fn runtime_predicate_field_name(&self, output_index: usize) -> Option<String> {
        self.runtime_predicate_field(output_index)
            .map(|(_, name)| name)
    }

    /// The Iceberg field id and name behind output column `output_index`, when every task maps
    /// it to the same top-level int, long or date field.
    pub(crate) fn runtime_predicate_field(&self, output_index: usize) -> Option<(i32, String)> {
        use arrow::datatypes::DataType;
        use iceberg::spec::{PrimitiveType, Type};

        let output = self.output_schema.fields().get(output_index)?;
        let mut field_id = None;
        let mut field_name: Option<String> = None;

        for task in &self.tasks {
            let current_id = *task.project_field_ids().get(output_index)?;
            if field_id.is_some_and(|expected| expected != current_id) {
                return None;
            }
            // Only top-level signed integer fields have a direct output-column mapping.
            // Looking up a nested leaf by id and then binding its short name could prune
            // an unrelated top-level field with that name.
            let field = task
                .schema()
                .as_struct()
                .fields()
                .iter()
                .find(|f| f.id == current_id)?;
            // Runtime references are bound by name using the task's case policy.
            // In particular, case-insensitive binding can resolve `key` and `KEY`
            // to the same id. Never let it change the projected field's identity.
            let bound_field = if task.case_sensitive() {
                task.schema().field_by_name(&field.name)
            } else {
                task.schema().field_by_name_case_insensitive(&field.name)
            }?;
            if bound_field.id != current_id {
                return None;
            }
            if !matches!(
                (output.data_type(), field.field_type.as_ref()),
                (DataType::Int32, Type::Primitive(PrimitiveType::Int))
                    | (DataType::Int64, Type::Primitive(PrimitiveType::Long))
                    | (DataType::Date32, Type::Primitive(PrimitiveType::Date))
            ) {
                return None;
            }
            let current_name = field.name.clone();
            if field_name
                .as_ref()
                .is_some_and(|expected| expected != &current_name)
            {
                return None;
            }
            field_id = Some(current_id);
            field_name = Some(current_name);
        }

        field_id.zip(field_name)
    }

    pub(crate) fn with_runtime_predicate_provider(
        &self,
        runtime_predicate_provider: Arc<dyn RuntimePredicateProvider>,
        task_order: Option<(i32, RuntimeScanOrder)>,
    ) -> Self {
        Self {
            metadata_location: self.metadata_location.clone(),
            output_schema: Arc::clone(&self.output_schema),
            plan_properties: Arc::clone(&self.plan_properties),
            catalog_properties: self.catalog_properties.clone(),
            catalog_name: self.catalog_name.clone(),
            tasks: self.tasks.clone(),
            data_file_concurrency_limit: self.data_file_concurrency_limit,
            runtime_predicate_provider: Some(runtime_predicate_provider),
            runtime_task_order: task_order,
            metrics: self.metrics.clone(),
        }
    }

    fn compute_properties(schema: SchemaRef, num_partitions: usize) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl ExecutionPlan for IcebergScanExec {
    fn name(&self) -> &str {
        "IcebergScanExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    /// The projection expressions are derived per data file inside the stream, not held on the
    /// node, so there is nothing to visit here.
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        self.execute_with_tasks(self.tasks.clone(), context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl IcebergScanExec {
    /// Handles MOR (Merge-On-Read) tables by automatically applying positional and equality
    /// deletes via iceberg-rust's ArrowReader.
    fn execute_with_tasks(
        &self,
        tasks: Vec<FileScanTask>,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let output_schema = Arc::clone(&self.output_schema);
        let file_io = load_file_io(
            &self.catalog_properties,
            &self.metadata_location,
            &self.catalog_name,
            AccessMode::Read,
        )?;
        let batch_size = context.session_config().batch_size();

        let metrics = IcebergScanMetrics::new(&self.metrics);
        metrics.num_splits.add(tasks.len());

        let mut tasks = tasks;
        if let Some((field_id, order)) = self.runtime_task_order {
            order_tasks_for_runtime_bound(&mut tasks, field_id, order);
        }

        // Delete-file sizes are not serialized and arrive as 0 (unknown).
        // iceberg-rust sizes each Parquet delete file once per reader, when a
        // task that is actually read first loads it.
        let task_stream = futures::stream::iter(tasks.into_iter().map(Ok::<_, Error>)).boxed();

        // iceberg-rust's ArrowReader spawns IO/CPU work onto an iceberg::Runtime, which only needs
        // a tokio handle. execute() runs on the JVM-called thread outside any tokio context, so we
        // enter Comet's global runtime to capture its handle (this is where the stream is later
        // polled). Capturing the handle rather than borrowing the runtime keeps it tear-downable
        // via release_runtime.
        let iceberg_runtime = {
            let handle = get_runtime();
            let _guard = handle.enter();
            IcebergRuntime::try_current().map_err(|e| {
                DataFusionError::Execution(format!("Failed to build Iceberg runtime: {e}"))
            })?
        };
        let mut reader_builder = iceberg::arrow::ArrowReaderBuilder::new(file_io, iceberg_runtime)
            .with_batch_size(batch_size)
            .with_data_file_concurrency_limit(self.data_file_concurrency_limit)
            .with_row_selection_enabled(true)
            .with_metadata_size_hint(512 * 1024); // Same as DataFusion's default
        if let Some(provider) = &self.runtime_predicate_provider {
            reader_builder = reader_builder.with_runtime_predicate_provider(Arc::clone(provider));
        }
        let reader = reader_builder.build();

        // Pass all tasks to iceberg-rust at once to utilize its flatten_unordered
        // parallelization, avoiding overhead of single-task streams
        let scan_result = reader.read(task_stream).map_err(|e| {
            DataFusionError::Execution(format!("Failed to read Iceberg tasks: {}", e))
        })?;

        let scan_metrics = scan_result.metrics().clone();
        let stream = scan_result.stream();

        let spark_options = SparkParquetOptions::new(EvalMode::Legacy, "UTC", false);
        let adapter_factory = SparkPhysicalExprAdapterFactory::new(spark_options, None);

        let adapted_stream =
            stream.map_err(|e| DataFusionError::Execution(format!("Iceberg scan error: {}", e)));

        let wrapped_stream = IcebergStreamWrapper {
            inner: adapted_stream,
            schema: output_schema,
            adapter_factory,
            cached: None,
            baseline_metrics: metrics.baseline,
            scan_metrics,
            bytes_scanned: metrics.bytes_scanned,
            runtime_file_tasks_pruned: metrics.runtime_file_tasks_pruned,
            runtime_predicate_tasks: metrics.runtime_predicate_tasks,
            runtime_row_groups_pruned: metrics.runtime_row_groups_pruned,
            runtime_predicate_refreshes: metrics.runtime_predicate_refreshes,
            runtime_row_groups_pruned_live: metrics.runtime_row_groups_pruned_live,
            last_reported_bytes: 0,
            last_reported_runtime_file_tasks_pruned: 0,
            last_reported_runtime_predicate_tasks: 0,
            last_reported_runtime_row_groups_pruned: 0,
            last_reported_runtime_predicate_refreshes: 0,
            last_reported_runtime_row_groups_pruned_live: 0,
        };

        Ok(Box::pin(wrapped_stream))
    }
}

/// Metrics for IcebergScanExec
struct IcebergScanMetrics {
    /// Baseline metrics (output rows, elapsed compute time)
    baseline: BaselineMetrics,
    /// Count of file splits (FileScanTasks) processed
    num_splits: Count,
    /// Total bytes read from storage
    bytes_scanned: Count,
    /// File tasks rejected by runtime predicates before opening the file
    runtime_file_tasks_pruned: Count,
    /// File tasks that used a runtime predicate
    runtime_predicate_tasks: Count,
    /// Row groups skipped by runtime predicate statistics, at task open or live
    runtime_row_groups_pruned: Count,
    /// Runtime predicate publications picked up while a file was being read
    runtime_predicate_refreshes: Count,
    /// Row groups skipped at row-group boundaries after a refresh
    runtime_row_groups_pruned_live: Count,
}

impl IcebergScanMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, 0),
            num_splits: MetricBuilder::new(metrics).counter("num_splits", 0),
            bytes_scanned: MetricBuilder::new(metrics).counter("bytes_scanned", 0),
            runtime_file_tasks_pruned: MetricBuilder::new(metrics)
                .counter("iceberg_runtime_file_tasks_pruned", 0),
            runtime_predicate_tasks: MetricBuilder::new(metrics)
                .counter("iceberg_runtime_predicate_tasks", 0),
            runtime_row_groups_pruned: MetricBuilder::new(metrics)
                .counter("iceberg_runtime_row_groups_pruned", 0),
            runtime_predicate_refreshes: MetricBuilder::new(metrics)
                .counter("iceberg_runtime_predicate_refreshes", 0),
            runtime_row_groups_pruned_live: MetricBuilder::new(metrics)
                .counter("iceberg_runtime_row_groups_pruned_live", 0),
        }
    }
}

/// Wrapper around iceberg-rust's stream that performs schema adaptation.
/// Handles batches from multiple files that may have different Arrow schemas
/// (metadata, field IDs, etc.).
struct IcebergStreamWrapper<S> {
    inner: S,
    schema: SchemaRef,
    /// Factory for creating adapters when file schema changes
    adapter_factory: SparkPhysicalExprAdapterFactory,
    /// Cached adapter and projection expressions for the current file schema,
    /// reused across batches with the same schema
    cached: Option<CachedProjection>,
    /// Metrics for output tracking
    baseline_metrics: BaselineMetrics,
    /// Iceberg scan metrics for bytes read tracking
    scan_metrics: ScanMetrics,
    /// DF metric counter bridging iceberg-rust's bytes_read to the metric tree
    bytes_scanned: Count,
    /// Last reported bytes_read value for delta computation
    last_reported_bytes: u64,
    runtime_file_tasks_pruned: Count,
    runtime_predicate_tasks: Count,
    runtime_row_groups_pruned: Count,
    runtime_predicate_refreshes: Count,
    runtime_row_groups_pruned_live: Count,
    last_reported_runtime_file_tasks_pruned: u64,
    last_reported_runtime_predicate_tasks: u64,
    last_reported_runtime_row_groups_pruned: u64,
    last_reported_runtime_predicate_refreshes: u64,
    last_reported_runtime_row_groups_pruned_live: u64,
}

/// Cached projection state: file schema, adapter, and pre-built projection expressions.
struct CachedProjection {
    file_schema: SchemaRef,
    projection_exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl<S> Stream for IcebergStreamWrapper<S>
where
    S: Stream<Item = DFResult<RecordBatch>> + Unpin,
{
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Time the whole poll (driving the inner reader plus schema adaptation) as elapsed_compute.
        // record_poll only records output rows; without this explicit timer elapsed_compute stays
        // 0, so the Spark "scan time" metric never moves. Clone the Time metric (Arc-backed) so the
        // guard does not hold a borrow of self across the inner poll below.
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer();

        let poll_result = self.inner.poll_next_unpin(cx);

        let result = match poll_result {
            Poll::Ready(Some(Ok(batch))) => {
                let file_schema = batch.schema();

                // Reuse cached projection expressions if file schema hasn't changed.
                // Batches from the same file share the same Arc<Schema> pointer,
                // so pointer equality is sufficient here.
                let projection_exprs = match &self.cached {
                    Some(cached) if Arc::ptr_eq(&cached.file_schema, &file_schema) => {
                        &cached.projection_exprs
                    }
                    _ => {
                        let adapter = self
                            .adapter_factory
                            .create(Arc::clone(&self.schema), Arc::clone(&file_schema))?;
                        let exprs =
                            build_projection_expressions(&self.schema, &adapter).map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "Failed to build projection expressions: {}",
                                    e
                                ))
                            })?;
                        self.cached = Some(CachedProjection {
                            file_schema,
                            projection_exprs: exprs,
                        });
                        &self.cached.as_ref().unwrap().projection_exprs
                    }
                };

                let result = adapt_batch_with_expressions(batch, &self.schema, projection_exprs)
                    .map_err(|e| {
                        DataFusionError::Execution(format!("Batch adaptation failed: {}", e))
                    });

                Poll::Ready(Some(result))
            }
            other => other,
        };

        // Bridge iceberg-rust's live AtomicU64 counter into the DF metric tree
        let current = self.scan_metrics.bytes_read();
        let delta = current - self.last_reported_bytes;
        if delta > 0 {
            self.bytes_scanned.add(delta as usize);
            self.last_reported_bytes = current;
        }

        let current = self.scan_metrics.runtime_file_tasks_pruned();
        let delta = current.saturating_sub(self.last_reported_runtime_file_tasks_pruned);
        if delta > 0 {
            self.runtime_file_tasks_pruned.add(delta as usize);
            self.last_reported_runtime_file_tasks_pruned = current;
        }

        let current_tasks = self.scan_metrics.runtime_predicate_tasks();
        let task_delta = current_tasks.saturating_sub(self.last_reported_runtime_predicate_tasks);
        if task_delta > 0 {
            self.runtime_predicate_tasks.add(task_delta as usize);
            self.last_reported_runtime_predicate_tasks = current_tasks;
        }

        let current_pruned = self.scan_metrics.runtime_row_groups_pruned();
        let pruned_delta =
            current_pruned.saturating_sub(self.last_reported_runtime_row_groups_pruned);
        if pruned_delta > 0 {
            self.runtime_row_groups_pruned.add(pruned_delta as usize);
            self.last_reported_runtime_row_groups_pruned = current_pruned;
        }

        let current = self.scan_metrics.runtime_predicate_refreshes();
        let delta = current.saturating_sub(self.last_reported_runtime_predicate_refreshes);
        if delta > 0 {
            self.runtime_predicate_refreshes.add(delta as usize);
            self.last_reported_runtime_predicate_refreshes = current;
        }

        let current = self.scan_metrics.runtime_row_groups_pruned_live();
        let delta = current.saturating_sub(self.last_reported_runtime_row_groups_pruned_live);
        if delta > 0 {
            self.runtime_row_groups_pruned_live.add(delta as usize);
            self.last_reported_runtime_row_groups_pruned_live = current;
        }

        self.baseline_metrics.record_poll(result)
    }
}

impl<S> RecordBatchStream for IcebergStreamWrapper<S>
where
    S: Stream<Item = DFResult<RecordBatch>> + Unpin,
{
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl DisplayAs for IcebergScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IcebergScanExec: metadata_location={}, num_tasks={}",
            self.metadata_location,
            self.tasks.len()
        )
    }
}

impl fmt::Debug for IcebergScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcebergScanExec")
            .field("metadata_location", &self.metadata_location)
            .field("catalog_name", &self.catalog_name)
            .field(
                "catalog_properties",
                &RedactedProperties(&self.catalog_properties),
            )
            .field("num_tasks", &self.tasks.len())
            .field(
                "runtime_predicate_attached",
                &self.runtime_predicate_provider.is_some(),
            )
            .field(
                "data_file_concurrency_limit",
                &self.data_file_concurrency_limit,
            )
            .finish_non_exhaustive()
    }
}

/// Wraps a property map so `Debug` shows keys but elides values, since the unfiltered FileIO bag
/// may contain bearer tokens and OAuth secrets.
struct RedactedProperties<'a>(&'a HashMap<String, String>);

impl fmt::Debug for RedactedProperties<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut m = f.debug_map();
        for k in self.0.keys() {
            m.key(k).value(&"<redacted>");
        }
        m.finish()
    }
}

/// Build projection expressions that adapt batches from a file schema to the target schema.
///
/// The returned expressions can be cached and reused across multiple batches
/// that share the same file schema, avoiding repeated expression construction.
fn build_projection_expressions(
    target_schema: &SchemaRef,
    adapter: &Arc<dyn PhysicalExprAdapter>,
) -> DFResult<Vec<Arc<dyn PhysicalExpr>>> {
    target_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, _field)| {
            let col_expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new_with_schema(
                target_schema.field(i).name(),
                target_schema.as_ref(),
            )?);
            adapter.rewrite(col_expr)
        })
        .collect::<DFResult<Vec<_>>>()
}

/// Adapt a batch to match the target schema using pre-built projection expressions.
///
/// The caller provides pre-built `projection_exprs` (from [`build_projection_expressions`])
/// which can be cached and reused across multiple batches with the same file schema.
fn adapt_batch_with_expressions(
    batch: RecordBatch,
    target_schema: &SchemaRef,
    projection_exprs: &[Arc<dyn PhysicalExpr>],
) -> DFResult<RecordBatch> {
    // If schemas match, no adaptation needed
    if Arc::ptr_eq(&batch.schema(), target_schema) {
        return Ok(batch);
    }

    // Zero-column projection (e.g. SELECT count(*)), preserve row count
    if projection_exprs.is_empty() {
        return Ok(RecordBatch::try_new_with_options(
            Arc::clone(target_schema),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )?);
    }

    // Evaluate expressions against batch
    let columns: Vec<ArrayRef> = projection_exprs
        .iter()
        .map(|expr| expr.evaluate(&batch)?.into_array(batch.num_rows()))
        .collect::<DFResult<Vec<_>>>()?;

    RecordBatch::try_new(Arc::clone(target_schema), columns).map_err(|e| e.into())
}

/// Direction in which a runtime producer's bound tightens: a top-k or MIN keeps the smallest
/// values (`Ascending`), a descending top-k or MAX the largest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimeScanOrder {
    Ascending { nulls_first: bool },
    Descending { nulls_first: bool },
}

/// Reorders file tasks so the ones holding the producer's best values are read first.
///
/// Read order never changes a top-k or MIN/MAX result, and a single Spark task reads its file
/// tasks in sequence by default. Files sorted by key are otherwise read in ascending order, so
/// a descending top-k or MAX bound only tightens on the last file and prunes nothing. Reading
/// the file with the highest upper bound first sets the bound at once; statistics then reject
/// the remaining files before they are opened.
///
/// Tasks are ranked by the key's whole-file bound (lower bound ascending, or upper bound
/// descending). When nulls sort first they are the best values, so files that hold nulls come
/// first. Splits of one file keep their offsets in the same direction. Tasks without a usable
/// bound keep their relative order after the ranked ones.
pub(crate) fn order_tasks_for_runtime_bound(
    tasks: &mut [FileScanTask],
    field_id: i32,
    order: RuntimeScanOrder,
) {
    use std::cmp::Ordering;

    let (descending, nulls_first) = match order {
        RuntimeScanOrder::Ascending { nulls_first } => (false, nulls_first),
        RuntimeScanOrder::Descending { nulls_first } => (true, nulls_first),
    };
    let field_type = |task: &FileScanTask| {
        task.schema()
            .field_by_id(field_id)
            .and_then(|field| field.field_type.as_primitive_type().cloned())
    };
    // Only bounds typed like the field compare with each other, so the ranking is a total order.
    let bound = |task: &FileScanTask| {
        let metrics = task.file_metrics()?;
        let bounds = if descending {
            metrics.upper_bounds()
        } else {
            metrics.lower_bounds()
        };
        let bound = bounds.get(&field_id)?;
        (Some(bound.data_type()) == field_type(task).as_ref()).then(|| bound.clone())
    };
    let holds_nulls = |task: &FileScanTask| {
        task.file_metrics()
            .and_then(|metrics| metrics.null_value_counts().get(&field_id))
            .is_some_and(|count| *count > 0)
    };
    let mut ranked: Vec<_> = tasks
        .iter()
        .map(|task| (nulls_first && holds_nulls(task), bound(task), task.start()))
        .zip(tasks.iter().cloned())
        .collect();
    ranked.sort_by(
        |((left_nulls, left, left_start), _), ((right_nulls, right, right_start), _)| {
            right_nulls
                .cmp(left_nulls)
                .then_with(|| match (left, right) {
                    (Some(left), Some(right)) => {
                        let order = left.partial_cmp(right).unwrap_or(Ordering::Equal);
                        if descending {
                            order.reverse()
                        } else {
                            order
                        }
                    }
                    (Some(_), None) => Ordering::Less,
                    (None, Some(_)) => Ordering::Greater,
                    (None, None) => Ordering::Equal,
                })
                .then_with(|| {
                    if descending {
                        right_start.cmp(left_start)
                    } else {
                        left_start.cmp(right_start)
                    }
                })
        },
    );
    for (slot, (_, task)) in tasks.iter_mut().zip(ranked) {
        *slot = task;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::encryption::StandardKeyMetadata;
    use iceberg::scan::FileScanTask;
    use iceberg::spec::{DataFileFormat, Schema};

    use super::IcebergScanExec;

    #[test]
    fn runtime_field_mapping_aligns_outputs_after_metadata_columns() {
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use iceberg::metadata_columns::RESERVED_FIELD_ID_FILE;
        use iceberg::spec::{NestedField, PrimitiveType, Type};

        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![NestedField::optional(
                    2,
                    "value",
                    Type::Primitive(PrimitiveType::Long),
                )
                .into()])
                .build()
                .unwrap(),
        );
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(1024)
            .with_start(0)
            .with_length(0)
            .with_data_file_path("/tmp/metadata-first.parquet".into())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(schema)
            .with_project_field_ids(vec![RESERVED_FIELD_ID_FILE, 2])
            .with_case_sensitive(false)
            .build()
            .unwrap();
        let scan = IcebergScanExec::new(
            "/tmp/metadata.json".into(),
            Arc::new(ArrowSchema::new(vec![
                Field::new("_file", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ])),
            Default::default(),
            String::new(),
            vec![task],
            1,
        )
        .unwrap();
        // A projected metadata column keeps its output position, so the data
        // column after it still maps to its own field id.
        assert_eq!(scan.runtime_predicate_field_name(0), None);
        assert_eq!(scan.runtime_predicate_field_name(1), Some("value".into()));
    }

    #[test]
    fn runtime_field_mapping_respects_projection_identity_and_type() {
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use iceberg::spec::{NestedField, PrimitiveType, Type};

        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "value", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );
        let task = |ids| {
            FileScanTask::builder()
                .with_file_size_in_bytes(1024)
                .with_start(0)
                .with_length(0)
                .with_data_file_path("/tmp/schema-only.parquet".into())
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(Arc::clone(&schema))
                .with_project_field_ids(ids)
                .with_case_sensitive(false)
                .build()
                .unwrap()
        };
        let scan = |data_type, tasks| {
            IcebergScanExec::new(
                "/tmp/metadata.json".into(),
                Arc::new(ArrowSchema::new(vec![Field::new("value", data_type, true)])),
                Default::default(),
                String::new(),
                tasks,
                1,
            )
            .unwrap()
        };
        assert_eq!(
            scan(DataType::Int64, vec![task(vec![2])]).runtime_predicate_field_name(0),
            Some("value".into())
        );
        assert_eq!(
            scan(DataType::Int32, vec![task(vec![2])]).runtime_predicate_field_name(0),
            None
        );
        assert_eq!(
            scan(DataType::UInt64, vec![task(vec![2])]).runtime_predicate_field_name(0),
            None
        );
        assert_eq!(
            scan(DataType::Int64, vec![task(vec![2]), task(vec![1])])
                .runtime_predicate_field_name(0),
            None
        );
        assert_eq!(
            scan(DataType::Int64, vec![task(vec![2])]).runtime_predicate_field_name(1),
            None
        );
        assert_eq!(
            scan(DataType::Int64, vec![]).runtime_predicate_field_name(0),
            None
        );

        let nested_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(
                        3,
                        "nested",
                        Type::Struct(iceberg::spec::StructType::new(vec![NestedField::optional(
                            1,
                            "value",
                            Type::Primitive(PrimitiveType::Long),
                        )
                        .into()])),
                    )
                    .into(),
                    NestedField::optional(2, "value", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );
        let nested_task = FileScanTask::builder()
            .with_file_size_in_bytes(1024)
            .with_start(0)
            .with_length(0)
            .with_data_file_path("/tmp/nested.parquet".into())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(nested_schema)
            .with_project_field_ids(vec![1])
            .with_case_sensitive(false)
            .build()
            .unwrap();
        assert_eq!(
            scan(DataType::Int64, vec![nested_task]).runtime_predicate_field_name(0),
            None
        );

        let case_schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "key", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(2, "KEY", Type::Primitive(PrimitiveType::Int)).into(),
                ])
                .build()
                .unwrap(),
        );
        let resolved_id = case_schema
            .field_by_name_case_insensitive("key")
            .unwrap()
            .id;
        let other_id = if resolved_id == 1 { 2 } else { 1 };
        let case_task = FileScanTask::builder()
            .with_file_size_in_bytes(1024)
            .with_start(0)
            .with_length(0)
            .with_data_file_path("/tmp/case.parquet".into())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(case_schema)
            .with_project_field_ids(vec![other_id])
            .with_case_sensitive(false)
            .build()
            .unwrap();
        assert_eq!(
            scan(DataType::Int32, vec![case_task]).runtime_predicate_field_name(0),
            None
        );
    }

    #[test]
    fn issue_5783_projection_rejects_selected_duplicate_root() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
        use datafusion_physical_expr_adapter::PhysicalExprAdapterFactory;

        let physical = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let mut options = super::SparkParquetOptions::new(super::EvalMode::Legacy, "UTC", false);
        options.case_sensitive = true;
        let factory = super::SparkPhysicalExprAdapterFactory::new(options, None);
        for name in ["a", "b"] {
            let target = Arc::new(ArrowSchema::new(vec![Field::new(
                name,
                DataType::Int64,
                false,
            )]));
            let adapter = factory
                .create(Arc::clone(&target), Arc::clone(&physical))
                .unwrap();
            let result = super::build_projection_expressions(&target, &adapter);
            if name == "a" {
                let error = result
                    .expect_err("selected root must be ambiguous")
                    .to_string();
                assert!(error.contains("duplicate"), "{error}");
            } else {
                let batch = super::RecordBatch::try_new(
                    Arc::clone(&physical),
                    vec![
                        Arc::new(Int64Array::from(vec![1])),
                        Arc::new(Int64Array::from(vec![2])),
                        Arc::new(Int64Array::from(vec![3])),
                    ],
                )
                .unwrap();
                let output =
                    super::adapt_batch_with_expressions(batch, &target, &result.unwrap()).unwrap();
                assert_eq!(output.num_rows(), 1);
                assert_eq!(
                    output
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    3
                );
            }
        }
    }

    fn from_hex(s: &str) -> Vec<u8> {
        assert!(s.len().is_multiple_of(2), "odd-length hex string");
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("invalid hex"))
            .collect()
    }

    // iceberg-rust encodes and decodes its own StandardKeyMetadata identically. Confirms the API
    // is present after the rev bump and the [version byte][Avro datum] format round-trips.
    #[test]
    fn standard_key_metadata_roundtrips() {
        let key: Vec<u8> = (0u8..16).collect();
        let aad: &[u8] = b"comet-aad-prefix";
        let km = StandardKeyMetadata::try_new(&key)
            .unwrap()
            .with_aad_prefix(aad);

        let encoded = km.encode().unwrap();
        assert_eq!(encoded[0], 0x01, "expected StandardKeyMetadata V1 marker");

        let decoded = StandardKeyMetadata::decode(&encoded).unwrap();
        assert_eq!(decoded.encryption_key().as_bytes(), key.as_slice());
        assert_eq!(decoded.aad_prefix(), Some(aad));
    }

    // Cross-language gate for the encrypted-read passthrough plan: iceberg-rust must decode the
    // exact StandardKeyMetadata bytes that Iceberg-Java writes into a data file's key_metadata,
    // because Comet forwards those bytes verbatim (no re-encoding, no KMS) into
    // FileScanTask::key_metadata. This fixture is a real Iceberg-Java blob produced by
    // StandardEncryptionManager. To regenerate (e.g. after a wire-format change), print the bytes
    // from CometIcebergEncryptionSuite's plaintext-DEK test and paste them here.
    const JAVA_KEY_METADATA_HEX: &str =
        "012084f49fba77f8ff1da0c115d1e46563cc0220f1d31d62b68808b469eb99fe9c57096000";
    const JAVA_DEK_HEX: &str = "84f49fba77f8ff1da0c115d1e46563cc";

    #[test]
    fn decodes_java_produced_key_metadata() {
        let blob = from_hex(JAVA_KEY_METADATA_HEX);
        let expected_dek = from_hex(JAVA_DEK_HEX);

        let decoded = StandardKeyMetadata::decode(&blob)
            .expect("iceberg-rust failed to decode a Java-produced StandardKeyMetadata blob");
        assert_eq!(
            decoded.encryption_key().as_bytes(),
            expected_dek.as_slice(),
            "DEK recovered by Rust differs from the Java plaintext DEK"
        );
        // The Java blob carries a 16-byte AAD prefix; confirm the optional-field union decodes too.
        assert_eq!(
            decoded.aad_prefix().map(|a| a.len()),
            Some(16),
            "expected a 16-byte AAD prefix from the Java blob"
        );
    }

    #[test]
    fn runtime_order_reads_best_files_first() {
        use std::collections::HashMap;

        use iceberg::scan::FileScanTaskMetrics;
        use iceberg::spec::{Datum, NestedField, PrimitiveType, Type};

        use super::{order_tasks_for_runtime_bound, RuntimeScanOrder};

        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![NestedField::optional(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )
                .into()])
                .build()
                .unwrap(),
        );
        let task = |path: &str, start: u64, bounds: Option<(i32, i32, u64)>| {
            let metrics = bounds.map(|(lower, upper, nulls)| {
                Arc::new(FileScanTaskMetrics::new(
                    Some(10),
                    HashMap::new(),
                    HashMap::from([(1, nulls)]),
                    HashMap::new(),
                    HashMap::from([(1, Datum::int(lower))]),
                    HashMap::from([(1, Datum::int(upper))]),
                ))
            });
            FileScanTask::builder()
                .with_file_size_in_bytes(1024)
                .with_start(start)
                .with_length(0)
                .with_data_file_path(path.into())
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(Arc::clone(&schema))
                .with_project_field_ids(vec![1])
                .with_case_sensitive(false)
                .with_file_metrics(metrics)
                .build()
                .unwrap()
        };
        let tasks = vec![
            task("a", 0, Some((0, 99, 0))),
            task("unknown", 0, None),
            task("b", 0, Some((100, 199, 3))),
            task("c", 0, Some((200, 299, 0))),
            task("c", 100, Some((200, 299, 0))),
        ];
        let order = |order| {
            let mut tasks = tasks.clone();
            order_tasks_for_runtime_bound(&mut tasks, 1, order);
            tasks
                .iter()
                .map(|task| format!("{}@{}", task.data_file_path(), task.start()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            order(RuntimeScanOrder::Descending { nulls_first: false }),
            ["c@100", "c@0", "b@0", "a@0", "unknown@0"]
        );
        assert_eq!(
            order(RuntimeScanOrder::Ascending { nulls_first: false }),
            ["a@0", "b@0", "c@0", "c@100", "unknown@0"]
        );
        // Nulls sort first: the file holding nulls has the best values.
        assert_eq!(
            order(RuntimeScanOrder::Ascending { nulls_first: true }),
            ["b@0", "a@0", "c@0", "c@100", "unknown@0"]
        );
    }
}
