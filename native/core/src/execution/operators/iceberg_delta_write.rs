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

//! Native executor-side Iceberg V2 position-delta writer.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, ArrayRef, BinaryArray, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
    UInt64Array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use futures::TryStreamExt;
use iceberg::arrow::{arrow_struct_to_literal, type_to_arrow_type};
use iceberg::arrow::delete_file_loader::PositionDeleteIndexLoader;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTaskDeleteFile;
use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, Literal, ManifestWriterBuilder, PartitionKey,
    PartitionSpecRef, SchemaRef as IcebergSchemaRef, Struct as IcebergStruct, StructType, Type,
};
use iceberg::writer::base_writer::position_delete_writer::{
    position_delete_schema, PositionDeleteFileWriter, PositionDeleteFileWriterBuilder,
};
use iceberg::writer::base_writer::sorting_position_only_delete_writer::{
    SortingPositionOnlyDeleteWriter, SortingPositionOnlyDeleteWriterBuilder,
};
use iceberg::writer::file_writer::location_generator::DefaultFileNameGenerator;
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use prost::Message;

use datafusion_comet_proto::spark_operator::{
    IcebergDeltaCommand, IcebergDeltaTaskPayload, IcebergDeltaWrite,
    IcebergPreviousDeleteFileDescriptor, IcebergPreviousDeletes, IcebergTaskManifest,
    IcebergWriterMode as ProtoIcebergWriterMode,
};

use crate::cloud::s3::credential_bridge::AccessMode;
use crate::execution::operators::iceberg_common::load_file_io;
use crate::execution::operators::iceberg_partition_path::CometLocationGenerator;
use crate::execution::operators::iceberg_write::{
    abort_guard_with_shared_locations, build_output_schema,
    build_writer_properties, file_name_prefix, manifest_partition_spec, parse_iceberg_schema,
    parse_partition_spec, run_write_task, TrackingLocationGenerator,
};

type DeleteRollingBuilder = RollingFileWriterBuilder<
    ParquetWriterBuilder,
    TrackingLocationGenerator,
    DefaultFileNameGenerator,
>;
type OrderedDeleteWriter =
    PositionDeleteFileWriter<ParquetWriterBuilder, TrackingLocationGenerator, DefaultFileNameGenerator>;
type SortingDeleteWriter = SortingPositionOnlyDeleteWriter<
    ParquetWriterBuilder,
    TrackingLocationGenerator,
    DefaultFileNameGenerator,
>;

#[derive(Clone)]
struct HistoricalSpec {
    spec: PartitionSpecRef,
    projection: Vec<usize>,
    partition_type: StructType,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct DeleteKey {
    spec_id: i32,
    partition: IcebergStruct,
    // Present only for FILE granularity. A file-scoped delete file must never mix target files.
    data_file_path: Option<String>,
}

enum DeleteWriter {
    Ordered(OrderedDeleteWriter),
    Sorting(SortingDeleteWriter),
}

struct DeleteRouter {
    ordered: bool,
    file_granularity: bool,
    writers: HashMap<DeleteKey, DeleteWriter>,
    active_key: Option<DeleteKey>,
    closed_keys: HashSet<DeleteKey>,
    last_position: HashMap<DeleteKey, (String, i64)>,
    referenced_data_files: HashSet<String>,
    previous_deletes: HashMap<String, Vec<IcebergPreviousDeleteFileDescriptor>>,
    rewritten_delete_files: HashSet<String>,
    delete_file_loader: PositionDeleteIndexLoader,
    completed: HashMap<i32, Vec<DataFile>>,
    iceberg_schema: IcebergSchemaRef,
    historical_specs: HashMap<i32, HistoricalSpec>,
    file_io: FileIO,
    writer_properties: Arc<parquet::file::properties::WriterProperties>,
    target_size: usize,
    delete_data_location: String,
    operation_id: String,
    partition_id: i32,
    task_attempt_id: i64,
    next_writer_id: usize,
    tracked_locations: Arc<Mutex<Vec<String>>>,
}

impl DeleteRouter {
    async fn route_batch(
        &mut self,
        batch: &RecordBatch,
        layout: &ResolvedLayout,
        operation_ordinal: usize,
        delete_operation: i32,
        delete_rows: &[usize],
    ) -> DFResult<()> {
        let operations = downcast::<Int32Array>(batch.column(operation_ordinal), "operation")?;
        let paths = downcast::<StringArray>(batch.column(layout.file_path), "_file")?;
        let positions = downcast::<Int64Array>(batch.column(layout.row_position), "_pos")?;
        let spec_ids = downcast::<Int32Array>(batch.column(layout.spec_id), "_spec_id")?;
        let partition_values = downcast::<StructArray>(batch.column(layout.partition), "_partition")?;

        // Validate and project the whole batch before opening or writing any file for it.
        let mut rows = Vec::with_capacity(delete_rows.len());
        for &row in delete_rows {
            if row >= batch.num_rows() {
                return Err(DataFusionError::Internal(format!(
                    "Native Iceberg DELETE row index {row} exceeds batch length {}",
                    batch.num_rows()
                )));
            }
            if operations.is_null(row) || operations.value(row) != delete_operation {
                return Err(DataFusionError::Execution(format!(
                    "Native Iceberg DELETE accepts only operation code {delete_operation}; row {row} had {}",
                    if operations.is_null(row) { "NULL".to_string() } else { operations.value(row).to_string() }
                )));
            }
            if paths.is_null(row) {
                return Err(DataFusionError::Execution(format!(
                    "Native Iceberg DELETE row {row} has a null _file path"
                )));
            }
            if positions.is_null(row) || positions.value(row) < 0 {
                return Err(DataFusionError::Execution(format!(
                    "Native Iceberg DELETE row {row} has a null or negative _pos"
                )));
            }
            if spec_ids.is_null(row) {
                return Err(DataFusionError::Execution(format!(
                    "Native Iceberg DELETE row {row} has a null _spec_id"
                )));
            }
            let spec_id = spec_ids.value(row);
            let historical = self.historical_specs.get(&spec_id).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Native Iceberg DELETE references unknown partition spec {spec_id}"
                ))
            })?;
            if partition_values.is_null(row) {
                return Err(DataFusionError::Execution(format!(
                    "Native Iceberg DELETE row {row} has a null _partition struct"
                )));
            }
            let partition = project_partition(partition_values, row, historical)?;
            let path = paths.value(row).to_owned();
            rows.push((
                DeleteKey {
                    spec_id,
                    partition,
                    data_file_path: self.file_granularity.then(|| path.clone()),
                },
                path,
                positions.value(row),
            ));
        }

        for (key, path, position) in rows {
            self.write_position(key, path, position).await?;
        }
        Ok(())
    }

    async fn write_position(
        &mut self,
        key: DeleteKey,
        path: String,
        position: i64,
    ) -> DFResult<()> {
        self.referenced_data_files.insert(path.clone());
        if key
            .data_file_path
            .as_deref()
            .is_some_and(|target| target != path.as_str())
        {
            return Err(DataFusionError::Internal(format!(
                "FILE-granularity delete key {:?} received a row for {path}",
                key.data_file_path
            )));
        }
        if self.ordered {
            if self.active_key.as_ref() != Some(&key) {
                if let Some(previous) = self.active_key.replace(key.clone()) {
                    self.closed_keys.insert(previous.clone());
                    if let Some(mut writer) = self.writers.remove(&previous) {
                        let files = match &mut writer {
                            DeleteWriter::Ordered(writer) => writer.close().await,
                            DeleteWriter::Sorting(writer) => writer.close().await,
                        }
                        .map_err(iceberg_err)?;
                        self.completed
                            .entry(previous.spec_id)
                            .or_default()
                            .extend(files);
                    }
                }
                if self.closed_keys.contains(&key) {
                    return Err(DataFusionError::Execution(format!(
                        "Ordered Iceberg position deletes revisited closed spec/partition key {:?}",
                        key
                    )));
                }
            }
            if let Some((last_path, last_position)) = self.last_position.get(&key) {
                let path_order = path.encode_utf16().cmp(last_path.encode_utf16());
                if path_order.is_lt() || (path_order.is_eq() && position < *last_position) {
                    return Err(DataFusionError::Execution(format!(
                        "Ordered Iceberg position deletes are not sorted: ({path}, {position}) follows ({last_path}, {last_position})"
                    )));
                }
                if path_order.is_eq() && position == *last_position {
                    return Ok(());
                }
            }
            self.last_position.insert(key.clone(), (path.clone(), position));
        }

        if !self.writers.contains_key(&key) {
            let writer = self.build_writer(&key).await?;
            self.writers.insert(key.clone(), writer);
        }
        match self.writers.get_mut(&key).expect("writer was inserted") {
            DeleteWriter::Ordered(writer) => {
                let schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&position_delete_schema()).map_err(iceberg_err)?);
                let batch = RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(StringArray::from(vec![path])),
                        Arc::new(Int64Array::from(vec![position])),
                    ],
                )
                .map_err(DataFusionError::from)?;
                writer.write(batch).await.map_err(iceberg_err)
            }
            DeleteWriter::Sorting(writer) => writer.write_delete(path, position).map_err(iceberg_err),
        }
    }

    async fn build_writer(&mut self, key: &DeleteKey) -> DFResult<DeleteWriter> {
        let historical = self.historical_specs.get(&key.spec_id).ok_or_else(|| {
            DataFusionError::Internal(format!("Missing historical spec {}", key.spec_id))
        })?;
        let partition_key = PartitionKey::new(
            (*historical.spec).clone(),
            Arc::clone(&self.iceberg_schema),
            key.partition.clone(),
        );
        let location_generator = TrackingLocationGenerator::with_shared_locations(
            CometLocationGenerator::try_new(
                self.delete_data_location.clone(),
                &historical.spec,
                &self.iceberg_schema,
            )
            .map_err(iceberg_err)?,
            Arc::clone(&self.tracked_locations),
        );
        let suffix = format!("{}-delta-{}", self.operation_id, self.next_writer_id);
        self.next_writer_id += 1;
        let file_name_generator = DefaultFileNameGenerator::new(
            file_name_prefix(self.partition_id, self.task_attempt_id, &suffix),
            Some("deletes".to_string()),
            iceberg::spec::DataFileFormat::Parquet,
        );
        let parquet_builder = ParquetWriterBuilder::new(
            self.writer_properties.as_ref().clone(),
            position_delete_schema(),
        );
        let rolling_builder: DeleteRollingBuilder = RollingFileWriterBuilder::new(
            parquet_builder,
            self.target_size,
            self.file_io.clone(),
            location_generator,
            file_name_generator,
        );
        if self.file_granularity {
            let mut writer = SortingPositionOnlyDeleteWriterBuilder::new(rolling_builder)
                .build(Some(partition_key))
                .await
                .map_err(iceberg_err)?;
            if let Some(data_file_path) = &key.data_file_path {
                if let Some(previous) = self.previous_deletes.get(data_file_path) {
                    for descriptor in previous {
                        let scan_delete = previous_delete_scan_task(descriptor)?;
                        let positions = self
                            .delete_file_loader
                            .load_file_scoped_positions(&scan_delete, data_file_path)
                            .await
                            .map_err(iceberg_err)?;
                        for position in positions.iter() {
                            let position = i64::try_from(position).map_err(|_| {
                                DataFusionError::Execution(format!(
                                    "Previous position delete {} contains position {position} above i64::MAX",
                                    descriptor.location
                                ))
                            })?;
                            writer
                                .write_delete(data_file_path.clone(), position)
                                .map_err(iceberg_err)?;
                        }
                        self.rewritten_delete_files
                            .insert(descriptor.location.clone());
                    }
                }
            }
            Ok(DeleteWriter::Sorting(writer))
        } else if self.ordered {
            let builder = PositionDeleteFileWriterBuilder::new(rolling_builder);
            Ok(DeleteWriter::Ordered(
                builder.build(Some(partition_key)).await.map_err(iceberg_err)?,
            ))
        } else {
            let builder = SortingPositionOnlyDeleteWriterBuilder::new(rolling_builder);
            Ok(DeleteWriter::Sorting(
                builder.build(Some(partition_key)).await.map_err(iceberg_err)?,
            ))
        }
    }

    async fn close(
        mut self,
    ) -> DFResult<(HashMap<i32, Vec<DataFile>>, Vec<String>, Vec<String>)> {
        for (key, mut writer) in self.writers.drain() {
            let files = match &mut writer {
                DeleteWriter::Ordered(writer) => writer.close().await,
                DeleteWriter::Sorting(writer) => writer.close().await,
            }
            .map_err(iceberg_err)?;
            self.completed.entry(key.spec_id).or_default().extend(files);
        }
        let mut referenced = self.referenced_data_files.into_iter().collect::<Vec<_>>();
        referenced.sort();
        let mut rewritten = self.rewritten_delete_files.into_iter().collect::<Vec<_>>();
        rewritten.sort();
        Ok((self.completed, referenced, rewritten))
    }
}

struct ResolvedLayout {
    data_columns: Vec<usize>,
    file_path: usize,
    row_position: usize,
    spec_id: usize,
    partition: usize,
}

#[derive(Debug)]
struct DeltaSemanticMetrics {
    updated: Count,
    deleted: Count,
    copied: Count,
}

impl DeltaSemanticMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            updated: MetricBuilder::new(metrics).counter("numUpdatedRows", partition),
            deleted: MetricBuilder::new(metrics).counter("numDeletedRows", partition),
            copied: MetricBuilder::new(metrics).counter("numCopiedRows", partition),
        }
    }
}

/// Native Iceberg V2 position-delta writer. Spark and Iceberg still own validation and commit.
pub struct IcebergDeltaWriteExec {
    input: Arc<dyn ExecutionPlan>,
    proto: Arc<IcebergDeltaWrite>,
    common: Arc<datafusion_comet_proto::spark_operator::IcebergWriteCommon>,
    iceberg_schema: IcebergSchemaRef,
    current_spec: PartitionSpecRef,
    historical_specs: HashMap<i32, HistoricalSpec>,
    previous_deletes: HashMap<String, Vec<IcebergPreviousDeleteFileDescriptor>>,
    file_granularity: bool,
    resolved_layout: ResolvedLayout,
    data_input_schema: SchemaRef,
    delete_writer_properties: Arc<parquet::file::properties::WriterProperties>,
    data_writer_properties: Arc<parquet::file::properties::WriterProperties>,
    data_writer_mode: ProtoIcebergWriterMode,
    output_schema: SchemaRef,
    plan_properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl IcebergDeltaWriteExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, proto: IcebergDeltaWrite) -> DFResult<Self> {
        use datafusion_comet_proto::spark_operator::IcebergDeleteGranularity;

        let common = proto.data_common.clone().ok_or_else(|| {
            DataFusionError::Plan("IcebergDeltaWrite missing data_common".into())
        })?;
        if proto.format_version != 2 {
            return Err(DataFusionError::Plan(format!(
                "IcebergDeltaWrite requires format version 2, got {}",
                proto.format_version
            )));
        }
        let command = IcebergDeltaCommand::try_from(proto.command).map_err(|_| {
            DataFusionError::Plan(format!("Unknown Iceberg delta command {}", proto.command))
        })?;
        if !matches!(
            command,
            IcebergDeltaCommand::IcebergDeltaCommandDelete
                | IcebergDeltaCommand::IcebergDeltaCommandUpdate
                | IcebergDeltaCommand::IcebergDeltaCommandMerge
        ) {
            return Err(DataFusionError::Plan(
                "Native Iceberg delta requires a DELETE, UPDATE, or MERGE command".into(),
            ));
        }
        let delete_granularity = IcebergDeleteGranularity::try_from(proto.delete_granularity)
            .map_err(|_| {
                DataFusionError::Plan(format!(
                    "Unknown Iceberg delete granularity {}",
                    proto.delete_granularity
                ))
            })?;
        let file_granularity = match delete_granularity {
            IcebergDeleteGranularity::IcebergDeleteGranularityPartition => false,
            IcebergDeleteGranularity::IcebergDeleteGranularityFile => true,
        };
        if !file_granularity
            && !proto.previous_deletes_blob.is_empty()
        {
            return Err(DataFusionError::Plan(
                "IcebergDeltaWrite carries previous deletes for PARTITION granularity".into(),
            ));
        }
        let previous_delete_message = IcebergPreviousDeletes::decode(
            proto.previous_deletes_blob.as_slice(),
        )
        .map_err(|error| {
            DataFusionError::Plan(format!(
                "IcebergDeltaWrite previous delete descriptor is invalid: {error}"
            ))
        })?;
        let layout = proto.input_layout.as_ref().ok_or_else(|| {
            DataFusionError::Plan("IcebergDeltaWrite missing input_layout".into())
        })?;
        let metadata = layout.metadata_layout.as_ref().ok_or_else(|| {
            DataFusionError::Plan("IcebergDeltaWrite missing metadata_layout".into())
        })?;
        let input_schema = input.schema();
        let width = input_schema.fields().len();
        let operation = checked_ordinal(layout.operation_ordinal, width, "operation")?;
        let row_id = layout.row_id_ordinals.as_slice();
        let metadata_ordinals = layout.metadata_ordinals.as_slice();
        let file_path = projected_ordinal(row_id, metadata.file_path_index, width, "_file")?;
        let row_position =
            projected_ordinal(row_id, metadata.row_position_index, width, "_pos")?;
        let spec_id_index = metadata.spec_id_index.ok_or_else(|| {
            DataFusionError::Plan("IcebergDeltaWrite missing _spec_id projection".into())
        })?;
        let partition_index = metadata.partition_index.ok_or_else(|| {
            DataFusionError::Plan("IcebergDeltaWrite missing _partition projection".into())
        })?;
        let spec_id = projected_ordinal(metadata_ordinals, spec_id_index, width, "_spec_id")?;
        let partition =
            projected_ordinal(metadata_ordinals, partition_index, width, "_partition")?;
        expect_type(&input_schema, operation, &DataType::Int32, "operation")?;
        expect_type(&input_schema, file_path, &DataType::Utf8, "_file")?;
        expect_type(&input_schema, row_position, &DataType::Int64, "_pos")?;
        expect_type(&input_schema, spec_id, &DataType::Int32, "_spec_id")?;
        let partition_width = match input_schema.field(partition).data_type() {
            DataType::Struct(fields) => fields.len(),
            _ => {
                return Err(DataFusionError::Plan(
                    "IcebergDeltaWrite _partition column must be a struct".into(),
                ))
            }
        };
        if layout.operation_ordinal != 0
            || layout.delete_operation != 1
            || layout.update_operation != 2
            || layout.insert_operation != 3
            || layout.reinsert_operation.is_some_and(|code| code != 4)
        {
            return Err(DataFusionError::Plan(format!(
                "Invalid Iceberg delta operation mapping (ordinal={}, DELETE={}, UPDATE={}, INSERT={}, REINSERT={:?})",
                layout.operation_ordinal,
                layout.delete_operation,
                layout.update_operation,
                layout.insert_operation,
                layout.reinsert_operation
            )));
        }

        let settings = proto.delete_parquet_settings.as_ref().ok_or_else(|| {
            DataFusionError::Plan("IcebergDeltaWrite missing delete_parquet_settings".into())
        })?;
        if proto.target_delete_file_size_bytes == 0 {
            return Err(DataFusionError::Plan(
                "IcebergDeltaWrite target delete file size must be positive".into(),
            ));
        }
        let delete_writer_properties = Arc::new(build_writer_properties(settings)?);
        let data_settings = common.parquet_settings.as_ref().ok_or_else(|| {
            DataFusionError::Plan("IcebergDeltaWrite data_common missing parquet_settings".into())
        })?;
        let data_writer_properties = Arc::new(build_writer_properties(data_settings)?);
        let data_writer_mode = ProtoIcebergWriterMode::try_from(common.writer_mode).map_err(|_| {
            DataFusionError::Plan(format!(
                "Unknown IcebergWriterMode proto value: {}",
                common.writer_mode
            ))
        })?;
        let iceberg_schema = parse_iceberg_schema(&common.iceberg_schema_json)?;
        let data_target_schema = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(&iceberg_schema).map_err(iceberg_err)?,
        );
        let delete_without_data_projection =
            command == IcebergDeltaCommand::IcebergDeltaCommandDelete
                && layout.data_ordinals.is_empty();
        if layout.data_ordinals.len() != data_target_schema.fields().len()
            && !delete_without_data_projection
        {
            return Err(DataFusionError::Plan(format!(
                "IcebergDeltaWrite data projection has {} columns but the write schema has {}",
                layout.data_ordinals.len(),
                data_target_schema.fields().len()
            )));
        }
        let data_columns = layout
            .data_ordinals
            .iter()
            .map(|ordinal| checked_ordinal(*ordinal, width, "data projection"))
            .collect::<DFResult<Vec<_>>>()?;
        let mut unique_data_columns = HashSet::with_capacity(data_columns.len());
        if data_columns.iter().any(|ordinal| !unique_data_columns.insert(*ordinal)) {
            return Err(DataFusionError::Plan(
                "IcebergDeltaWrite data projection contains a duplicate input ordinal".into(),
            ));
        }
        let data_input_schema = if delete_without_data_projection {
            data_target_schema
        } else {
            Arc::new(arrow::datatypes::Schema::new(
                data_columns
                    .iter()
                    .map(|ordinal| input_schema.field(*ordinal).clone())
                    .collect::<Vec<_>>(),
            ))
        };
        let current_spec = parse_partition_spec(&common.partition_spec_json)?;
        let mut historical_specs = HashMap::new();
        for proto_spec in &proto.historical_specs {
            let spec = parse_partition_spec(&proto_spec.partition_spec_json)?;
            if spec.spec_id() != proto_spec.spec_id {
                return Err(DataFusionError::Plan(format!(
                    "Historical partition descriptor id {} disagrees with parsed id {}",
                    proto_spec.spec_id,
                    spec.spec_id()
                )));
            }
            if historical_specs.contains_key(&proto_spec.spec_id) {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate historical partition spec {}",
                    proto_spec.spec_id
                )));
            }
            let partition_type = spec.partition_type(&iceberg_schema).map_err(iceberg_err)?;
            if partition_type.fields().len() != proto_spec.target_partition_field_ids.len()
                || proto_spec.partition_projection.len() != partition_type.fields().len()
            {
                return Err(DataFusionError::Plan(format!(
                    "Historical partition projection for spec {} has an invalid width",
                    proto_spec.spec_id
                )));
            }
            let projection = proto_spec
                .partition_projection
                .iter()
                .map(|ordinal| checked_ordinal(*ordinal, partition_width, "partition projection"))
                .collect::<DFResult<Vec<_>>>()?;
            for (field, id) in partition_type
                .fields()
                .iter()
                .zip(&proto_spec.target_partition_field_ids)
            {
                if field.id != *id {
                    return Err(DataFusionError::Plan(format!(
                        "Historical partition spec {} field id {} disagrees with descriptor {}",
                        proto_spec.spec_id, field.id, id
                    )));
                }
            }
            historical_specs.insert(
                proto_spec.spec_id,
                HistoricalSpec {
                    spec,
                    projection,
                    partition_type,
                },
            );
        }
        if historical_specs.is_empty() {
            return Err(DataFusionError::Plan(
                "IcebergDeltaWrite has no historical partition specs".into(),
            ));
        }
        let mut previous_deletes = HashMap::new();
        for group in previous_delete_message.groups {
            if group.data_file.is_empty() {
                return Err(DataFusionError::Plan(
                    "IcebergDeltaWrite previous delete group has an empty data file path".into(),
                ));
            }
            if !file_granularity {
                return Err(DataFusionError::Plan(
                    "IcebergDeltaWrite previous deletes require FILE granularity".into(),
                ));
            }
            let mut seen_locations = HashSet::with_capacity(group.delete_files.len());
            for delete_file in &group.delete_files {
                if delete_file.location.is_empty()
                    || delete_file.format.to_ascii_uppercase() != "PARQUET"
                    || delete_file.file_size_in_bytes <= 0
                    || delete_file.record_count < 0
                    || delete_file.key_metadata.is_some()
                    || delete_file.content_offset.is_some()
                    || delete_file.content_size_in_bytes.is_some()
                    || !historical_specs.contains_key(&delete_file.partition_spec_id)
                    || delete_file
                        .referenced_data_file
                        .as_deref()
                        .is_some_and(|path| path != group.data_file)
                    || !seen_locations.insert(delete_file.location.clone())
                {
                    return Err(DataFusionError::Plan(format!(
                        "Invalid previous position-delete descriptor for data file {}: {:?}",
                        group.data_file, delete_file.location
                    )));
                }
            }
            if previous_deletes
                .insert(group.data_file.clone(), group.delete_files)
                .is_some()
            {
                return Err(DataFusionError::Plan(format!(
                    "Duplicate previous delete group for data file {}",
                    group.data_file
                )));
            }
        }
        let output_schema = build_output_schema();
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(input.output_partitioning().partition_count()),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        let resolved_layout = ResolvedLayout {
            data_columns,
            file_path,
            row_position,
            spec_id,
            partition,
        };
        Ok(Self {
            input,
            proto: Arc::new(proto),
            common: Arc::new(common),
            iceberg_schema,
            current_spec,
            historical_specs,
            previous_deletes,
            file_granularity,
            resolved_layout,
            data_input_schema,
            delete_writer_properties,
            data_writer_properties,
            data_writer_mode,
            output_schema,
            plan_properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl ExecutionPlan for IcebergDeltaWriteExec {
    fn name(&self) -> &str {
        "IcebergDeltaWriteExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "IcebergDeltaWriteExec requires exactly one child".into(),
            ));
        }
        let mut proto = (*self.proto).clone();
        let common = self.common.as_ref().clone();
        proto.data_common = Some(common);
        let mut replacement = Self::try_new(children.remove(0), proto)?;
        replacement.metrics = self.metrics.clone();
        Ok(Arc::new(replacement))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let semantic_metrics = Arc::new(DeltaSemanticMetrics::new(&self.metrics, partition));
        let time = MetricBuilder::new(&self.metrics).subset_time("write_time", partition);
        let proto = Arc::clone(&self.proto);
        let common = Arc::clone(&self.common);
        let iceberg_schema = Arc::clone(&self.iceberg_schema);
        let current_spec = Arc::clone(&self.current_spec);
        let historical_specs = self.historical_specs.clone();
        let previous_deletes = self.previous_deletes.clone();
        let file_granularity = self.file_granularity;
        let layout = ResolvedLayout {
            data_columns: self.resolved_layout.data_columns.clone(),
            file_path: self.resolved_layout.file_path,
            row_position: self.resolved_layout.row_position,
            spec_id: self.resolved_layout.spec_id,
            partition: self.resolved_layout.partition,
        };
        let output_schema = Arc::clone(&self.output_schema);
        let data_input_schema = Arc::clone(&self.data_input_schema);
        let delete_writer_properties = Arc::clone(&self.delete_writer_properties);
        let data_writer_properties = Arc::clone(&self.data_writer_properties);
        let data_writer_mode = self.data_writer_mode;
        let task = async move {
            let partition_id = proto.partition_id.ok_or_else(|| {
                DataFusionError::Internal("IcebergDeltaWrite missing partition_id".into())
            })?;
            let task_attempt_id = proto.task_attempt_id.ok_or_else(|| {
                DataFusionError::Internal("IcebergDeltaWrite missing task_attempt_id".into())
            })?;
            let catalog_properties = common
                .catalog_properties
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            let file_io = load_file_io(
                &catalog_properties,
                &proto.delete_data_location,
                &common.catalog_name,
                AccessMode::Write,
            )?;
            let locations = Arc::new(Mutex::new(Vec::new()));
            let mut delete_guard =
                abort_guard_with_shared_locations(file_io.clone(), Arc::clone(&locations));
            let input_layout = proto.input_layout.as_ref().expect("validated input layout");
            let operation_ordinal = input_layout.operation_ordinal as usize;
            let router = DeleteRouter {
                ordered: proto.delete_input_ordered && !file_granularity,
                file_granularity,
                writers: HashMap::new(),
                active_key: None,
                closed_keys: HashSet::new(),
                last_position: HashMap::new(),
                referenced_data_files: HashSet::new(),
                previous_deletes,
                rewritten_delete_files: HashSet::new(),
                delete_file_loader: PositionDeleteIndexLoader::new(file_io.clone()),
                iceberg_schema: Arc::clone(&iceberg_schema),
                historical_specs: historical_specs.clone(),
                file_io: file_io.clone(),
                writer_properties: Arc::clone(&delete_writer_properties),
                target_size: proto.target_delete_file_size_bytes as usize,
                delete_data_location: proto.delete_data_location.clone(),
                operation_id: common.operation_id.clone(),
                partition_id,
                task_attempt_id,
                next_writer_id: 0,
                tracked_locations: Arc::clone(&locations),
            };
            let router = Arc::new(tokio::sync::Mutex::new(Some(router)));
            let router_for_stream = Arc::clone(&router);
            let layout_for_stream = ResolvedLayout {
                data_columns: layout.data_columns.clone(),
                file_path: layout.file_path,
                row_position: layout.row_position,
                spec_id: layout.spec_id,
                partition: layout.partition,
            };
            let delete_operation = input_layout.delete_operation;
            let update_operation = input_layout.update_operation;
            let insert_operation = input_layout.insert_operation;
            let reinsert_operation = input_layout.reinsert_operation;
            let command = IcebergDeltaCommand::try_from(proto.command).map_err(|_| {
                DataFusionError::Internal(format!("Unknown Iceberg delta command {}", proto.command))
            })?;
            let data_input_schema_for_stream = Arc::clone(&data_input_schema);
            let input_row_count = Arc::new(AtomicU64::new(0));
            let input_row_count_for_stream = Arc::clone(&input_row_count);
            let semantic_metrics_for_stream = Arc::clone(&semantic_metrics);
            let filtered = futures::stream::try_unfold(input, move |mut source| {
                let router = Arc::clone(&router_for_stream);
                let input_row_count = Arc::clone(&input_row_count_for_stream);
                let semantic_metrics = Arc::clone(&semantic_metrics_for_stream);
                let data_input_schema = Arc::clone(&data_input_schema_for_stream);
                let layout = ResolvedLayout {
                    data_columns: layout_for_stream.data_columns.clone(),
                    file_path: layout_for_stream.file_path,
                    row_position: layout_for_stream.row_position,
                    spec_id: layout_for_stream.spec_id,
                    partition: layout_for_stream.partition,
                };
                async move {
                    while let Some(batch) = source.try_next().await? {
                        input_row_count.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                        let operations = downcast::<Int32Array>(
                            batch.column(operation_ordinal),
                            "operation",
                        )?;
                        let mut delete_rows = Vec::new();
                        let mut data_rows = Vec::new();
                        let mut updated_rows = 0usize;
                        for row in 0..batch.num_rows() {
                            if operations.is_null(row) {
                                return Err(DataFusionError::Execution(format!(
                                    "Native Iceberg delta row {row} has a null operation code"
                                )));
                            }
                            match operations.value(row) {
                                code if code == delete_operation => {
                                    delete_rows.push(row);
                                }
                                code if code == update_operation => {
                                    return Err(DataFusionError::Execution(
                                        "Native Iceberg delta input contains direct UPDATE; Iceberg must lower UPDATE to DELETE plus INSERT/REINSERT".into(),
                                    ));
                                }
                                code if code == insert_operation => {
                                    if command == IcebergDeltaCommand::IcebergDeltaCommandDelete {
                                        return Err(DataFusionError::Execution(
                                            "Native Iceberg DELETE input contains an INSERT row".into(),
                                        ));
                                    }
                                    data_rows.push(row);
                                }
                                code if reinsert_operation == Some(code) => {
                                    if command == IcebergDeltaCommand::IcebergDeltaCommandDelete {
                                        return Err(DataFusionError::Execution(
                                            "Native Iceberg DELETE input contains a REINSERT row".into(),
                                        ));
                                    }
                                    updated_rows += 1;
                                    data_rows.push(row);
                                }
                                code => {
                                    return Err(DataFusionError::Execution(format!(
                                        "Native Iceberg delta input contains unknown operation code {code}"
                                    )));
                                }
                            }
                        }
                        semantic_metrics.deleted.add(delete_rows.len());
                        semantic_metrics.updated.add(updated_rows);
                        // The current Iceberg DeltaWriter path emits no COPY operation, but
                        // Spark's UPDATE/DELETE summary contract still includes this counter.
                        semantic_metrics.copied.add(0);
                        let data_batch = if data_rows.is_empty() {
                            None
                        } else {
                            Some(project_data_rows(
                                &batch,
                                &layout.data_columns,
                                &data_rows,
                                &data_input_schema,
                            )?)
                        };
                        if !delete_rows.is_empty() {
                            router
                                .lock()
                                .await
                                .as_mut()
                                .ok_or_else(|| {
                                    DataFusionError::Internal("delta router closed early".into())
                                })?
                                .route_batch(
                                    &batch,
                                    &layout,
                                    operation_ordinal,
                                    delete_operation,
                                    &delete_rows,
                                )
                                .await?;
                        }
                        if let Some(data_batch) = data_batch {
                            return Ok::<_, DataFusionError>(Some((data_batch, source)));
                        }
                    }
                    Ok::<Option<(RecordBatch, SendableRecordBatchStream)>, DataFusionError>(None)
                }
            });
            let data_stream = Box::pin(RecordBatchStreamAdapter::new(data_input_schema, filtered));
            let (data_files, mut data_guard) = match run_write_task(
                data_stream,
                Arc::clone(&common),
                Arc::clone(&iceberg_schema),
                Arc::clone(&current_spec),
                data_writer_mode,
                data_writer_properties.as_ref().clone(),
                Some(partition_id),
                Some(task_attempt_id),
                time,
            )
            .await
            {
                Ok(result) => result,
                Err(error) => {
                    delete_guard.abort().await;
                    return Err(error);
                }
            };
            let router = router.lock().await.take().ok_or_else(|| {
                DataFusionError::Internal("Iceberg delta router was already finalized".into())
            })?;
            let (delete_files, referenced_data_files, rewritten_delete_file_locations) =
                match router.close().await {
                Ok(result) => result,
                Err(error) => {
                    data_guard.abort().await;
                    delete_guard.abort().await;
                    return Err(error);
                }
            };
            let data_rows_written = data_files.iter().map(DataFile::record_count).sum();
            let data_bytes_written = data_files.iter().map(DataFile::file_size_in_bytes).sum();
            let delete_rows_written = delete_files
                .values()
                .flatten()
                .map(DataFile::record_count)
                .sum();
            let delete_bytes_written = delete_files
                .values()
                .flatten()
                .map(DataFile::file_size_in_bytes)
                .sum();
            let mut manifests = match encode_delete_manifests(
                delete_files,
                historical_specs,
                Arc::clone(&iceberg_schema),
                partition_id,
                task_attempt_id,
                &common.operation_id,
            )
            .await
            {
                Ok(manifests) => manifests,
                Err(error) => {
                    data_guard.abort().await;
                    delete_guard.abort().await;
                    return Err(error);
                }
            };
            if !data_files.is_empty() {
                match super::iceberg_write::encode_data_files_as_manifest(
                    data_files,
                    Arc::clone(&iceberg_schema),
                    Arc::clone(&current_spec),
                    Some(partition_id),
                    Some(task_attempt_id),
                    &common.operation_id,
                )
                .await
                {
                    Ok(bytes) => manifests.push(IcebergTaskManifest {
                        content: 0,
                        partition_spec_id: current_spec.spec_id(),
                        avro_manifest: bytes,
                    }),
                    Err(error) => {
                        data_guard.abort().await;
                        delete_guard.abort().await;
                        return Err(error);
                    }
                }
            }
            let payload = IcebergDeltaTaskPayload {
                schema_revision: 1,
                manifests,
                referenced_data_files,
                rewritten_delete_file_locations,
                input_rows: input_row_count.load(Ordering::Relaxed),
                data_rows_written,
                delete_rows_written,
                bytes_written: data_bytes_written + delete_bytes_written,
            }
            .encode_to_vec();
            let mut tracked = data_guard.locations();
            tracked.extend(delete_guard.locations());
            let output = build_delta_output_batch(payload, &tracked, &output_schema);
            match output {
                Ok(batch) => {
                    data_guard.disarm();
                    delete_guard.disarm();
                    Ok::<_, DataFusionError>(futures::stream::iter(vec![Ok(batch)]))
                }
                Err(error) => {
                    data_guard.abort().await;
                    delete_guard.abort().await;
                    Err(error)
                }
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.output_schema),
            futures::stream::once(task).try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl fmt::Debug for IcebergDeltaWriteExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IcebergDeltaWriteExec")
            .field("metadata_location", &self.common.metadata_location)
            .field("data_location", &self.common.data_location)
            .field("operation_id", &self.common.operation_id)
            .field("delete_granularity", &self.proto.delete_granularity)
            .finish()
    }
}

impl DisplayAs for IcebergDeltaWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "IcebergDeltaWriteExec: metadata_location={}, data_location={}, operation_id={}",
            self.common.metadata_location, self.common.data_location, self.common.operation_id
        )
    }
}

fn project_partition(
    union: &StructArray,
    row: usize,
    historical: &HistoricalSpec,
) -> DFResult<IcebergStruct> {
    if historical.spec.is_unpartitioned() {
        return Ok(IcebergStruct::empty());
    }
    let target = type_to_arrow_type(&Type::Struct(historical.partition_type.clone()))
        .map_err(iceberg_err)?;
    let DataType::Struct(target_fields) = target else {
        return Err(DataFusionError::Internal(
            "Iceberg partition type conversion did not produce a struct".into(),
        ));
    };
    let mut arrays = Vec::with_capacity(historical.projection.len());
    for (target_field, source_ordinal) in target_fields.iter().zip(&historical.projection) {
        let source = union.column(*source_ordinal).slice(row, 1);
        let cast = arrow::compute::cast(&source, target_field.data_type())?;
        arrays.push(cast);
    }
    let projected = Arc::new(StructArray::try_new(target_fields, arrays, None)?) as ArrayRef;
    let values = arrow_struct_to_literal(&projected, &historical.partition_type)
        .map_err(iceberg_err)?;
    match values.into_iter().next().flatten() {
        Some(Literal::Struct(value)) => Ok(value),
        _ => Err(DataFusionError::Execution(
            "Native Iceberg DELETE partition projection produced a null value".into(),
        )),
    }
}

async fn encode_delete_manifests(
    files_by_spec: HashMap<i32, Vec<DataFile>>,
    specs: HashMap<i32, HistoricalSpec>,
    schema: IcebergSchemaRef,
    partition_id: i32,
    task_attempt_id: i64,
    operation_id: &str,
) -> DFResult<Vec<IcebergTaskManifest>> {
    let memory_io = load_file_io(&HashMap::new(), "memory:///", "", AccessMode::Write)?;
    let mut spec_ids = files_by_spec.keys().copied().collect::<Vec<_>>();
    spec_ids.sort_unstable();
    let mut manifests = Vec::with_capacity(spec_ids.len());
    for spec_id in spec_ids {
        let historical = specs.get(&spec_id).ok_or_else(|| {
            DataFusionError::Internal(format!("Missing spec {spec_id} while encoding deletes"))
        })?;
        let path = format!(
            "memory:///comet-delta-manifest-{partition_id:05}-{task_attempt_id:05}-{operation_id}-{spec_id}.avro"
        );
        let output = memory_io.new_output(&path).map_err(iceberg_err)?;
        let mut writer = ManifestWriterBuilder::new(
            output,
            None,
            Arc::clone(&schema),
            manifest_partition_spec(&historical.spec),
        )
        .build_v2_deletes();
        for file in files_by_spec.remove(&spec_id).unwrap_or_default() {
            writer.add_file(file, 0).map_err(iceberg_err)?;
        }
        writer.write_manifest_file().await.map_err(iceberg_err)?;
        let bytes = memory_io
            .new_input(&path)
            .map_err(iceberg_err)?
            .read()
            .await
            .map_err(iceberg_err)?;
        manifests.push(IcebergTaskManifest {
            content: 1,
            partition_spec_id: spec_id,
            avro_manifest: bytes.to_vec(),
        });
    }
    Ok(manifests)
}

fn build_delta_output_batch(
    payload: Vec<u8>,
    locations: &[String],
    schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    let encoded_locations = super::iceberg_write::encode_locations(locations);
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(BinaryArray::from(vec![payload.as_slice()])) as ArrayRef,
            Arc::new(BinaryArray::from(vec![encoded_locations.as_slice()])) as ArrayRef,
        ],
    )
    .map_err(DataFusionError::from)
}

fn previous_delete_scan_task(
    descriptor: &IcebergPreviousDeleteFileDescriptor,
) -> DFResult<FileScanTaskDeleteFile> {
    let file_size = u64::try_from(descriptor.file_size_in_bytes).map_err(|_| {
        DataFusionError::Plan(format!(
            "Previous delete file {} has an invalid file size {}",
            descriptor.location, descriptor.file_size_in_bytes
        ))
    })?;
    let record_count = u64::try_from(descriptor.record_count).map_err(|_| {
        DataFusionError::Plan(format!(
            "Previous delete file {} has an invalid record count {}",
            descriptor.location, descriptor.record_count
        ))
    })?;
    Ok(FileScanTaskDeleteFile::builder()
        .with_file_path(descriptor.location.clone())
        .with_file_size_in_bytes(file_size)
        .with_file_type(DataContentType::PositionDeletes)
        .with_file_format(DataFileFormat::Parquet)
        .with_partition_spec_id(descriptor.partition_spec_id)
        .with_equality_ids(None)
        .with_referenced_data_file(descriptor.referenced_data_file.clone())
        .with_content_offset(descriptor.content_offset)
        .with_content_size_in_bytes(descriptor.content_size_in_bytes)
        .with_record_count(Some(record_count))
        .with_key_metadata(
            descriptor
                .key_metadata
                .as_ref()
                .map(|metadata| metadata.clone().into_boxed_slice()),
        )
        .build())
}

fn project_data_rows(
    batch: &RecordBatch,
    data_columns: &[usize],
    row_indices: &[usize],
    schema: &SchemaRef,
) -> DFResult<RecordBatch> {
    if data_columns.len() != schema.fields().len() {
        return Err(DataFusionError::Internal(format!(
            "Iceberg delta data projection has {} ordinals for a {}-field input schema",
            data_columns.len(),
            schema.fields().len()
        )));
    }
    let indices = UInt64Array::from(
        row_indices
            .iter()
            .map(|index| *index as u64)
            .collect::<Vec<_>>(),
    );
    let columns = data_columns
        .iter()
        .map(|ordinal| take(batch.column(*ordinal).as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(DataFusionError::from)
}

fn checked_ordinal(value: i32, width: usize, label: &str) -> DFResult<usize> {
    let ordinal = usize::try_from(value).map_err(|_| {
        DataFusionError::Plan(format!("IcebergDeltaWrite {label} ordinal {value} is negative"))
    })?;
    if ordinal >= width {
        return Err(DataFusionError::Plan(format!(
            "IcebergDeltaWrite {label} ordinal {ordinal} exceeds input width {width}"
        )));
    }
    Ok(ordinal)
}

fn projected_ordinal(
    projection: &[i32],
    index: i32,
    width: usize,
    label: &str,
) -> DFResult<usize> {
    let index = usize::try_from(index).map_err(|_| {
        DataFusionError::Plan(format!("IcebergDeltaWrite {label} projection index is negative"))
    })?;
    let value = *projection.get(index).ok_or_else(|| {
        DataFusionError::Plan(format!("IcebergDeltaWrite {label} projection is missing index {index}"))
    })?;
    checked_ordinal(value, width, label)
}

fn expect_type(schema: &SchemaRef, ordinal: usize, expected: &DataType, label: &str) -> DFResult<()> {
    if schema.field(ordinal).data_type() != expected {
        return Err(DataFusionError::Plan(format!(
            "IcebergDeltaWrite {label} column must be {expected:?}, got {:?}",
            schema.field(ordinal).data_type()
        )));
    }
    Ok(())
}

fn downcast<'a, T: 'static>(array: &'a ArrayRef, label: &str) -> DFResult<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        DataFusionError::Execution(format!("IcebergDeltaWrite {label} column has an invalid Arrow array"))
    })
}

fn iceberg_err(error: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
