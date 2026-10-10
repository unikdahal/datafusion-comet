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

// The clippy throws an error if the reference clone not wrapped into `Arc::clone`
// The lint makes easier for code reader/reviewer separate references clones from more heavyweight ones
#![deny(clippy::clone_on_ref_ptr)]

// Include generated modules from .proto files.
#[allow(missing_docs)]
#[allow(clippy::large_enum_variant)]
pub mod spark_expression {
    include!(concat!("generated", "/spark.spark_expression.rs"));
}

// Include generated modules from .proto files.
#[allow(missing_docs)]
pub mod spark_partitioning {
    include!(concat!("generated", "/spark.spark_partitioning.rs"));
}

// Include generated modules from .proto files.
#[allow(missing_docs)]
#[allow(clippy::large_enum_variant)]
pub mod spark_operator {
    include!(concat!("generated", "/spark.spark_operator.rs"));
}

// Include generated modules from .proto files.
#[allow(missing_docs)]
pub mod spark_metric {
    include!(concat!("generated", "/spark.spark_metric.rs"));
}

// Include generated modules from .proto files.
#[allow(missing_docs)]
pub mod spark_config {
    include!(concat!("generated", "/spark.spark_config.rs"));
}

#[cfg(test)]
mod tests {
    use super::spark_operator::{
        partition_writer, HashAggregate, LocalPartitionWriter, PartitionWriter, RssPartitionWriter,
        ShuffleWriter,
    };
    use prost::Message;

    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacyIcebergCommon {
        #[prost(string, tag = "2")]
        metadata_location: String,
        #[prost(string, repeated, tag = "4")]
        schema_pool: Vec<String>,
        #[prost(message, repeated, tag = "8")]
        project_field_ids_pool: Vec<super::spark_operator::ProjectFieldIdList>,
        #[prost(message, repeated, tag = "10")]
        delete_files_pool: Vec<super::spark_operator::DeleteFileList>,
        #[prost(message, repeated, tag = "14")]
        delete_file_pool: Vec<super::spark_operator::IcebergDeleteFile>,
        #[prost(string, repeated, tag = "16")]
        delete_file_path_pool: Vec<String>,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacyIcebergTask {
        #[prost(string, tag = "1")]
        data_file_path: String,
        #[prost(uint64, tag = "2")]
        start: u64,
        #[prost(uint64, tag = "3")]
        length: u64,
        #[prost(uint64, optional, tag = "4")]
        record_count: Option<u64>,
        #[prost(uint64, tag = "5")]
        file_size_in_bytes: u64,
        #[prost(uint32, tag = "15")]
        schema_idx: u32,
        #[prost(uint32, tag = "19")]
        project_field_ids_idx: u32,
        #[prost(uint32, optional, tag = "21")]
        delete_files_idx: Option<u32>,
    }

    #[test]
    fn iceberg_pool_legacy_wire_roundtrip() {
        use super::spark_operator::{
            DeleteFileList, IcebergDeleteFile, IcebergFileMetrics, IcebergFileScanTask,
            IcebergScanCommon, ProjectFieldIdList,
        };

        let legacy_common = LegacyIcebergCommon {
            metadata_location: "file:///table/metadata.json".into(),
            schema_pool: vec!["schema-0".into(), "schema-1".into()],
            project_field_ids_pool: vec![ProjectFieldIdList {
                field_ids: vec![1, 3],
            }],
            delete_files_pool: vec![DeleteFileList {
                delete_file_indices: vec![0],
            }],
            delete_file_pool: vec![IcebergDeleteFile {
                file_path_idx: 0,
                content_type: "EQUALITY_DELETES".into(),
                equality_ids: vec![3],
                ..Default::default()
            }],
            delete_file_path_pool: vec!["file:///table/deletes.parquet".into()],
        };
        let legacy_task = LegacyIcebergTask {
            data_file_path: "file:///table/data.parquet".into(),
            start: 128,
            length: 512,
            record_count: Some(27),
            file_size_in_bytes: 2048,
            schema_idx: 1,
            project_field_ids_idx: 0,
            delete_files_idx: Some(0),
        };
        let mut common =
            IcebergScanCommon::decode(legacy_common.encode_to_vec().as_slice()).unwrap();
        let mut task =
            IcebergFileScanTask::decode(legacy_task.encode_to_vec().as_slice()).unwrap();
        assert!(common.file_metrics_pool.is_empty());
        assert_eq!(task.file_metrics_idx, None);
        common.file_metrics_pool.push(IcebergFileMetrics {
            record_count: Some(27),
            lower_bounds: [(1, 10_i32.to_le_bytes().to_vec())].into(),
            upper_bounds: [(1, 20_i32.to_le_bytes().to_vec())].into(),
            ..Default::default()
        });
        // Some(0) must remain distinguishable from an absent pool reference.
        task.file_metrics_idx = Some(0);
        let common_bytes = common.encode_to_vec();
        let task_bytes = task.encode_to_vec();
        assert_eq!(
            LegacyIcebergCommon::decode(common_bytes.as_slice()).unwrap(),
            legacy_common,
        );
        assert_eq!(
            LegacyIcebergTask::decode(task_bytes.as_slice()).unwrap(),
            legacy_task,
        );
        assert_eq!(
            IcebergScanCommon::decode(common_bytes.as_slice()).unwrap(),
            common,
        );
        assert_eq!(IcebergFileScanTask::decode(task_bytes.as_slice()).unwrap(), task);
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacyShuffleWriter {
        #[prost(string, tag = "3")]
        output_data_file: String,
        #[prost(string, tag = "4")]
        output_index_file: String,
    }

    // Main already assigns aggregate output ordering to tag 9. Runtime filtering must not
    // reinterpret an existing ordered aggregate plan, or cause an older reader to invent ordering.
    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacyAggregateOrdering {
        #[prost(bool, tag = "9")]
        ordered_by_grouping_keys: bool,
    }

    #[test]
    fn runtime_filter_preserves_existing_aggregate_ordering_wire_field() {
        let legacy = LegacyAggregateOrdering {
            ordered_by_grouping_keys: true,
        };
        let decoded = HashAggregate::decode(legacy.encode_to_vec().as_slice()).unwrap();
        assert!(decoded.ordered_by_grouping_keys);
        assert!(!decoded.dynamic_filter_enabled);
    }

    #[test]
    fn runtime_filter_does_not_advertise_aggregate_ordering_to_legacy_reader() {
        let filtered = HashAggregate {
            dynamic_filter_enabled: true,
            ..Default::default()
        };
        let bytes = filtered.encode_to_vec();
        let legacy = LegacyAggregateOrdering::decode(bytes.as_slice()).unwrap();
        assert!(!legacy.ordered_by_grouping_keys);
        assert!(
            HashAggregate::decode(bytes.as_slice())
                .unwrap()
                .dynamic_filter_enabled
        );
    }

    fn local_shuffle_writer() -> ShuffleWriter {
        let output_data_file = "/tmp/shuffle.data".to_string();

        ShuffleWriter {
            output_data_file: output_data_file.clone(),
            partition_writer: Some(PartitionWriter {
                writer: Some(partition_writer::Writer::Local(LocalPartitionWriter {
                    output_data_file,
                })),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn shuffle_partition_writer_round_trips_local_destination() {
        let encoded = local_shuffle_writer().encode_to_vec();
        let decoded = ShuffleWriter::decode(encoded.as_slice()).unwrap();

        assert_eq!(decoded.output_data_file, "/tmp/shuffle.data");
        let Some(partition_writer::Writer::Local(local)) =
            decoded.partition_writer.and_then(|writer| writer.writer)
        else {
            panic!("expected a local shuffle partition writer");
        };
        assert_eq!(local.output_data_file, "/tmp/shuffle.data");
    }

    #[test]
    fn shuffle_partition_writer_round_trips_rss_destination() {
        let writer = ShuffleWriter {
            partition_writer: Some(PartitionWriter {
                writer: Some(partition_writer::Writer::Rss(RssPartitionWriter {})),
            }),
            ..Default::default()
        };
        let decoded = ShuffleWriter::decode(writer.encode_to_vec().as_slice()).unwrap();

        assert!(matches!(
            decoded.partition_writer.and_then(|writer| writer.writer),
            Some(partition_writer::Writer::Rss(_))
        ));
    }

    #[test]
    fn legacy_shuffle_writer_decodes_new_plan_using_compatibility_paths() {
        let encoded = local_shuffle_writer().encode_to_vec();
        let decoded = LegacyShuffleWriter::decode(encoded.as_slice()).unwrap();

        assert_eq!(decoded.output_data_file, "/tmp/shuffle.data");
        // a new plan carries no index path, so a reader expecting tag 4 sees it unset
        assert!(decoded.output_index_file.is_empty());
    }

    /// A plan still carrying the retired index path decodes cleanly: tag 4 is reserved, so it is
    /// skipped as an unknown field.
    #[test]
    fn new_shuffle_writer_decodes_legacy_plan_without_destination() {
        let legacy = LegacyShuffleWriter {
            output_data_file: "/tmp/legacy.data".to_string(),
            output_index_file: "/tmp/legacy.index".to_string(),
        };
        let decoded = ShuffleWriter::decode(legacy.encode_to_vec().as_slice()).unwrap();

        assert_eq!(decoded.output_data_file, "/tmp/legacy.data");
        assert!(decoded.partition_writer.is_none());
    }
}
