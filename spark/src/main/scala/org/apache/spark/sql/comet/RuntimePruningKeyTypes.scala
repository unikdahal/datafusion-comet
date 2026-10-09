/*
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

package org.apache.spark.sql.comet

import org.apache.spark.sql.types._

/**
 * Single source of truth for eligible runtime-pruning key types on the JVM/Spark side.
 *
 * Today, the JVM side gates two sets of types:
 *   1. Driver-side file statistics key types ([[isFileStatsKey]]): IntegerType, LongType,
 *      DateType, TimestampType, TimestampNTZType and corresponding Iceberg schema types
 *      ([[isFileStatsIcebergType]]). Used by CometScanRule (join/TopK/MinMax file pruning),
 *      CometIcebergNativeScan (file statistics collection), and CometLocalTopKExec (Iceberg
 *      TopK). 2. Native Parquet reader filter keys ([[isParquetReaderKey]]): ByteType, ShortType,
 *      IntegerType, LongType Used by CometLocalTopKExec (Parquet TopK pushdown).
 *
 * Native join eligibility (including Decimal128 and string variants) and native TopK pushdown
 * eligibility are decided in native Rust code, not duplicated on the driver.
 */
object RuntimePruningKeyTypes {

  /**
   * Supported Spark data types for driver-side file statistics / runtime pruning keys. Gated
   * today for CometScanRule (join, TopK, MinMax), CometIcebergNativeScan, and CometLocalTopKExec
   * (Iceberg TopK).
   */
  val SUPPORTED_FILE_STATS_TYPES: Seq[DataType] =
    Seq(IntegerType, LongType, DateType, TimestampType, TimestampNTZType)

  /** Supported Spark data types for native Parquet reader pushdown keys. */
  val SUPPORTED_PARQUET_READER_TYPES: Seq[DataType] =
    Seq(ByteType, ShortType, IntegerType, LongType)

  /** Supported Iceberg primitive schema type strings for driver-side file statistics. */
  val SUPPORTED_ICEBERG_FILE_STATS_TYPES: Set[String] =
    Set("int", "long", "date", "timestamp", "timestamptz")

  /** Aliases for driver file stats types. */
  val SUPPORTED_JOIN_TYPES: Seq[DataType] = SUPPORTED_FILE_STATS_TYPES
  val SUPPORTED_TOPK_TYPES: Seq[DataType] = SUPPORTED_FILE_STATS_TYPES
  val SUPPORTED_MINMAX_TYPES: Seq[DataType] = SUPPORTED_FILE_STATS_TYPES
  val SUPPORTED_COLUMN_STATS_TYPES: Seq[DataType] = SUPPORTED_FILE_STATS_TYPES
  val SUPPORTED_ICEBERG_TYPES: Set[String] = SUPPORTED_ICEBERG_FILE_STATS_TYPES
  val SUPPORTED_TYPES: Seq[DataType] = SUPPORTED_FILE_STATS_TYPES

  /**
   * Whether the given Spark `dataType` is an eligible driver-side file statistics key type (Int,
   * Long, Date, Timestamp, TimestampNTZ).
   */
  def isFileStatsKey(dataType: DataType): Boolean = dataType match {
    case IntegerType | LongType | DateType | TimestampType | TimestampNTZType => true
    case _ => false
  }

  /** Whether the given Spark `dataType` is supported by native Parquet reader pushdown. */
  def isParquetReaderKey(dataType: DataType): Boolean = dataType match {
    case ByteType | ShortType | IntegerType | LongType => true
    case _ => false
  }

  /** Whether the given Iceberg primitive type string is eligible for runtime file statistics. */
  def isFileStatsIcebergType(typeStr: String): Boolean =
    SUPPORTED_ICEBERG_FILE_STATS_TYPES.contains(typeStr)

  /** Alias for [[isFileStatsIcebergType]]. */
  def isSupportedIcebergType(typeStr: String): Boolean = isFileStatsIcebergType(typeStr)

  /** Consumer aliases delegating to [[isFileStatsKey]]. */
  def isSupportedJoinKey(dataType: DataType): Boolean = isFileStatsKey(dataType)
  def isSupportedTopKKey(dataType: DataType): Boolean = isFileStatsKey(dataType)
  def isSupportedMinMaxKey(dataType: DataType): Boolean = isFileStatsKey(dataType)
  def isSupportedColumnStatsKey(dataType: DataType): Boolean = isFileStatsKey(dataType)
  def isSupported(dataType: DataType): Boolean = isFileStatsKey(dataType)
}
