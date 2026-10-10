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

import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types._

import org.apache.comet.shims.CometTypeShim

/**
 * Single source of truth for eligible runtime-pruning key types on the JVM/Spark side.
 *
 * Today, the JVM side gates two sets of types:
 *   1. Driver-side file statistics key types ([[isFileStatsKey]]): IntegerType, LongType,
 *      DateType, TimestampType, TimestampNTZType, exact DecimalType, and uncollated StringType
 *      (excluding fixed CHAR), and corresponding Iceberg schema types
 *      ([[isFileStatsIcebergType]]). Used by CometScanRule (join/TopK/MinMax file pruning),
 *      CometIcebergNativeScan (file statistics collection), and CometLocalTopKExec (Iceberg
 *      TopK). 2. Native Parquet reader filter keys ([[isParquetReaderKey]]): ByteType, ShortType,
 *      IntegerType, LongType Used by CometLocalTopKExec (Parquet TopK pushdown).
 *
 * Native join eligibility (including Decimal128 and string variants) and native TopK pushdown
 * eligibility are decided in native Rust code, not duplicated on the driver.
 */
object RuntimePruningKeyTypes extends CometTypeShim {

  /**
   * Supported Spark data types for driver-side file statistics / runtime pruning keys. Gated for
   * CometScanRule (join) and CometIcebergNativeScan.
   */
  val SUPPORTED_FILE_STATS_TYPES: Seq[DataType] =
    Seq(
      IntegerType,
      LongType,
      DateType,
      TimestampType,
      TimestampNTZType,
      DecimalType.SYSTEM_DEFAULT,
      StringType)

  /** Supported Spark data types for native Parquet reader pushdown keys. */
  val SUPPORTED_PARQUET_READER_TYPES: Seq[DataType] =
    Seq(ByteType, ShortType, IntegerType, LongType)

  /** Supported Iceberg primitive schema type strings for driver-side file statistics. */
  val SUPPORTED_ICEBERG_FILE_STATS_TYPES: Set[String] =
    Set("int", "long", "date", "timestamp", "timestamptz", "string")

  /** Aliases for driver file stats types. */
  val SUPPORTED_JOIN_TYPES: Seq[DataType] = SUPPORTED_FILE_STATS_TYPES
  val SUPPORTED_TOPK_TYPES: Seq[DataType] =
    Seq(IntegerType, LongType, DateType, TimestampType, TimestampNTZType, StringType)
  val SUPPORTED_MINMAX_TYPES: Seq[DataType] =
    Seq(IntegerType, LongType, DateType, TimestampType, TimestampNTZType, StringType)
  val SUPPORTED_COLUMN_STATS_TYPES: Seq[DataType] = SUPPORTED_FILE_STATS_TYPES
  val SUPPORTED_ICEBERG_TYPES: Set[String] = SUPPORTED_ICEBERG_FILE_STATS_TYPES
  val SUPPORTED_TYPES: Seq[DataType] = SUPPORTED_FILE_STATS_TYPES

  /**
   * Whether Catalyst metadata marks an attribute as a fixed-length CHAR. Stored Iceberg strings
   * do not promise CHAR padding, so reader pruning must not compare them to padded join literals.
   */
  def isCharType(metadata: Metadata): Boolean =
    metadata.contains("__CHAR_VARCHAR_TYPE_STRING") &&
      metadata
        .getString("__CHAR_VARCHAR_TYPE_STRING")
        .toLowerCase(Locale.ROOT)
        .startsWith("char(")

  /**
   * Whether the given Spark `dataType` is an eligible driver-side file statistics key type (Int,
   * Long, Date, Timestamp, TimestampNTZ, Decimal, String). Collated strings are excluded because
   * Iceberg string bounds are binary/UTF-8 ordered.
   */
  def isFileStatsKey(dataType: DataType): Boolean = dataType match {
    case IntegerType | LongType | DateType | TimestampType | TimestampNTZType | _: DecimalType =>
      true
    case st: StringType if !isStringCollationType(st) => true
    case _ => false
  }

  /**
   * Whether the given Spark `attr` is an eligible driver-side file statistics attribute. In
   * addition to checking [[isFileStatsKey]], excludes fixed-length CHAR attributes.
   */
  def isFileStatsAttribute(attr: Attribute): Boolean =
    isFileStatsKey(attr.dataType) && !isCharType(attr.metadata)

  /** Whether the given Spark `dataType` is supported by native Parquet reader pushdown. */
  def isParquetReaderKey(dataType: DataType): Boolean = dataType match {
    case ByteType | ShortType | IntegerType | LongType => true
    case _ => false
  }

  /** Alias for [[isParquetReaderKey]]. */
  def isSupportedParquetReaderKey(dataType: DataType): Boolean = isParquetReaderKey(dataType)

  private val icebergDecimal = "decimal\\(([0-9]{1,2}),\\s*([0-9]{1,2})\\)".r

  /** Whether the given Iceberg primitive type string is eligible for runtime file statistics. */
  def isFileStatsIcebergType(typeStr: String): Boolean = typeStr match {
    case icebergDecimal(precision, scale) =>
      val p = precision.toInt
      val s = scale.toInt
      p >= 1 && p <= 38 && s <= p
    case _ => SUPPORTED_ICEBERG_FILE_STATS_TYPES.contains(typeStr)
  }

  /** Alias for [[isFileStatsIcebergType]]. */
  def isSupportedIcebergType(typeStr: String): Boolean = isFileStatsIcebergType(typeStr)

  /** Consumer aliases. */
  def isSupportedJoinKey(dataType: DataType): Boolean = isFileStatsKey(dataType)
  def isSupportedTopKKey(dataType: DataType): Boolean = dataType match {
    case IntegerType | LongType | DateType | TimestampType | TimestampNTZType => true
    case st: StringType if !isStringCollationType(st) => true
    case _ => false
  }
  def isSupportedMinMaxKey(dataType: DataType): Boolean = dataType match {
    case IntegerType | LongType | DateType | TimestampType | TimestampNTZType => true
    case st: StringType if !isStringCollationType(st) => true
    case _ => false
  }
  def isSupportedColumnStatsKey(dataType: DataType): Boolean = isFileStatsKey(dataType)
  def isSupported(dataType: DataType): Boolean = isFileStatsKey(dataType)
}
