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
 * JVM key-type policy for runtime pruning.
 *
 * Driver file statistics admit INT, BIGINT, DATE, microsecond timestamps, DECIMAL and UTF8_BINARY
 * strings. Native Parquet reader pushdown admits signed integer keys, including BYTE and SHORT,
 * which Iceberg schemas cannot produce. TopK and MIN/MAX exclude DECIMAL. Native code makes the
 * final reader-attachment decision.
 *
 * This is not a single shared definition: Scala and Rust each define key eligibility, linked by a
 * cross-language eligibility contract with agreement tests (`RuntimePruningKeyTypesSuite` and
 * `native/core/src/execution/operators/dynamic_filter/tests.rs`). A change to either side must
 * update both and their agreement tests.
 */
object RuntimePruningKeyTypes extends CometTypeShim {

  /** Binary string comparison support, independent of any pruning consumer's eligibility. */
  def isBinaryStringType(dataType: DataType): Boolean =
    dataType.isInstanceOf[StringType] && !dataType.isInstanceOf[CharType] &&
      !isStringCollationType(dataType)

  /** Whether Catalyst metadata marks an attribute as a fixed-length CHAR. */
  def isCharType(metadata: Metadata): Boolean =
    metadata.contains("__CHAR_VARCHAR_TYPE_STRING") &&
      metadata
        .getString("__CHAR_VARCHAR_TYPE_STRING")
        .toLowerCase(Locale.ROOT)
        .startsWith("char(")

  /** CHAR may be preserved as a data type or represented by StringType plus metadata. */
  def isCharType(attribute: Attribute): Boolean =
    attribute.dataType.isInstanceOf[CharType] || isCharType(attribute.metadata)

  /** Eligible driver file-statistics types; Iceberg string bounds use binary UTF-8 ordering. */
  def isFileStatsKey(dataType: DataType): Boolean = dataType match {
    case IntegerType | LongType | DateType | TimestampType | TimestampNTZType | _: DecimalType =>
      true
    case _ => isBinaryStringType(dataType)
  }

  /** Stored Iceberg strings do not promise the padding required by CHAR comparisons. */
  def isFileStatsAttribute(attr: Attribute): Boolean =
    isFileStatsKey(attr.dataType) && !isCharType(attr)

  /** Whether the given Spark data type is supported by native Parquet reader pushdown. */
  def isParquetReaderKey(dataType: DataType): Boolean = dataType match {
    case ByteType | ShortType | IntegerType | LongType => true
    case _ => false
  }

  private val icebergDecimal = "decimal\\(([0-9]{1,2}),\\s*([0-9]{1,2})\\)".r

  /** Whether the given Iceberg primitive type string is eligible for runtime file statistics. */
  def isFileStatsIcebergType(typeStr: String): Boolean = typeStr match {
    case icebergDecimal(precision, scale) =>
      val p = precision.toInt
      val s = scale.toInt
      p >= 1 && p <= 38 && s <= p
    case "int" | "long" | "date" | "timestamp" | "timestamptz" | "string" => true
    case _ => false
  }

  def isSupportedTopKKey(dataType: DataType): Boolean = dataType match {
    case IntegerType | LongType | DateType | TimestampType | TimestampNTZType => true
    case _ => isBinaryStringType(dataType)
  }

  def isSupportedMinMaxKey(dataType: DataType): Boolean = dataType match {
    case IntegerType | LongType | DateType | TimestampType | TimestampNTZType => true
    case _ => isBinaryStringType(dataType)
  }
}
