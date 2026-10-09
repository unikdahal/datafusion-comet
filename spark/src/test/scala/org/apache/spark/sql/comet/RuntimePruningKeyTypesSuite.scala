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

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.types._

/**
 * Cross-language agreement tests verifying per-consumer runtime pruning key types in Scala
 * agree with native Arrow types.
 *
 * Hard-coded cross-reference with native Rust tests:
 * `native/core/src/execution/operators/dynamic_filter/tests.rs`.
 */
class RuntimePruningKeyTypesSuite extends AnyFunSuite {

  test("HashJoin consumer key types agreement with Arrow") {
    val supportedJoinSparkToArrow: Seq[(DataType, String)] = Seq(
      (IntegerType, "Int32"),
      (LongType, "Int64"),
      (DateType, "Date32"),
      (TimestampType, "Timestamp(Microsecond, Some(...))"),
      (TimestampNTZType, "Timestamp(Microsecond, None)"),
      (StringType, "Utf8"))

    for ((sparkType, _) <- supportedJoinSparkToArrow) {
      assert(
        RuntimePruningKeyTypes.isSupportedJoinKey(sparkType),
        s"Expected $sparkType to be supported as Join key")
      assert(
        RuntimePruningKeyTypes.isFileStatsKey(sparkType),
        s"Expected $sparkType to be supported as file stats key")
    }

    assert(
      RuntimePruningKeyTypes.SUPPORTED_JOIN_TYPES == supportedJoinSparkToArrow.map(_._1),
      "SUPPORTED_JOIN_TYPES must match expected sequence")

    val unsupportedJoinTypes: Seq[DataType] = Seq(
      ByteType,
      ShortType,
      DecimalType(10, 2),
      DecimalType(38, 18),
      FloatType,
      DoubleType,
      BooleanType,
      BinaryType,
      ArrayType(IntegerType),
      MapType(StringType, StringType),
      StructType(Seq(StructField("a", IntegerType))))

    for (unsupported <- unsupportedJoinTypes) {
      assert(
        !RuntimePruningKeyTypes.isSupportedJoinKey(unsupported),
        s"Expected $unsupported to be rejected as Join key")
      assert(
        !RuntimePruningKeyTypes.isFileStatsKey(unsupported),
        s"Expected $unsupported to be rejected as file stats key")
    }
  }

  test("TopK consumer key types agreement with Arrow") {
    val supportedTopKSparkToArrow: Seq[(DataType, String)] = Seq(
      (IntegerType, "Int32"),
      (LongType, "Int64"),
      (DateType, "Date32"),
      (TimestampType, "Timestamp(Microsecond, Some(...))"),
      (TimestampNTZType, "Timestamp(Microsecond, None)"),
      (StringType, "Utf8"))

    for ((sparkType, _) <- supportedTopKSparkToArrow) {
      assert(
        RuntimePruningKeyTypes.isSupportedTopKKey(sparkType),
        s"Expected $sparkType to be supported as TopK key")
      assert(
        RuntimePruningKeyTypes.isFileStatsKey(sparkType),
        s"Expected $sparkType to be supported as file stats key")
    }

    assert(
      RuntimePruningKeyTypes.SUPPORTED_TOPK_TYPES == supportedTopKSparkToArrow.map(_._1),
      "SUPPORTED_TOPK_TYPES must match expected sequence")

    val unsupportedTopKTypes: Seq[DataType] = Seq(
      ByteType,
      ShortType,
      DecimalType(10, 2),
      DecimalType(38, 18),
      FloatType,
      DoubleType,
      BooleanType,
      BinaryType)

    for (unsupported <- unsupportedTopKTypes) {
      assert(
        !RuntimePruningKeyTypes.isSupportedTopKKey(unsupported),
        s"Expected $unsupported to be rejected as TopK key")
      assert(
        !RuntimePruningKeyTypes.isFileStatsKey(unsupported),
        s"Expected $unsupported to be rejected as file stats key")
    }
  }

  test("MinMax consumer key types agreement with Arrow") {
    val supportedMinMaxSparkToArrow: Seq[(DataType, String)] = Seq(
      (IntegerType, "Int32"),
      (LongType, "Int64"),
      (DateType, "Date32"),
      (TimestampType, "Timestamp(Microsecond, Some(...))"),
      (TimestampNTZType, "Timestamp(Microsecond, None)"),
      (StringType, "Utf8"))

    for ((sparkType, _) <- supportedMinMaxSparkToArrow) {
      assert(
        RuntimePruningKeyTypes.isSupportedMinMaxKey(sparkType),
        s"Expected $sparkType to be supported as MinMax key")
      assert(
        RuntimePruningKeyTypes.isFileStatsKey(sparkType),
        s"Expected $sparkType to be supported as file stats key")
    }

    assert(
      RuntimePruningKeyTypes.SUPPORTED_MINMAX_TYPES == supportedMinMaxSparkToArrow.map(_._1),
      "SUPPORTED_MINMAX_TYPES must match expected sequence")

    val unsupportedMinMaxTypes: Seq[DataType] = Seq(
      ByteType,
      ShortType,
      DecimalType(10, 2),
      FloatType,
      DoubleType,
      BooleanType,
      BinaryType)

    for (unsupported <- unsupportedMinMaxTypes) {
      assert(
        !RuntimePruningKeyTypes.isSupportedMinMaxKey(unsupported),
        s"Expected $unsupported to be rejected as MinMax key")
      assert(
        !RuntimePruningKeyTypes.isFileStatsKey(unsupported),
        s"Expected $unsupported to be rejected as file stats key")
    }
  }

  test("ParquetReader consumer key types agreement with Arrow") {
    val supportedParquetReaderSparkToArrow: Seq[(DataType, String)] =
      Seq((ByteType, "Int8"), (ShortType, "Int16"), (IntegerType, "Int32"), (LongType, "Int64"))

    for ((sparkType, _) <- supportedParquetReaderSparkToArrow) {
      assert(
        RuntimePruningKeyTypes.isSupportedParquetReaderKey(sparkType),
        s"Expected $sparkType to be supported as ParquetReader key")
    }

    assert(
      RuntimePruningKeyTypes.SUPPORTED_PARQUET_READER_TYPES ==
        supportedParquetReaderSparkToArrow.map(_._1),
      "SUPPORTED_PARQUET_READER_TYPES must match expected sequence")

    val unsupportedParquetReaderTypes: Seq[DataType] = Seq(
      DateType,
      TimestampType,
      TimestampNTZType,
      StringType,
      DecimalType(10, 2),
      FloatType,
      DoubleType,
      BooleanType)

    for (unsupported <- unsupportedParquetReaderTypes) {
      assert(
        !RuntimePruningKeyTypes.isSupportedParquetReaderKey(unsupported),
        s"Expected $unsupported to be rejected as ParquetReader key")
    }
  }

  test("ColumnStats key types and Iceberg schema type string agreement") {
    // Column stats selection includes driver-side file stats types
    assert(
      RuntimePruningKeyTypes.SUPPORTED_COLUMN_STATS_TYPES ==
        RuntimePruningKeyTypes.SUPPORTED_FILE_STATS_TYPES,
      "ColumnStats supported types must match driver file stats types")

    for (sparkType <- RuntimePruningKeyTypes.SUPPORTED_COLUMN_STATS_TYPES) {
      assert(
        RuntimePruningKeyTypes.isSupportedColumnStatsKey(sparkType),
        s"Expected $sparkType to be supported for column stats")
      assert(
        RuntimePruningKeyTypes.isFileStatsKey(sparkType),
        s"Expected isFileStatsKey to return true for $sparkType")
      assert(
        RuntimePruningKeyTypes.isSupported(sparkType),
        s"Expected isSupported alias to return true for $sparkType")
    }

    val unsupportedColumnStatsTypes: Seq[DataType] =
      Seq(ByteType, ShortType, DecimalType(10, 2), DecimalType(38, 18), FloatType, DoubleType)

    for (unsupported <- unsupportedColumnStatsTypes) {
      assert(
        !RuntimePruningKeyTypes.isSupportedColumnStatsKey(unsupported),
        s"Expected $unsupported to be rejected for column stats")
      assert(
        !RuntimePruningKeyTypes.isFileStatsKey(unsupported),
        s"Expected isFileStatsKey to reject $unsupported")
    }

    val supportedIcebergTypes =
      Seq("int", "long", "date", "timestamp", "timestamptz", "string")

    for (icebergType <- supportedIcebergTypes) {
      assert(
        RuntimePruningKeyTypes.isSupportedIcebergType(icebergType),
        s"Expected Iceberg type string '$icebergType' to be supported")
      assert(
        RuntimePruningKeyTypes.isFileStatsIcebergType(icebergType),
        s"Expected isFileStatsIcebergType to return true for '$icebergType'")
    }

    val unsupportedIcebergTypes =
      Seq(
        "decimal(10,2)",
        "decimal(38,18)",
        "float",
        "double",
        "boolean",
        "binary",
        "fixed[16]",
        "uuid")

    for (icebergType <- unsupportedIcebergTypes) {
      assert(
        !RuntimePruningKeyTypes.isSupportedIcebergType(icebergType),
        s"Expected Iceberg type string '$icebergType' to be unsupported")
      assert(
        !RuntimePruningKeyTypes.isFileStatsIcebergType(icebergType),
        s"Expected isFileStatsIcebergType to reject '$icebergType'")
    }
  }
}
