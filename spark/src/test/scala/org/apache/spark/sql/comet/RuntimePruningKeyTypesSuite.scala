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

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types._

import org.apache.comet.serde.{AggSerde, Compatible, Unsupported}

/** JVM key-type policy tests; native reader attachment is tested separately in Rust. */
class RuntimePruningKeyTypesSuite extends AnyFunSuite {

  private val orderedTypes: Seq[DataType] =
    Seq(IntegerType, LongType, DateType, TimestampType, TimestampNTZType, StringType)

  private val unsupportedTypes: Seq[DataType] = Seq(
    FloatType,
    DoubleType,
    BooleanType,
    BinaryType,
    ArrayType(IntegerType),
    MapType(StringType, StringType),
    StructType(Seq(StructField("a", IntegerType))))

  test("driver file-statistics key policy includes exact decimals") {
    val supported = orderedTypes ++ Seq(DecimalType(10, 2), DecimalType(38, 18))
    supported.foreach { dataType =>
      assert(RuntimePruningKeyTypes.isFileStatsKey(dataType), dataType.toString)
      val attr = AttributeReference("key", dataType)()
      assert(RuntimePruningKeyTypes.isFileStatsAttribute(attr), dataType.toString)
    }
    // Native joins also accept Int8/Int16, but Iceberg has no BYTE/SHORT schema types.
    (unsupportedTypes ++ Seq(ByteType, ShortType)).foreach { dataType =>
      assert(!RuntimePruningKeyTypes.isFileStatsKey(dataType), dataType.toString)
    }
  }

  test("TopK and MIN/MAX key policies exclude decimals and narrow integers") {
    orderedTypes.foreach { dataType =>
      assert(RuntimePruningKeyTypes.isSupportedTopKKey(dataType), dataType.toString)
      assert(RuntimePruningKeyTypes.isSupportedMinMaxKey(dataType), dataType.toString)
    }
    val rejected = unsupportedTypes ++
      Seq(ByteType, ShortType, DecimalType(10, 2), DecimalType(38, 18))
    rejected.foreach { dataType =>
      assert(!RuntimePruningKeyTypes.isSupportedTopKKey(dataType), dataType.toString)
      assert(!RuntimePruningKeyTypes.isSupportedMinMaxKey(dataType), dataType.toString)
    }
  }

  test("Parquet reader policy accepts only signed integer keys") {
    Seq(ByteType, ShortType, IntegerType, LongType).foreach { dataType =>
      assert(RuntimePruningKeyTypes.isParquetReaderKey(dataType), dataType.toString)
    }
    val rejected = unsupportedTypes ++
      Seq(DateType, TimestampType, TimestampNTZType, StringType, DecimalType(10, 2))
    rejected.foreach { dataType =>
      assert(!RuntimePruningKeyTypes.isParquetReaderKey(dataType), dataType.toString)
    }
  }

  test("CHAR data types and CHAR metadata fail closed") {
    val directChar = AttributeReference("char", CharType(8))()
    assert(RuntimePruningKeyTypes.isCharType(directChar))
    assert(!RuntimePruningKeyTypes.isBinaryStringType(directChar.dataType))
    assert(!RuntimePruningKeyTypes.isFileStatsAttribute(directChar))
    assert(!RuntimePruningKeyTypes.isSupportedTopKKey(directChar.dataType))
    assert(!RuntimePruningKeyTypes.isSupportedMinMaxKey(directChar.dataType))
    val metadataTypes = Seq("char(8)" -> true, "CHAR(8)" -> true, "varchar(8)" -> false)
    for ((typeName, isChar) <- metadataTypes) {
      val metadata = new MetadataBuilder()
        .putString("__CHAR_VARCHAR_TYPE_STRING", typeName)
        .build()
      val attr = AttributeReference("text", StringType, metadata = metadata)()
      assert(RuntimePruningKeyTypes.isCharType(attr) == isChar)
      assert(RuntimePruningKeyTypes.isFileStatsAttribute(attr) == !isChar)
    }
  }

  test("batch binary-string MIN/MAX remains native while window MIN/MAX falls back") {
    assert(AggSerde.minMaxSupportLevel(StringType) == Compatible())
    assert(!AggSerde.minMaxDataTypeSupported(StringType))
    assert(AggSerde.minMaxSupportLevel(CharType(8)).isInstanceOf[Unsupported])
  }

  test("binary string comparison policy excludes non-default collations") {
    assert(RuntimePruningKeyTypes.isBinaryStringType(StringType))
    assert(!RuntimePruningKeyTypes.isBinaryStringType(IntegerType))
    // StringType.apply(String) exists only on Spark 4; reflection keeps this suite cross-version.
    val factory = StringType.getClass.getMethods.find(method =>
      method.getName == "apply" && method.getParameterTypes.toSeq == Seq(classOf[String]))
    if (org.apache.spark.SPARK_VERSION.startsWith("4.")) assert(factory.nonEmpty)
    factory.foreach { method =>
      Seq("UTF8_LCASE", "UTF8_BINARY_RTRIM").foreach { name =>
        val collated = method.invoke(StringType, name).asInstanceOf[DataType]
        assert(!RuntimePruningKeyTypes.isBinaryStringType(collated))
        assert(!RuntimePruningKeyTypes.isFileStatsKey(collated))
        assert(!RuntimePruningKeyTypes.isSupportedTopKKey(collated))
        assert(!RuntimePruningKeyTypes.isSupportedMinMaxKey(collated))
        assert(AggSerde.minMaxSupportLevel(collated).isInstanceOf[Unsupported])
      }
    }
  }

  test("Iceberg schema policy accepts supported primitives and valid decimal domains") {
    Seq("int", "long", "date", "timestamp", "timestamptz", "string").foreach { name =>
      assert(RuntimePruningKeyTypes.isFileStatsIcebergType(name), name)
    }
    for (precision <- 1 to 38; scale <- 0 to precision) {
      assert(RuntimePruningKeyTypes.isFileStatsIcebergType(s"decimal($precision,$scale)"))
      assert(RuntimePruningKeyTypes.isFileStatsIcebergType(s"decimal($precision, $scale)"))
    }
    val rejected = Seq(
      "float",
      "double",
      "boolean",
      "binary",
      "fixed[16]",
      "uuid",
      "timestamp_ns",
      "decimal(",
      "decimal(0,0)",
      "decimal(39,0)",
      "decimal(10,11)",
      "decimal(10,-1)",
      "decimal(10,2)junk",
      "decimal(999999999999999999,2)")
    rejected.foreach { name =>
      assert(!RuntimePruningKeyTypes.isFileStatsIcebergType(name), name)
    }
  }
}
