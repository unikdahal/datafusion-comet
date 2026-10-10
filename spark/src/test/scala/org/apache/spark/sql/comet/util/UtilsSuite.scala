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

package org.apache.spark.sql.comet.util

import java.io.{ByteArrayOutputStream, DataOutputStream, IOException, OutputStream}
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Arrays

import scala.jdk.CollectionConverters._

import org.apache.arrow.c.CDataDictionaryProvider
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.{BaseVariableWidthVector, FieldVector, IntVector, ValueVector, VarBinaryVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.complex.ListVector
import org.apache.arrow.vector.dictionary.{Dictionary, DictionaryProvider}
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.types.pojo.{ArrowType, DictionaryEncoding, FieldType}
import org.apache.arrow.vector.util.TransferPair
import org.apache.spark.SparkEnv
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}

import org.apache.comet.CometArrowAllocator
import org.apache.comet.vector.CometVector

class UtilsSuite extends CometTestBase {

  test("batch IPC releases vectors when Arrow writer construction fails") {
    val allocator = new RootAllocator(Long.MaxValue)
    val indexType = new ArrowType.Int(32, true)
    val encoding = new DictionaryEncoding(7L, false, indexType)
    val indices = new IntVector("key", new FieldType(true, indexType, encoding), allocator)
    val root = new VectorSchemaRoot(Arrays.asList[FieldVector](indices))
    val provider = new CDataDictionaryProvider
    val out = new DataOutputStream(new ByteArrayOutputStream)
    try {
      indices.allocateNew(2)
      indices.set(0, 0)
      indices.set(1, 1)
      root.setRowCount(2)
      val failure = intercept[IllegalArgumentException] {
        // Arrow resolves dictionary fields in its constructor, before start/writeBatch.
        Utils.writeBatch(root, provider, out)
      }
      assert(failure.getMessage.contains("Could not find dictionary with ID 7"))
      assert(allocator.getAllocatedMemory == 0L)
    } finally {
      root.close()
      provider.close()
      out.close()
      allocator.close()
    }
  }

  test("batch IPC retains the write error when ending the failed stream also throws") {
    val allocator = new RootAllocator(Long.MaxValue)
    val vector = new VarCharVector("key", allocator)
    val root = new VectorSchemaRoot(Arrays.asList[FieldVector](vector))
    val writeError = new IOException("injected IPC write failure")
    val out = new DataOutputStream(new OutputStream {
      override def write(value: Int): Unit = throw writeError
    })
    try {
      vector.allocateNew()
      vector.setSafe(0, "key".getBytes(UTF_8))
      root.setRowCount(1)
      val failure = intercept[IOException](Utils.writeBatch(root, null, out))
      assert(failure eq writeError)
      assert(failure.getSuppressed.nonEmpty, "Ending IPC must fail on the same broken output")
      assert(allocator.getAllocatedMemory == 0L)
    } finally {
      root.close()
      out.close()
      allocator.close()
    }
  }

  test("batch IPC releases normalized vectors when dictionary serialization fails") {
    val allocator = new RootAllocator(Long.MaxValue)
    val indexType = new ArrowType.Int(32, true)
    val encoding = new DictionaryEncoding(7L, false, indexType)
    val indices = new IntVector("key", new FieldType(true, indexType, encoding), allocator)
    val values = new VarCharVector("values", allocator)
    val root = new VectorSchemaRoot(Arrays.asList[FieldVector](indices))
    val writeError = new IOException("injected dictionary write failure")
    val provider = new DictionaryProvider {
      private var lookups = 0
      override def getDictionaryIds: java.util.Set[java.lang.Long] =
        java.util.Collections.singleton(java.lang.Long.valueOf(7L))
      override def lookup(id: Long): Dictionary = {
        lookups += 1
        if (lookups == 1) new Dictionary(values, encoding) else throw writeError
      }
    }
    val out = new DataOutputStream(new ByteArrayOutputStream)
    try {
      values.allocateNew()
      values.setSafe(0, "value".getBytes(UTF_8))
      values.setValueCount(1)
      val dictionaryBytes = allocator.getAllocatedMemory
      indices.allocateNew(1)
      indices.set(0, 0)
      root.setRowCount(1)
      val failure = intercept[IOException](Utils.writeBatch(root, provider, out))
      assert(failure eq writeError)
      // The caller owns the dictionary. Only the batch indices and normalized copies are cleared.
      assert(allocator.getAllocatedMemory == dictionaryBytes)
      assert(values.getObject(0).toString == "value")
    } finally {
      root.close()
      values.close()
      out.close()
      allocator.close()
    }
  }

  test("offset normalization releases earlier and partially transferred columns on failure") {
    val allocator = new RootAllocator(Long.MaxValue)
    val first = new VarCharVector("first", allocator)
    val transferError = new IOException("injected partial transfer failure")
    val second = new IntVector("second", allocator) {
      override def getTransferPair(targetAllocator: BufferAllocator): TransferPair = {
        val pair = super.getTransferPair(targetAllocator)
        new TransferPair {
          override def getTo: ValueVector = pair.getTo
          override def transfer(): Unit = pair.transfer()
          override def copyValueSafe(from: Int, to: Int): Unit = pair.copyValueSafe(from, to)
          override def splitAndTransfer(start: Int, length: Int): Unit = {
            pair.splitAndTransfer(start, length)
            throw transferError
          }
        }
      }
    }
    val root = new VectorSchemaRoot(Arrays.asList[FieldVector](first, second))
    try {
      first.allocateNew()
      first.setSafe(0, "retained source".getBytes(UTF_8))
      second.allocateNew(1)
      second.set(0, 42)
      root.setRowCount(1)
      val sourceBytes = allocator.getAllocatedMemory
      val buffers = root.getFieldVectors.asScala.flatMap(_.getFieldBuffers.asScala).toSeq
      val refs = buffers.map(_.refCnt())
      val failure = intercept[IOException](Utils.normalizeBatchOffsets(root))
      assert(failure eq transferError)
      assert(allocator.getAllocatedMemory == sourceBytes)
      assert(buffers.map(_.refCnt()) == refs)
      assert(first.getObject(0).toString == "retained source")
      assert(second.get(0) == 42)
      root.clear()
      assert(allocator.getAllocatedMemory == 0L)
    } finally {
      root.close()
      allocator.close()
    }
  }

  test("batch IPC round trips incompressible binary data across scratch chunks") {
    val payload = new Array[Byte](1024 * 1024)
    new java.util.Random(17).nextBytes(payload)
    val vector = new VarBinaryVector("b", CometArrowAllocator)
    val provider = new CDataDictionaryProvider
    try {
      vector.allocateNew()
      vector.setSafe(0, payload)
      vector.setNull(1)
      vector.setValueCount(2)
      val batch =
        new ColumnarBatch(Array[ColumnVector](CometVector.getVector(vector, provider)), 2)
      val (count, bytes) = Utils.serializeBatches(Iterator(batch)).next()
      assert(count == 2)
      assert(bytes.getChunks.length > 1, "The fixture must exercise multiple IPC chunks")
      val decoded = Utils.decodeBatches(bytes, "multi-chunk-binary")
      val output = decoded.next()
      assert(Arrays.equals(output.column(0).getBinary(0), payload))
      assert(output.column(0).isNullAt(1))
      assert(!decoded.hasNext)
    } finally {
      vector.close()
      provider.close()
    }
  }

  test("broadcast transport normalizes sliced string and binary offsets with nulls") {
    val values = Seq(
      Some(""),
      None,
      Some("short"),
      Some("long \u03bb" * 20),
      Some("last"),
      None,
      Some("tail"),
      Some("end"))

    def slicedBuffer(serializeWithUtils: Boolean): ChunkedByteBuffer = {
      val originals = Seq[BaseVariableWidthVector](
        new VarCharVector("s", CometArrowAllocator),
        new VarBinaryVector("b", CometArrowAllocator))
      val sliced = Seq[BaseVariableWidthVector](
        new VarCharVector("s", CometArrowAllocator),
        new VarBinaryVector("b", CometArrowAllocator))
      val root = new VectorSchemaRoot(Arrays.asList[FieldVector](sliced: _*))
      try {
        originals.zip(sliced).foreach { case (original, slice) =>
          original.allocateNew()
          (Seq.fill(8)(Some("unused prefix" * 20)) ++ values).zipWithIndex.foreach {
            case (Some(value), row) => original.setSafe(row, value.getBytes(UTF_8))
            case (None, row) => original.setNull(row)
          }
          original.setValueCount(16)
          slice.loadFieldBuffers(
            new ArrowFieldNode(8, 2),
            Arrays.asList(
              original.getValidityBuffer.slice(1, 1),
              original.getOffsetBuffer.slice(8 * 4, 9 * 4),
              original.getDataBuffer))
          assert(slice.getOffsetBuffer.getInt(0) > 0)
        }
        root.setRowCount(8)
        if (serializeWithUtils) {
          val provider = new CDataDictionaryProvider
          val columns = sliced.map(vector => CometVector.getVector(vector, provider))
          val batch = new ColumnarBatch(columns.toArray[ColumnVector], 8)
          Utils.serializeBatches(Iterator(batch)).next()._2
        } else {
          // External IPC retains its original offsets; the coalescer must normalize it too.
          val output = new ChunkedByteBufferOutputStream(1024, ByteBuffer.allocate)
          val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
          val compressed = new DataOutputStream(codec.compressedOutputStream(output))
          val writer = new ArrowStreamWriter(root, null, Channels.newChannel(compressed))
          try {
            writer.start()
            writer.writeBatch()
          } finally {
            writer.close()
          }
          output.toChunkedByteBuffer
        }
      } finally {
        root.close()
        originals.foreach(_.close())
      }
    }

    Seq(false, true).foreach { serializeWithUtils =>
      val input = Seq.fill(3)(slicedBuffer(serializeWithUtils))
      if (serializeWithUtils) {
        input.foreach { bytes =>
          val decoded = Utils.decodeBatches(bytes, "compact-broadcast")
          val batch = decoded.next()
          (0 until batch.numCols()).foreach { column =>
            val vector = batch.column(column).asInstanceOf[CometVector].getValueVector
            assert(vector.getOffsetBuffer.getInt(0) == 0)
            assert(vector.getDataBuffer.capacity() < 1024)
          }
          assert(!decoded.hasNext)
        }
      }
      val (buffers, batchCount, rowCount) = Utils.coalesceBroadcastBatches(input.iterator)
      assert(batchCount == 3)
      assert(rowCount == 24)
      val actual = buffers.iterator.flatMap { bytes =>
        Utils.decodeBatches(bytes, "sliced-broadcast").flatMap { batch =>
          (0 until batch.numRows()).map { row =>
            val string =
              if (batch.column(0).isNullAt(row)) None
              else Some(batch.column(0).getUTF8String(row).toString)
            val binary =
              if (batch.column(1).isNullAt(row)) None
              else Some(batch.column(1).getBinary(row).toSeq)
            (string, binary)
          }
        }
      }.toSeq
      val expected =
        Seq.fill(3)(values).flatten.map(value => (value, value.map(_.getBytes(UTF_8).toSeq)))
      assert(actual == expected)
    }
  }

  test("broadcast transport normalizes sliced lists and their string children") {
    val expected = Seq(
      Some(Seq(Some("λ"), None, Some(""))),
      None,
      Some(Seq.empty),
      Some(Seq(Some("tail"))),
      Some(Seq(None)),
      Some(Seq(Some("last"))),
      None,
      Some(Seq(Some("end"))))

    def buffer(serializeWithUtils: Boolean): ChunkedByteBuffer = {
      val allocator = new RootAllocator(Long.MaxValue)
      val original = ListVector.empty("list", allocator)
      val sliced = ListVector.empty("list", allocator)
      val childType = FieldType.nullable(ArrowType.Utf8.INSTANCE)
      val originalValues = original.addOrGetVector[VarCharVector](childType).getVector
      val slicedValues = sliced.addOrGetVector[VarCharVector](childType).getVector
      val root = new VectorSchemaRoot(Arrays.asList[FieldVector](sliced))
      val provider = new CDataDictionaryProvider
      try {
        original.allocateNew()
        val rows = Seq.fill(8)(Some(Seq(Some("unused prefix" * 20)))) ++ expected
        rows.zipWithIndex.foreach {
          case (None, row) => original.setNull(row)
          case (Some(values), row) =>
            val start = original.startNewValue(row)
            values.zipWithIndex.foreach {
              case (Some(value), index) => originalValues.setSafe(start + index, value.getBytes(UTF_8))
              case (None, index) => originalValues.setNull(start + index)
            }
            original.endValue(row, values.size)
        }
        original.setValueCount(rows.size)
        slicedValues.loadFieldBuffers(
          new ArrowFieldNode(originalValues.getValueCount, originalValues.getNullCount),
          originalValues.getFieldBuffers)
        sliced.loadFieldBuffers(
          new ArrowFieldNode(8, 2),
          Arrays.asList(
            original.getValidityBuffer.slice(1, 1),
            original.getOffsetBuffer.slice(8 * 4, 9 * 4)))
        root.setRowCount(8)
        assert(sliced.getOffsetBuffer.getInt(0) > 0)
        val bytes = if (serializeWithUtils) {
          val batch = new ColumnarBatch(
            Array[ColumnVector](CometVector.getVector(sliced, provider)),
            8)
          Utils.serializeBatches(Iterator(batch)).next()._2
        } else {
          val output = new ChunkedByteBufferOutputStream(1024, ByteBuffer.allocate)
          val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
          val out = new DataOutputStream(codec.compressedOutputStream(output))
          val writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))
          try {
            writer.start()
            writer.writeBatch()
          } finally {
            writer.close()
          }
          output.toChunkedByteBuffer
        }
        bytes
      } finally {
        root.close()
        original.close()
        provider.close()
        assert(allocator.getAllocatedMemory == 0L)
        allocator.close()
      }
    }

    Seq(false, true).foreach { serializeWithUtils =>
      val input = Seq.fill(2)(buffer(serializeWithUtils))
      val (buffers, batchCount, rowCount) = Utils.coalesceBroadcastBatches(input.iterator)
      assert(batchCount == 2)
      assert(rowCount == 16)
      val actual = buffers.iterator.flatMap { bytes =>
        Utils.decodeBatches(bytes, "sliced-list").flatMap { batch =>
          val vector = batch.column(0).asInstanceOf[CometVector].getValueVector
          val list = vector.asInstanceOf[ListVector]
          assert(list.getOffsetBuffer.getInt(0) == 0)
          assert(list.getDataVector.asInstanceOf[VarCharVector].getOffsetBuffer.getInt(0) == 0)
          (0 until batch.numRows()).map { row =>
            if (batch.column(0).isNullAt(row)) None
            else {
              val array = batch.column(0).getArray(row)
              Some((0 until array.numElements()).map { index =>
                if (array.isNullAt(index)) None else Some(array.getUTF8String(index).toString)
              })
            }
          }
        }
      }.toSeq
      assert(actual == expected ++ expected)
    }
  }

  test("broadcast dictionary fallback preserves independent dictionaries and earlier batches") {
    def buffer(value: String, dictionaryEncoded: Boolean): ChunkedByteBuffer = {
      val allocator = new RootAllocator(Long.MaxValue)
      val values = new VarCharVector("key", allocator)
      val indexType = new ArrowType.Int(32, true)
      val encoding = new DictionaryEncoding(7L, false, indexType)
      val indices = new IntVector("key", new FieldType(true, indexType, encoding), allocator)
      val provider = new MapDictionaryProvider(new Dictionary(values, encoding))
      try {
        values.allocateNew()
        values.setSafe(0, value.getBytes(UTF_8))
        values.setValueCount(1)
        indices.allocateNew(1)
        indices.set(0, 0)
        indices.setValueCount(1)
        val vector = if (dictionaryEncoded) indices else values
        val batch = new ColumnarBatch(
          Array[ColumnVector](CometVector.getVector(vector, provider)),
          1)
        Utils.serializeBatches(Iterator(batch)).next()._2
      } finally {
        indices.close()
        provider.close()
        assert(allocator.getAllocatedMemory == 0L)
        allocator.close()
      }
    }

    // The first buffer creates a target root. The dictionary buffer must release it and return
    // all original buffers, including the last buffer whose dictionary reuses ID 7 differently.
    val input = Seq(buffer("plain", false), buffer("alpha", true), buffer("beta", true))
    val (buffers, batchCount, rowCount) = Utils.coalesceBroadcastBatches(input.iterator)
    assert(batchCount == 0L)
    assert(rowCount == 0L)
    assert(buffers.length == input.length)
    assert(buffers.zip(input).forall { case (actual, original) => actual eq original })
    val values = buffers.iterator.flatMap { bytes =>
      Utils.decodeBatches(bytes, "dictionary-fallback").flatMap { batch =>
        (0 until batch.numRows()).map(row => batch.column(0).getUTF8String(row).toString)
      }
    }.toSeq
    assert(values == Seq("plain", "alpha", "beta"))
  }

  test("broadcast coalescing preserves all DISTINCT string keys across batch boundaries") {
    withSQLConf(
      "spark.sql.adaptive.enabled" -> "false",
      "spark.sql.shuffle.partitions" -> "4",
      "spark.comet.expression.Cast.allowIncompatible" -> "true") {
      val df = spark
        .range(100000, 140000, 1, 4)
        .selectExpr("concat('k', lpad(cast(id as string), 10, '0')) AS id")
        .distinct()
      val executed = df.queryExecution.executedPlan
      val plan = executed
        .collectFirst { case columnar: org.apache.spark.sql.comet.CometColumnarToRowExec =>
          columnar.child
        }
        .getOrElse(fail(s"No native DISTINCT output: $executed"))
      val buffers = plan
        .executeColumnar()
        .mapPartitions(iter => Utils.serializeBatches(iter))
        .collect()
        .map(_._2)
      def keys(input: Iterator[ChunkedByteBuffer]): Seq[String] = {
        input.flatMap { bytes =>
          Utils.decodeBatches(bytes, "broadcast-test").flatMap { batch =>
            (0 until batch.numRows()).map(row => batch.column(0).getUTF8String(row).toString)
          }
        }.toSeq
      }
      val original = keys(buffers.iterator)
      val (coalesced, _, _) = Utils.coalesceBroadcastBatches(buffers.iterator)
      val actual = keys(coalesced.iterator)
      val expected = (100000 until 140000).map(id => f"k$id%010d")
      assert(original.sorted == expected)
      assert(actual.sorted == expected)
    }
  }

  test("serializeBatches preserves row count for a zero-column batch") {
    val numRows = 5
    val batch = new ColumnarBatch(Array.empty[ColumnVector], numRows)

    val (rowCount, buf) = Utils.serializeBatches(Iterator(batch)).next()
    assert(rowCount == numRows)

    val decoded = Utils.decodeBatches(buf, "test").toSeq
    assert(decoded.map(_.numRows()).sum == numRows)
  }

  test("coalesceBroadcastBatches preserves row count across zero-column inputs") {
    val numRows = 5
    val numBatches = 3
    val batches =
      (0 until numBatches).map(_ => new ColumnarBatch(Array.empty[ColumnVector], numRows))

    val bufs = Utils.serializeBatches(batches.iterator).map(_._2).toSeq.iterator
    val (coalesced, batchCount, totalRows) = Utils.coalesceBroadcastBatches(bufs)

    val expected = numRows.toLong * numBatches
    assert(batchCount == numBatches)
    assert(totalRows == expected)

    val decoded = coalesced.iterator.flatMap(b => Utils.decodeBatches(b, "test")).toSeq
    assert(decoded.map(_.numRows()).sum == expected)
  }

  test("serializeBatches materializes ConstantColumnVector columns") {
    // Spark wraps file-source partition columns and other per-batch constants in
    // ConstantColumnVector. When such a batch reaches Comet's serialization/export path
    // (getBatchFieldVectors), it must be materialized to an Arrow vector rather than
    // rejected with "Comet execution only takes Arrow Arrays".
    val numRows = 4

    val valueCol = new ConstantColumnVector(numRows, IntegerType)
    valueCol.setInt(42)
    val nullCol = new ConstantColumnVector(numRows, IntegerType)
    nullCol.setNull()
    val batch = new ColumnarBatch(Array[ColumnVector](valueCol, nullCol), numRows)

    val (rowCount, buf) = Utils.serializeBatches(Iterator(batch)).next()
    assert(rowCount == numRows)

    // Read the decoded values eagerly: ArrowReaderIterator releases a batch's buffers once the
    // iterator advances past it (hasNext closes the previous batch), so values must be read from
    // the current batch before calling hasNext/next again.
    val it = Utils.decodeBatches(buf, "test")
    assert(it.hasNext)
    val out = it.next()
    assert(out.numCols() == 2)
    assert(out.numRows() == numRows)
    val values = (0 until numRows).map(i => out.column(0).getInt(i))
    val nulls = (0 until numRows).map(i => out.column(1).isNullAt(i))
    assert(!it.hasNext)

    assert(values.forall(_ == 42), s"expected all 42, got $values")
    assert(nulls.forall(identity), s"expected all null, got $nulls")
  }

  test("serializeBatches materializes a TimestampType ConstantColumnVector") {
    // Covers the TimestampType materialize path (TimestampWriter -> TimeStampMicroTZVector) and
    // pins down the "UTC" timezone choice in materializeConstantColumnVector: Spark stores
    // TimestampType as micros in UTC, and Comet tags its timestamp Arrow vectors "UTC", so the
    // constant micros round-trip unchanged. This guards against anyone later swapping the zone
    // argument, which would make the materialised constant's Arrow field metadata diverge from the
    // sibling non-constant timestamp columns it shares a VectorSchemaRoot with.
    val numRows = 3
    // 2023-11-14T22:13:20Z in micros since epoch.
    val micros = 1700000000000000L

    val tsCol = new ConstantColumnVector(numRows, TimestampType)
    tsCol.setLong(micros)
    val batch = new ColumnarBatch(Array[ColumnVector](tsCol), numRows)

    val (rowCount, buf) = Utils.serializeBatches(Iterator(batch)).next()
    assert(rowCount == numRows)

    val it = Utils.decodeBatches(buf, "test")
    assert(it.hasNext)
    val out = it.next()
    assert(out.numCols() == 1)
    assert(out.numRows() == numRows)
    val got = (0 until numRows).map(i => out.column(0).getLong(i))
    assert(!it.hasNext)

    assert(got.forall(_ == micros), s"expected all $micros, got $got")
  }

  test("serializeBatches materializes a nullable StructType ConstantColumnVector") {
    // Exercises a different ArrowFieldWriter path than the scalar cases: a struct constant is
    // written via getStruct(rowId) -> getChild(ordinal). Covers both a non-null struct (with a
    // null nested field) and a wholly-null struct constant.
    val numRows = 3
    val schema = StructType(
      Seq(StructField("id", IntegerType), StructField("name", StringType, nullable = true)))

    // Non-null struct whose `name` field is null, proving nested nullability round-trips.
    val structCol = new ConstantColumnVector(numRows, schema)
    structCol.setNotNull()
    val idChild = new ConstantColumnVector(numRows, IntegerType)
    idChild.setInt(7)
    val nameChild = new ConstantColumnVector(numRows, StringType)
    nameChild.setNull()
    structCol.setChild(0, idChild)
    structCol.setChild(1, nameChild)

    // A wholly-null struct constant.
    val nullStructCol = new ConstantColumnVector(numRows, schema)
    nullStructCol.setNull()
    nullStructCol.setChild(0, new ConstantColumnVector(numRows, IntegerType))
    nullStructCol.setChild(1, new ConstantColumnVector(numRows, StringType))

    val batch =
      new ColumnarBatch(Array[ColumnVector](structCol, nullStructCol), numRows)

    val (rowCount, buf) = Utils.serializeBatches(Iterator(batch)).next()
    assert(rowCount == numRows)

    val it = Utils.decodeBatches(buf, "test")
    assert(it.hasNext)
    val out = it.next()
    assert(out.numCols() == 2)
    assert(out.numRows() == numRows)
    val ids = (0 until numRows).map(i => out.column(0).getStruct(i).getInt(0))
    val nameNulls = (0 until numRows).map(i => out.column(0).getStruct(i).isNullAt(1))
    val structNulls = (0 until numRows).map(i => out.column(1).isNullAt(i))
    assert(!it.hasNext)

    assert(ids.forall(_ == 7), s"expected all id 7, got $ids")
    assert(nameNulls.forall(identity), s"expected all name null, got $nameNulls")
    assert(structNulls.forall(identity), s"expected all struct null, got $structNulls")
  }

  test("isArrowBacked rejects large-offset Arrow vectors") {
    // A CometPlainVector can wrap a LargeVarCharVector or LargeVarBinaryVector -- an accelerated
    // mapInArrow returning pa.large_string() produces one -- but getFieldVector rejects both. If
    // isArrowBacked accepted them, a caller would take the direct write path and then fail, so it
    // must report false and let the caller convert the batch instead.
    val numRows = 2
    Seq[org.apache.arrow.vector.FieldVector](
      {
        val v = new org.apache.arrow.vector.LargeVarCharVector("s", CometArrowAllocator)
        v.allocateNew(numRows)
        v.setSafe(0, "hello".getBytes("UTF-8"))
        v.setSafe(1, "world".getBytes("UTF-8"))
        v.setValueCount(numRows)
        v
      }, {
        val v = new org.apache.arrow.vector.LargeVarBinaryVector("b", CometArrowAllocator)
        v.allocateNew(numRows)
        v.setSafe(0, "hello".getBytes("UTF-8"))
        v.setSafe(1, "world".getBytes("UTF-8"))
        v.setValueCount(numRows)
        v
      }).foreach { vector =>
      try {
        val col = CometVector.getVector(vector, new CDataDictionaryProvider)
        val batch = new ColumnarBatch(Array[ColumnVector](col), numRows)
        assert(
          !Utils.isArrowBacked(batch),
          s"${vector.getClass.getSimpleName} must not be reported as directly writable")
      } finally {
        vector.close()
      }
    }
  }
}
