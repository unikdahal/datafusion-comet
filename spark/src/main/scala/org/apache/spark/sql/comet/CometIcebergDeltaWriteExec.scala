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

import java.util.ArrayList

import scala.jdk.CollectionConverters._

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, UnsafeProjection}
import org.apache.spark.sql.comet.execution.arrow.CometArrowStream
import org.apache.spark.sql.comet.util.{Utils => CometUtils}
import org.apache.spark.sql.connector.write.BatchWrite
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import com.google.protobuf.{ByteString, CodedOutputStream}

import org.apache.comet.CometExecIterator
import org.apache.comet.iceberg.{DataManifestContent, DeleteManifestContent, IcebergDeltaReflection, IcebergReflection, TransportManifest}
import org.apache.comet.iceberg.{DeltaDelete, DeltaMerge, DeltaUpdate, IcebergSemanticMetricsShim}
import org.apache.comet.serde.OperatorOuterClass.IcebergDeltaCommand
import org.apache.comet.serde.OperatorOuterClass.Operator

/** Native executor-side writer for Iceberg V2 PositionDeltaWrite task output. */
case class CometIcebergDeltaWriteExec(
    nativeOp: Operator,
    child: SparkPlan,
    @transient batchWrite: BatchWrite,
    @transient table: AnyRef,
    outputSpecId: Int,
    @transient specsById: java.util.Map[Integer, AnyRef],
    previousDeletesBroadcast: Option[Broadcast[Array[Byte]]] = None)
    extends CometNativeExec
    with UnaryExecNode {

  override def originalPlan: SparkPlan = child
  override def output: Seq[Attribute] = Seq(
    AttributeReference(IcebergWriteExec.CommitMessageColumn, BinaryType, nullable = false)())
  override def supportsColumnar: Boolean = false

  override def executeCollect(): Array[InternalRow] = doExecute().collect()

  override def serializedPlanOpt: SerializedPlan = {
    val size = nativeOp.getSerializedSize
    val bytes = new Array[Byte](size)
    val codedOutput = CodedOutputStream.newInstance(bytes)
    nativeOp.writeTo(codedOutput)
    codedOutput.checkNoSpaceLeft()
    SerializedPlan(Some(bytes))
  }

  override def withNewChildInternal(newChild: SparkPlan): SparkPlan = copy(child = newChild)

  override def nodeName: String = "CometIcebergDeltaWrite"

  override def stringArgs: Iterator[Any] = {
    val delta = nativeOp.getIcebergDeltaWrite
    Iterator(output, s"${delta.getDataCommon.getDataLocation}, ${delta.getDeleteGranularity}")
  }

  private def semanticCommand =
    nativeOp.getIcebergDeltaWrite.getCommand match {
      case IcebergDeltaCommand.ICEBERG_DELTA_COMMAND_DELETE => Some(DeltaDelete)
      case IcebergDeltaCommand.ICEBERG_DELTA_COMMAND_UPDATE => Some(DeltaUpdate)
      case IcebergDeltaCommand.ICEBERG_DELTA_COMMAND_MERGE => Some(DeltaMerge)
      case _ => None
    }

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of input delta rows"),
    "numDataFiles" -> SQLMetrics.createMetric(sparkContext, "number of data files written"),
    "numDataRowsWritten" -> SQLMetrics.createMetric(sparkContext, "number of data rows written"),
    "numDeleteFiles" -> SQLMetrics.createMetric(sparkContext, "number of delete files written"),
    "numDeleteRecords" -> SQLMetrics.createMetric(sparkContext, "number of delete records"),
    "numReferencedDataFiles" -> SQLMetrics.createMetric(sparkContext, "referenced data files"),
    "bytesWritten" -> SQLMetrics.createSizeMetric(sparkContext, "written output"),
    "write_time" -> SQLMetrics.createNanoTimingMetric(
      sparkContext,
      "time in native Iceberg writer")) ++
    IcebergSemanticMetricsShim.deltaMetrics(sparkContext, semanticCommand)

  override def doExecute(): RDD[InternalRow] = {
    val columnarRdd = doExecuteColumnar()
    val dataFilesMetric = longMetric("numDataFiles")
    val dataRowsMetric = longMetric("numDataRowsWritten")
    val deleteFilesMetric = longMetric("numDeleteFiles")
    val deleteRowsMetric = longMetric("numDeleteRecords")
    val referencedMetric = longMetric("numReferencedDataFiles")
    val bytesMetric = longMetric("bytesWritten")
    val schemaTypes = output.map(_.dataType).toArray

    val tableIO =
      IcebergDeltaWriteExec.requireReflection(IcebergReflection.getTableIO(table), "Table.io()")
    val outputSpec = IcebergDeltaWriteExec.requireReflection(
      IcebergReflection.getPartitionSpecById(table, outputSpecId),
      s"partition spec id=$outputSpecId")
    val metricsConfig = IcebergDeltaWriteExec.requireReflection(
      IcebergReflection.metricsConfigForTable(table),
      "MetricsConfig.forTable")
    val positionDeltaWrite = IcebergDeltaWriteExec.requireReflection(
      IcebergReflection.getOuterPositionDeltaWrite(batchWrite),
      "outer PositionDeltaWrite")
    val decodedWriteSchema = IcebergDeltaWriteExec
      .requireReflection(
        IcebergReflection.getWriteSchemaFromPositionDeltaWrite(positionDeltaWrite),
        "PositionDeltaWrite.Context.dataSchema")
      .asInstanceOf[AnyRef]
    val sortOrderId = nativeOp.getIcebergDeltaWrite.getDataCommon.getSortOrderId
    val sortOrder = IcebergDeltaWriteExec.requireReflection(
      IcebergReflection.getSortOrderById(table, sortOrderId),
      s"sort order id=$sortOrderId")
    val capturedSpecs = specsById
    val capturedPreviousDeletes = previousDeletesBroadcast

    columnarRdd.mapPartitionsInternal { batches =>
      IcebergDeltaReflection.executorReflectionUnresolved().foreach { reason =>
        throw new IllegalStateException(reason)
      }
      val cleanup = new CometIcebergWriteExec.WrittenFileCleanup(tableIO)
      Option(TaskContext.get()).foreach(_.addTaskFailureListener(cleanup))
      val previousDeleteFiles = capturedPreviousDeletes
        .map(broadcast => IcebergDeltaWriteExec.decodePreviousDeleteFiles(broadcast.value))
        .getOrElse(Map.empty[String, AnyRef])
      val (payloadBytes, locations) = drainNativePayload(batches)
      cleanup.own(locations)
      val payload = org.apache.comet.serde.OperatorOuterClass.IcebergDeltaTaskPayload
        .parseFrom(payloadBytes)
      require(
        payload.getSchemaRevision == 1,
        s"Unsupported Iceberg delta payload revision ${payload.getSchemaRevision}")
      longMetric("numOutputRows").add(payload.getInputRows)

      val dataFiles = new ArrayList[AnyRef]()
      val deleteFiles = new ArrayList[AnyRef]()
      val transport = payload.getManifestsList
      val manifests = new java.util.ArrayList[TransportManifest](transport.size())
      val iterator = transport.iterator()
      while (iterator.hasNext) {
        val manifest = iterator.next()
        val content = manifest.getContentValue match {
          case 0 => DataManifestContent
          case 1 => DeleteManifestContent
          case value =>
            throw new IllegalArgumentException(s"Unknown task manifest content $value")
        }
        if (!capturedSpecs.containsKey(Integer.valueOf(manifest.getPartitionSpecId))) {
          throw new IllegalArgumentException(
            s"Task payload references unknown partition spec ${manifest.getPartitionSpecId}")
        }
        if (content == DataManifestContent && manifest.getPartitionSpecId != outputSpecId) {
          throw new IllegalArgumentException(
            s"DATA manifest uses spec ${manifest.getPartitionSpecId}, " +
              s"expected current output spec $outputSpecId")
        }
        manifests.add(
          TransportManifest(
            content,
            manifest.getPartitionSpecId,
            manifest.getAvroManifest.toByteArray))
      }
      val manifestIterator = manifests.iterator()
      while (manifestIterator.hasNext) {
        val manifest = manifestIterator.next()
        val files = IcebergDeltaReflection.decodeTransportManifest(manifest, capturedSpecs)
        val target = if (manifest.content == DataManifestContent) dataFiles else deleteFiles
        val fileIterator = files.iterator()
        while (fileIterator.hasNext) target.add(fileIterator.next())
      }

      val rewrittenDeleteFiles = new ArrayList[AnyRef]()
      val rewrittenLocations = new java.util.HashSet[String]()
      val rewrittenIterator = payload.getRewrittenDeleteFileLocationsList.iterator()
      while (rewrittenIterator.hasNext) {
        val location = rewrittenIterator.next()
        require(
          rewrittenLocations.add(location),
          s"Native Iceberg delta payload repeated rewritten delete file $location")
        val original = previousDeleteFiles.getOrElse(
          location,
          throw new IllegalArgumentException(
            s"Native Iceberg delta payload rewrote unknown delete file $location"))
        rewrittenDeleteFiles.add(original)
      }

      val dataSpec = outputSpec.asInstanceOf[AnyRef]
      val rebuiltData = IcebergReflection.rebuildDataFilesWithJavaMetrics(
        dataFiles,
        tableIO,
        metricsConfig,
        dataSpec,
        decodedWriteSchema,
        sortOrder)
      val referencedDataFiles = new ArrayList[CharSequence](payload.getReferencedDataFilesCount)
      val referencedIterator = payload.getReferencedDataFilesList.iterator()
      while (referencedIterator.hasNext) referencedDataFiles.add(referencedIterator.next())
      val writeResult = IcebergDeltaReflection.buildDeltaWriteResult(
        rebuiltData,
        deleteFiles,
        referencedDataFiles,
        rewrittenDeleteFiles)
      val commit = IcebergDeltaReflection.buildDeltaTaskCommit(writeResult)

      val (dataRows, dataBytes) = IcebergReflection.sumDataFileMetrics(rebuiltData)
      val (deleteRows, deleteBytes) = IcebergReflection.sumDataFileMetrics(deleteFiles)
      dataFilesMetric.add(rebuiltData.size().toLong)
      dataRowsMetric.add(dataRows)
      deleteFilesMetric.add(deleteFiles.size().toLong)
      deleteRowsMetric.add(deleteRows)
      referencedMetric.add(referencedDataFiles.size().toLong)
      bytesMetric.add(dataBytes + deleteBytes)
      Option(TaskContext.get()).foreach { tc =>
        val outputMetrics = tc.taskMetrics().outputMetrics
        outputMetrics.setBytesWritten(dataBytes + deleteBytes)
        outputMetrics.setRecordsWritten(dataRows + deleteRows)
      }
      val projection = UnsafeProjection.create(schemaTypes)
      Iterator.single(projection(InternalRow(IcebergWriteExec.serializeMessage(commit))).copy())
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val childRDD =
      if (child.supportsColumnar) {
        child.executeColumnar()
      } else {
        throw new UnsupportedOperationException(
          "CometIcebergDeltaWriteExec requires a columnar Comet-native child; got " +
            child.getClass.getName)
      }
    val partitions = childRDD.getNumPartitions
    val capturedNativeOp = nativeOp
    val capturedPreviousDeletes = previousDeletesBroadcast
    childRDD.mapPartitionsInternal { batches =>
      IcebergDeltaReflection.executorReflectionUnresolved().foreach { reason =>
        throw new IllegalStateException(reason)
      }
      val partitionId = TaskContext.getPartitionId()
      val taskAttemptId = TaskContext.get().taskAttemptId()
      val previousBlob = capturedPreviousDeletes
        .map(broadcast => ByteString.copyFrom(broadcast.value))
        .getOrElse(ByteString.EMPTY)
      val delta = capturedNativeOp.getIcebergDeltaWrite.toBuilder
        .setPartitionId(partitionId)
        .setTaskAttemptId(taskAttemptId)
        .setPreviousDeletesBlob(previousBlob)
        .build()
      val taskNativeOp = capturedNativeOp.toBuilder.setIcebergDeltaWrite(delta).build()
      val bytes = new Array[Byte](taskNativeOp.getSerializedSize)
      val codedOutput = CodedOutputStream.newInstance(bytes)
      taskNativeOp.writeTo(codedOutput)
      codedOutput.checkNoSpaceLeft()
      new CometExecIterator(
        CometExec.newIterId,
        CometArrowStream.inputObjects(
          batches,
          CometUtils.fromAttributes(child.output),
          "CometIcebergDeltaWriteExec"),
        2,
        bytes,
        CometMetricNode(metrics, Nil),
        partitions,
        partitionId,
        None,
        Seq.empty)
    }
  }

  private def drainNativePayload(batches: Iterator[ColumnarBatch]): (Array[Byte], Seq[String]) = {
    require(batches.hasNext, "iceberg_delta_write produced no output batch for this task")
    val batch = batches.next()
    val payload =
      try {
        require(
          batch.numRows() == 1,
          s"iceberg_delta_write expected one row, got ${batch.numRows()}")
        require(
          batch.numCols() == 2,
          s"iceberg_delta_write expected two columns, got ${batch.numCols()}")
        val bytes = batch.column(0).getBinary(0)
        val locations = CometIcebergWriteExec.decodeLocations(batch.column(1).getBinary(0))
        (bytes, locations)
      } finally batch.close()
    require(!batches.hasNext, "iceberg_delta_write produced more than one batch for this task")
    payload
  }
}

private object IcebergDeltaWriteExec {
  def requireReflection[A](value: Option[A], description: String): A =
    value.getOrElse(
      throw new IllegalStateException(s"Native Iceberg delta write: $description unavailable"))

  def decodePreviousDeleteFiles(bytes: Array[Byte]): Map[String, AnyRef] = {
    if (bytes.isEmpty) return Map.empty
    val transport =
      org.apache.comet.serde.OperatorOuterClass.IcebergPreviousDeletes.parseFrom(bytes)
    val groups = transport.getGroupsList.asScala.toSeq
    val expected = groups
      .map { group =>
        require(
          group.getDataFile.nonEmpty,
          "Previous delete transport has an empty data file path")
        group.getDataFile -> group.getDeleteFilesList.asScala.map(_.getLocation).toSeq
      }
    require(
      expected.map(_._1).distinct.size == expected.size,
      "Previous delete transport repeats a data file group")
    val expectedByDataFile = expected.toMap
    val serialized = transport.getSerializedDeleteFiles.toByteArray
    require(
      serialized.nonEmpty,
      "Previous delete transport is missing original DeleteFile objects")
    val original =
      Utils.deserialize[Map[String, Seq[AnyRef]]](serialized, Utils.getContextOrSparkClassLoader)
    require(
      original.keySet == expectedByDataFile.keySet,
      "Previous delete transport group keys disagree")

    val byLocation = scala.collection.mutable.HashMap.empty[String, AnyRef]
    expectedByDataFile.foreach { case (dataFile, locations) =>
      val files = original(dataFile)
      val actualLocations = files.map(file =>
        IcebergReflection.extractFileLocation(file).getOrElse {
          throw new IllegalArgumentException("Original DeleteFile has no location()/path()")
        })
      require(
        actualLocations.sorted == locations.sorted,
        s"Previous delete transport locations disagree for data file $dataFile")
      files.foreach { file =>
        val location = IcebergReflection.extractFileLocation(file).get
        require(
          byLocation.put(location, file).isEmpty,
          s"Previous delete transport repeats location $location")
      }
    }
    byLocation.toMap
  }

}
