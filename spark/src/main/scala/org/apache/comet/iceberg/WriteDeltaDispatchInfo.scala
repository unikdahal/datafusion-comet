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

package org.apache.comet.iceberg

import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.comet.util.Utils
import org.apache.spark.sql.types.StructType

case class DeltaOperationCodes(
    delete: Int,
    update: Int,
    insert: Int,
    reinsert: Option[Int])

case class DeltaMetadataLayout(
    filePathIndex: Int,
    rowPositionIndex: Int,
    specIdIndex: Option[Int],
    partitionIndex: Option[Int])

/**
 * Version-neutral task contract for Spark's operation-coded WriteDelta row stream.
 *
 * The Spark projection wrappers are retained for the JVM writer path. Their schemas and ordinal
 * vectors are also copied explicitly so the native ABI never needs to inspect Catalyst objects.
 */
case class WriteDeltaDispatchInfo(
    operationOrdinal: Int,
    rowOrdinals: Option[IndexedSeq[Int]],
    rowIdOrdinals: IndexedSeq[Int],
    metadataOrdinals: Option[IndexedSeq[Int]],
    rowSchema: Option[StructType],
    rowIdSchema: StructType,
    metadataSchema: Option[StructType],
    metadataLayout: DeltaMetadataLayout,
    operationCodes: DeltaOperationCodes,
    rowProjection: Option[ProjectingInternalRow],
    rowIdProjection: ProjectingInternalRow,
    metadataProjection: Option[ProjectingInternalRow],
    command: Option[DeltaCommand] = None)

private[iceberg] object WriteDeltaDispatchInfo {

  private val FilePathColumn = "_file"
  private val RowPositionColumn = "_pos"
  private val SpecIdColumn = "_spec_id"
  private val PartitionColumn = "_partition"

  def build(
      projections: org.apache.spark.sql.catalyst.util.WriteDeltaProjections,
      childOutput: Seq[org.apache.spark.sql.catalyst.expressions.Attribute],
      operationCodes: DeltaOperationCodes,
      command: Option[DeltaCommand] = None): Option[WriteDeltaDispatchInfo] = {
    val rowIdProjection = projections.rowIdProjection
    val rowProjection = projections.rowProjection
    val metadataProjection = projections.metadataProjection

    def descriptor(
        projection: ProjectingInternalRow): Option[(IndexedSeq[Int], StructType)] = {
      val ordinals = projection.colOrdinals.toIndexedSeq
      val schema = projection.schema
      if (ordinals.length != schema.length ||
        ordinals.exists(index => index < 0 || index >= childOutput.length) ||
        ordinals.zip(schema.fields).exists { case (index, field) =>
          !Utils.equalsIgnoreCompatibleNullability(
            childOutput(index).dataType,
            field.dataType)
        }) {
        None
      } else {
        Some(ordinals -> schema)
      }
    }

    def uniqueIndex(schema: StructType, name: String): Option[Int] = {
      val matches = schema.fields.zipWithIndex.collect { case (field, index) if field.name == name =>
        index
      }
      if (matches.length == 1) Some(matches.head) else None
    }

    for {
      rowIdDescriptor <- descriptor(rowIdProjection)
      filePathIndex <- uniqueIndex(rowIdDescriptor._2, FilePathColumn)
      rowPositionIndex <- uniqueIndex(rowIdDescriptor._2, RowPositionColumn)
      rowDescriptor <- rowProjection match {
        case Some(projection) => descriptor(projection).map(Some(_))
        case None => Some(None)
      }
      metadataDescriptor <- metadataProjection match {
        case Some(projection) => descriptor(projection).map(Some(_))
        case None => Some(None)
      }
      specIdIndex <- metadataDescriptor match {
        case Some((_, schema)) => uniqueIndex(schema, SpecIdColumn).map(Some(_))
        case None => Some(None)
      }
      partitionIndex <- metadataDescriptor match {
        case Some((_, schema)) => uniqueIndex(schema, PartitionColumn).map(Some(_))
        case None => Some(None)
      }
      if childOutput.nonEmpty &&
        childOutput.head.dataType == org.apache.spark.sql.types.IntegerType &&
        !childOutput.head.nullable
    } yield WriteDeltaDispatchInfo(
      operationOrdinal = 0,
      rowOrdinals = rowDescriptor.map(_._1),
      rowIdOrdinals = rowIdDescriptor._1,
      metadataOrdinals = metadataDescriptor.map(_._1),
      rowSchema = rowDescriptor.map(_._2),
      rowIdSchema = rowIdDescriptor._2,
      metadataSchema = metadataDescriptor.map(_._2),
      metadataLayout = DeltaMetadataLayout(
        filePathIndex,
        rowPositionIndex,
        specIdIndex,
        partitionIndex),
      operationCodes = operationCodes,
      rowProjection = rowProjection,
      rowIdProjection = rowIdProjection,
      metadataProjection = metadataProjection,
      command = command)
  }
}
