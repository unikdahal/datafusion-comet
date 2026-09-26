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

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.comet.IcebergWriteExec

import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.serde.operator.CometIcebergNativeScan

/** JVM-resolved inputs shared by ordinary Iceberg writes and V2 row-level delta writes. */
private[comet] final case class IcebergNativeWriteEnvironment(
    table: AnyRef,
    fileIO: AnyRef,
    formatVersion: Int,
    metadataLocation: String,
    dataLocation: String,
    catalogName: Option[String],
    operationId: String,
    icebergVersion: String,
    tableProperties: Map[String, String],
    resolvedWriteProperties: Map[String, String],
    effectiveProperties: Map[String, String],
    fileIOProperties: Map[String, String],
    hadoopObjectStoreProperties: Map[String, String],
    catalogProperties: Map[String, String],
    writeSchema: AnyRef,
    writeSchemaJson: String,
    outputSpec: AnyRef,
    outputSpecId: Int,
    outputSpecJson: String,
    sortOrderId: Int,
    targetDataFileSize: Long,
    useFanoutWriter: Boolean)

private[comet] object IcebergNativeWriteEnvironment {

  /** Resolves all common environment values without creating files or mutating the table. */
  def resolve(
      op: IcebergWriteExec,
      table: AnyRef,
      writeSchema: AnyRef,
      outputSpec: AnyRef,
      operationId: String,
      targetDataFileSize: Long,
      useFanoutWriter: Boolean,
      sortOrderId: Int,
      resolvedWriteProperties: Map[String, String])
      : Either[String, IcebergNativeWriteEnvironment] =
    try {
      val tableProperties = IcebergReflection
        .getTableProperties(table)
        .map(_.asScala.toMap)
        .getOrElse(Map.empty[String, String])
      val fileIOProperties = IcebergReflection
        .getFileIOProperties(table)
        .getOrElse(Map.empty[String, String])
      val fileIO = IcebergReflection
        .getFileIO(table)
        .map(_.asInstanceOf[AnyRef])
        .toRight("could not resolve table.io()")
      val formatVersion = IcebergReflection
        .getFormatVersion(table)
        .toRight("could not determine the table format-version")
      val dataLocation = IcebergReflection
        .getDataLocation(table)
        .toRight("Table.locationProvider().newDataLocation reflection failed")
      val schemaJson = IcebergReflection
        .schemaToJson(writeSchema)
        .toRight("SchemaParser.toJson failed")
      val outputSpecId = IcebergReflection
        .getSpecId(outputSpec)
        .toRight("PartitionSpec.specId reflection failed")
      val specJson = IcebergReflection
        .partitionSpecToJson(outputSpec)
        .toRight("PartitionSpecParser.toJson failed")

      for {
        io <- fileIO
        version <- formatVersion
        location <- dataLocation
        serializedSchema <- schemaJson
        specId <- outputSpecId
        serializedSpec <- specJson
        environment <- {
          val writeHadoopConf = op.session.sessionState.newHadoopConf()
          val dataUri = new java.net.URI(location)
          val bucket = NativeConfig.bucketForUri(dataUri, Set.empty)
          val hadoopProperties = CometIcebergNativeScan.hadoopToIcebergS3Properties(
            NativeConfig.extractObjectStoreOptions(writeHadoopConf, dataUri),
            bucket)
          val catalogProperties = hadoopProperties ++ fileIOProperties
          Right(
            IcebergNativeWriteEnvironment(
              table = table,
              fileIO = io,
              formatVersion = version,
              metadataLocation = IcebergReflection.getMetadataLocation(table).getOrElse(""),
              dataLocation = location,
              catalogName = IcebergReflection.deriveCatalogName(table),
              operationId = operationId,
              icebergVersion = IcebergReflection.icebergVersion(),
              tableProperties = tableProperties,
              resolvedWriteProperties = resolvedWriteProperties,
              effectiveProperties = tableProperties ++ resolvedWriteProperties,
              fileIOProperties = fileIOProperties,
              hadoopObjectStoreProperties = hadoopProperties,
              catalogProperties = catalogProperties,
              writeSchema = writeSchema,
              writeSchemaJson = serializedSchema,
              outputSpec = outputSpec,
              outputSpecId = specId,
              outputSpecJson = serializedSpec,
              sortOrderId = sortOrderId,
              targetDataFileSize = targetDataFileSize,
              useFanoutWriter = useFanoutWriter))
        }
      } yield environment
    } catch {
      case NonFatal(e) =>
        Left(s"could not resolve shared Iceberg write environment: ${e.getMessage}")
    }
}
