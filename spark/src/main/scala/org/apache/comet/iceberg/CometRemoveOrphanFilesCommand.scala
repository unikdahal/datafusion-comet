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

import java.util.Locale

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.CometConf

/**
 * Runnable Spark control-plane command for native `remove_orphan_files`.
 *
 * The command prefers the native Rust implementation but falls back to Iceberg-Java whenever the
 * call uses unsupported options or storage. Fallback reparses the original SQL with a thread-local
 * parser bypass, preserving Iceberg's canonical binding and errors without mutating session-wide
 * configuration.
 */
private[iceberg] case class CometRemoveOrphanFilesCommand(
    catalogName: String,
    argumentNames: Seq[Option[String]],
    argumentExpressions: Seq[Expression],
    originalSql: Option[String] = None)
    extends LeafRunnableCommand {

  require(
    argumentNames.length == argumentExpressions.length,
    "remove_orphan_files argument metadata is inconsistent")

  override val output: Seq[AttributeReference] =
    Seq(
      org.apache.spark.sql.catalyst.expressions
        .AttributeReference("orphan_file_location", StringType, nullable = false)())

  override def run(spark: SparkSession): Seq[Row] = {
    val conf = spark.sessionState.conf
    if (!CometConf.COMET_ENABLED.get(conf) ||
      !CometConf.COMET_EXEC_ENABLED.get(conf) ||
      !CometConf.COMET_ICEBERG_NATIVE_ENABLED.get(conf) ||
      !CometConf.COMET_ICEBERG_REMOVE_ORPHAN_FILES_NATIVE_ENABLED.get(conf)) {
      return fallback(spark, "native maintenance is disabled")
    }

    val values =
      try {
        bindArguments()
      } catch {
        case NonFatal(_) => return fallback(spark, "argument binding")
      }

    val parsed =
      try {
        parseArguments(values)
      } catch {
        // Interception happens before Spark/Iceberg procedure binding. Any expression whose type
        // or coercion semantics we do not reproduce exactly must go back through the canonical
        // binder rather than being interpreted by Comet.
        case NonFatal(_) => return fallback(spark, "argument parsing")
      }

    // JVM-only options: let Iceberg-Java handle them. In particular, prefix_listing changes the
    // listing implementation and validates SupportsPrefixOperations; silently treating it as the
    // ordinary object-store listing path changes procedure semantics.
    if (parsed.fileListView.isDefined) {
      return fallback(spark, "file_list_view")
    }
    if (parsed.prefixListing) {
      return fallback(spark, "prefix_listing=true")
    }
    if (parsed.streamResults) {
      return fallback(spark, "stream_results=true")
    }

    val catalog =
      try {
        spark.sessionState.catalogManager.catalog(catalogName) match {
          case tableCatalog: TableCatalog => tableCatalog
          case _ => return fallback(spark, "non-table catalog")
        }
      } catch {
        case NonFatal(_) => return fallback(spark, "catalog lookup")
      }

    val icebergTable =
      try {
        resolveIcebergTable(spark, catalog, parsed.tableName)
      } catch {
        case NonFatal(_) => return fallback(spark, "table resolution")
      }
    if (icebergTable.isEmpty) {
      return fallback(spark, "non-Iceberg table")
    }

    if (!CometIcebergMaintenance.isNativeSupported(icebergTable.get)) {
      return fallback(spark, "unsupported FileIO or storage")
    }

    try {
      CometIcebergMaintenance
        .removeOrphanFiles(
          spark,
          icebergTable.get,
          olderThanMillis = parsed.olderThanMillis,
          location = parsed.location,
          dryRun = parsed.dryRun,
          maxConcurrentDeletes = parsed.maxConcurrentDeletes,
          equalSchemes = parsed.equalSchemes,
          equalAuthorities = parsed.equalAuthorities,
          prefixMismatchMode = parsed.prefixMismatchMode)
        .map(Row(_))
    } catch {
      // JNI surfaces native errors as generic RuntimeExceptions. Only the stable machine-readable
      // marker is control flow; user errors such as retention, gc.enabled, and prefix mismatch do
      // not carry it and therefore still propagate.
      case e: Exception
          if Option(e.getMessage).exists(_.contains(CometIcebergMaintenance.FallbackMarker)) =>
        fallback(spark, e.getMessage)
    }
  }

  private def fallback(spark: SparkSession, reason: String): Seq[Row] = {
    originalSql match {
      case Some(sql) =>
        logFallback(reason)
        CometIcebergMaintenanceParser.withNativeMaintenanceBypassed {
          spark.sql(sql).collect().toSeq
        }
      case None =>
        throw new UnsupportedOperationException(
          s"Native remove_orphan_files cannot handle this call ($reason). " +
            s"Set ${CometConf.COMET_ICEBERG_REMOVE_ORPHAN_FILES_NATIVE_ENABLED.key}=false " +
            "to use Iceberg-Java.")
    }
  }

  private def logFallback(reason: String): Unit = {
    logWarning(s"Comet native remove_orphan_files falling back to Iceberg-Java ($reason)")
  }

  private def resolveIcebergTable(
      spark: SparkSession,
      catalog: TableCatalog,
      tableName: String): Option[Any] = {
    val rawParts = spark.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    val tableParts =
      if (rawParts.headOption.exists(_.equalsIgnoreCase(catalogName))) rawParts.tail else rawParts
    if (tableParts.isEmpty) {
      return None
    }
    val identifier = Identifier.of(tableParts.dropRight(1).toArray, tableParts.last)
    val sparkTable = catalog.loadTable(identifier)
    IcebergReflection
      .findMethodInHierarchy(sparkTable.getClass, "table")
      .map(_.invoke(sparkTable))
  }

  private case class ParsedArgs(
      tableName: String,
      olderThanMillis: Option[Long],
      location: Option[String],
      dryRun: Boolean,
      maxConcurrentDeletes: Int,
      fileListView: Option[String],
      equalSchemes: Map[String, String],
      equalAuthorities: Map[String, String],
      prefixMismatchMode: String,
      prefixListing: Boolean,
      streamResults: Boolean)

  private def parseArguments(values: Map[String, Expression]): ParsedArgs = {
    val tableName = values
      .get("table")
      .flatMap(parseNullableString(_, "table"))
      .filter(_.nonEmpty)
      .getOrElse(
        throw new IllegalArgumentException("remove_orphan_files requires a non-empty table"))

    val olderThanMillis = values.get("older_than").flatMap(parseNullableTimestampMillis)
    val location = values.get("location").flatMap(parseNullableString(_, "location"))
    val dryRun =
      values.get("dry_run").flatMap(parseNullableBoolean(_, "dry_run")).getOrElse(false)
    val maxConcurrentDeletes = values
      .get("max_concurrent_deletes")
      .flatMap(parseNullableInt(_, "max_concurrent_deletes"))
      .getOrElse(1)
    require(
      maxConcurrentDeletes > 0,
      s"max_concurrent_deletes should have value > 0, value: $maxConcurrentDeletes")
    val fileListView =
      values.get("file_list_view").flatMap(parseNullableString(_, "file_list_view"))
    val equalSchemes = values
      .get("equal_schemes")
      .map(parseStringMap(_, "equal_schemes"))
      .getOrElse(Map.empty)
    val equalAuthorities = values
      .get("equal_authorities")
      .map(parseStringMap(_, "equal_authorities"))
      .getOrElse(Map.empty)
    val prefixMismatchMode = values
      .get("prefix_mismatch_mode")
      .flatMap(parseNullableString(_, "prefix_mismatch_mode"))
      .getOrElse("ERROR")
    val prefixListing = values
      .get("prefix_listing")
      .flatMap(parseNullableBoolean(_, "prefix_listing"))
      .getOrElse(false)
    val streamResults = values
      .get("stream_results")
      .flatMap(parseNullableBoolean(_, "stream_results"))
      .getOrElse(false)

    ParsedArgs(
      tableName,
      olderThanMillis,
      location,
      dryRun,
      maxConcurrentDeletes,
      fileListView,
      equalSchemes,
      equalAuthorities,
      prefixMismatchMode,
      prefixListing,
      streamResults)
  }

  private def bindArguments(): Map[String, Expression] = {
    val names = CometRemoveOrphanFilesParameters.supportedNames
    val values = mutable.LinkedHashMap.empty[String, Expression]
    var positional = 0
    var sawNamed = false

    argumentNames.zip(argumentExpressions).foreach { case (name, expression) =>
      name match {
        case Some(rawName) =>
          sawNamed = true
          val normalized = rawName.toLowerCase(Locale.ROOT)
          if (!names.contains(normalized)) {
            throw new IllegalArgumentException(
              s"Unknown remove_orphan_files argument for this Iceberg version: $rawName")
          }
          if (values.contains(normalized)) {
            throw new IllegalArgumentException(
              s"Duplicate remove_orphan_files argument: $rawName")
          }
          values(normalized) = expression

        case None =>
          if (sawNamed) {
            throw new IllegalArgumentException(
              "Positional remove_orphan_files arguments cannot follow named arguments")
          }
          if (positional >= names.length) {
            throw new IllegalArgumentException(
              "Too many remove_orphan_files arguments for this Iceberg version: " +
                s"expected at most ${names.length}")
          }
          values(names(positional)) = expression
          positional += 1
      }
    }
    values.toMap
  }

  private def evalFoldable(expression: Expression, name: String): (Any, DataType) = {
    if (!expression.resolved || !expression.foldable) {
      throw new IllegalArgumentException(
        s"remove_orphan_files argument '$name' must be a foldable expression: ${expression.sql}")
    }
    (expression.eval(InternalRow.empty), expression.dataType)
  }

  private def parseNullableString(expression: Expression, name: String): Option[String] = {
    val (value, dataType) = evalFoldable(expression, name)
    if (value == null) {
      None
    } else if (dataType != StringType) {
      throw typeError(name, "STRING", dataType)
    } else {
      Some(value.asInstanceOf[UTF8String].toString)
    }
  }

  private def parseNullableBoolean(expression: Expression, name: String): Option[Boolean] = {
    val (value, dataType) = evalFoldable(expression, name)
    if (value == null) {
      None
    } else if (dataType != BooleanType) {
      throw typeError(name, "BOOLEAN", dataType)
    } else {
      Some(value.asInstanceOf[Boolean])
    }
  }

  private def parseNullableInt(expression: Expression, name: String): Option[Int] = {
    val (value, dataType) = evalFoldable(expression, name)
    if (value == null) {
      None
    } else if (dataType != IntegerType) {
      throw typeError(name, "INT", dataType)
    } else {
      Some(value.asInstanceOf[Int])
    }
  }

  private def parseNullableTimestampMillis(expression: Expression): Option[Long] = {
    val (value, dataType) = evalFoldable(expression, "older_than")
    if (value == null) {
      None
    } else if (dataType != TimestampType) {
      // Do not reinterpret TIMESTAMP_NTZ's wall-clock micros as an epoch instant. Falling back lets
      // Spark/Iceberg apply the procedure binder's timezone-sensitive coercion semantics.
      throw typeError("older_than", "TIMESTAMP", dataType)
    } else {
      Some(Math.floorDiv(value.asInstanceOf[Long], 1000L))
    }
  }

  private def parseStringMap(expression: Expression, name: String): Map[String, String] = {
    val (value, dataType) = evalFoldable(expression, name)
    if (value == null) {
      return Map.empty
    }
    dataType match {
      case MapType(StringType, StringType, _) =>
      case other => throw typeError(name, "MAP<STRING,STRING>", other)
    }

    val map = value.asInstanceOf[MapData]
    val keys = map.keyArray()
    val values = map.valueArray()
    (0 until map.numElements()).map { index =>
      if (keys.isNullAt(index) || values.isNullAt(index)) {
        throw new IllegalArgumentException(
          s"remove_orphan_files argument '$name' cannot contain null keys or values")
      }
      keys.getUTF8String(index).toString -> values.getUTF8String(index).toString
    }.toMap
  }

  private def typeError(
      name: String,
      expected: String,
      actual: DataType): IllegalArgumentException =
    new IllegalArgumentException(
      s"Wrong arg type for $name: expected $expected but found ${actual.sql}")
}