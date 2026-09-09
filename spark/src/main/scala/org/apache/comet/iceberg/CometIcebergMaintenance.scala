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

import java.net.URI
import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession

import org.apache.comet.objectstore.NativeConfig

/** Native Iceberg maintenance operations backed by Comet's Rust execution library. */
object CometIcebergMaintenance {

  private val DefaultRetentionMillis = TimeUnit.DAYS.toMillis(3)
  // Match Iceberg's procedure behavior when no explicit delete executor is configured. Callers
  // that want parallel native deletes can opt in through maxConcurrentDeletes.
  private val DefaultConcurrentDeletes = 1
  private val CatalogCredentialProvider = "s3.comet.credential.provider.class"
  private val EncryptionKeyIdProperty = "encryption.key-id"
  private val HadoopFileIOClass = "org.apache.iceberg.hadoop.HadoopFileIO"
  private val S3FileIOClass = "org.apache.iceberg.aws.s3.S3FileIO"
  private val FlagDryRun = 1
  private val FlagAllowUnsafeOlderThan = 2

  // Keep this marker synchronized with native/core/src/iceberg_maintenance.rs. JNI wraps native
  // errors in generic RuntimeExceptions, so fallback needs a machine-readable discriminator that
  // is independent of human-facing wording.
  private[comet] val FallbackMarker = "[COMET_ICEBERG_FALLBACK]"

  // Native listing is proven for local files and S3. GCS, OSS, and Azure fall back to
  // Iceberg-Java until table-scoped credential wiring has end to end tests.
  private val SupportedSchemes = Set("file", "s3", "s3a", "s3n")

  private def fallbackUnsupported(message: String): UnsupportedOperationException =
    new UnsupportedOperationException(s"$FallbackMarker $message")

  /**
   * Whether the native implementation is expected to handle this table. Anything else falls back
   * to Iceberg-Java. Maintenance uses a deliberately narrower FileIO gate than native reads: this
   * path deletes data, so wrappers, subclasses, resolving FileIOs, and credential-routing FileIOs
   * are rejected unless their semantics are explicitly reproduced and tested natively.
   */
  def isNativeSupported(table: Any): Boolean = {
    try {
      val metadataLocation =
        IcebergReflection.getMetadataLocation(table).getOrElse(return false)
      if (!isSupportedScheme(metadataLocation)) {
        return false
      }
      val fileIO = IcebergReflection.getFileIO(table).getOrElse(return false)
      if (!isMaintenanceFileIOClass(fileIO.getClass.getName, metadataLocation)) {
        return false
      }
      val props = IcebergReflection.getFileIOProperties(table).getOrElse(Map.empty)
      if (props.contains(CatalogCredentialProvider)) {
        return false
      }
      val tableProps =
        try {
          IcebergReflection.getTableProperties(table).map { m =>
            import scala.jdk.CollectionConverters._
            m.asScala.toMap
          }
        } catch {
          case _: Exception => None
        }
      if (tableProps.exists(_.contains(EncryptionKeyIdProperty))) {
        return false
      }
      true
    } catch {
      case _: Exception => false
    }
  }

  private def schemeOf(location: String): Option[String] = {
    try {
      Option(URI.create(location).getScheme).map(_.toLowerCase(Locale.ROOT))
    } catch {
      case _: Exception => None
    }
  }

  private def isSupportedScheme(location: String): Boolean =
    schemeOf(location).orElse(Some("file")).exists(SupportedSchemes.contains)

  private[comet] def isMaintenanceFileIOClass(
      fileIOClassName: String,
      location: String): Boolean =
    schemeOf(location).getOrElse("file") match {
      case "file" => fileIOClassName == HadoopFileIOClass
      case "s3" | "s3a" | "s3n" => fileIOClassName == S3FileIOClass
      case _ => false
    }

  /**
   * Finds and optionally deletes files under an Iceberg table location that are not reachable
   * from the table's current metadata.
   *
   * Unsupported tables or storage fall back to Iceberg-Java via an exception containing the
   * machine-readable [[FallbackMarker]]. Callers that already replaced the parsed plan (see
   * `CometRemoveOrphanFilesCommand`) catch only that marker and re-execute the original SQL.
   *
   * @param table
   *   an Iceberg `org.apache.iceberg.Table` instance
   * @param olderThanMillis
   *   only files older than this epoch-millis cutoff are considered; defaults to three days ago
   * @param location
   *   optional location to scan instead of the table root
   * @param dryRun
   *   when true, return orphan candidates without deleting them
   * @param maxConcurrentDeletes
   *   maximum number of native object-store delete requests in flight
   * @param equalSchemes
   *   URI scheme aliases, using Iceberg's comma-separated-key convention
   * @param equalAuthorities
   *   URI authority aliases, using Iceberg's comma-separated-key convention
   * @param prefixMismatchMode
   *   ERROR, IGNORE, or DELETE; ERROR is the safe default
   */
  def removeOrphanFiles(
      table: Any,
      olderThanMillis: Option[Long] = None,
      location: Option[String] = None,
      dryRun: Boolean = false,
      maxConcurrentDeletes: Int = DefaultConcurrentDeletes,
      equalSchemes: Map[String, String] = Map.empty,
      equalAuthorities: Map[String, String] = Map.empty,
      prefixMismatchMode: String = "ERROR"): Seq[String] =
    removeOrphanFiles(
      SparkSession.active,
      table,
      olderThanMillis,
      location,
      dryRun,
      maxConcurrentDeletes,
      equalSchemes,
      equalAuthorities,
      prefixMismatchMode)

  def removeOrphanFiles(
      spark: SparkSession,
      table: Any,
      olderThanMillis: Option[Long],
      location: Option[String],
      dryRun: Boolean,
      maxConcurrentDeletes: Int,
      equalSchemes: Map[String, String],
      equalAuthorities: Map[String, String],
      prefixMismatchMode: String): Seq[String] = {
    require(table != null, "table must not be null")
    require(maxConcurrentDeletes > 0, "maxConcurrentDeletes must be greater than 0")

    val fileIO = IcebergReflection.getFileIO(table).getOrElse {
      throw fallbackUnsupported("Cannot determine the Iceberg FileIO for remove_orphan_files")
    }
    val metadataLocation = IcebergReflection.getMetadataLocation(table).getOrElse {
      throw fallbackUnsupported(
        "Cannot determine the Iceberg metadata file location for remove_orphan_files")
    }
    if (!isSupportedScheme(metadataLocation)) {
      throw fallbackUnsupported(
        s"Unsupported storage scheme for native remove_orphan_files: $metadataLocation")
    }

    val tableLocation =
      IcebergReflection.getMethod(table.getClass, "location").invoke(table).asInstanceOf[String]
    val scanLocation = location.getOrElse(tableLocation)
    if (!isSupportedScheme(scanLocation)) {
      throw fallbackUnsupported(
        s"Unsupported scan location for native remove_orphan_files: $scanLocation")
    }

    val fileIOClassName = fileIO.getClass.getName
    if (!isMaintenanceFileIOClass(fileIOClassName, metadataLocation) ||
      !isMaintenanceFileIOClass(fileIOClassName, scanLocation)) {
      throw fallbackUnsupported(
        s"Iceberg FileIO $fileIOClassName is not proven safe for native remove_orphan_files " +
          s"across metadata location '$metadataLocation' and scan location '$scanLocation'")
    }

    val fileIOProperties = IcebergReflection.getFileIOProperties(table).getOrElse(Map.empty)
    if (fileIOProperties.contains(CatalogCredentialProvider)) {
      throw fallbackUnsupported(
        s"Native remove_orphan_files does not support catalog property '$CatalogCredentialProvider'")
    }

    val hadoopConf = spark.sessionState.newHadoopConf()
    val metadataUri =
      try {
        URI.create(metadataLocation)
      } catch {
        case _: Exception =>
          throw fallbackUnsupported(
            s"Unsupported metadata location for native remove_orphan_files: $metadataLocation")
      }
    val scanUri =
      try {
        URI.create(scanLocation)
      } catch {
        case _: Exception =>
          throw fallbackUnsupported(
            s"Unsupported scan location for native remove_orphan_files: $scanLocation")
      }
    val metadataOptions = NativeConfig.extractObjectStoreOptions(hadoopConf, metadataUri)
    val scanOptions = NativeConfig.extractObjectStoreOptions(hadoopConf, scanUri)
    val fileIOStorageOptions = icebergFileIOToObjectStoreOptions(fileIOProperties)
    val objectStoreOptions = metadataOptions ++ scanOptions ++ fileIOStorageOptions

    val cutoff = olderThanMillis.getOrElse(System.currentTimeMillis() - DefaultRetentionMillis)
    val allowUnsafeOlderThan =
      java.lang.Boolean.parseBoolean(spark.conf.get("spark.testing", "false"))

    val native = new NativeIcebergMaintenance()
    val flags =
      (if (dryRun) FlagDryRun else 0) |
        (if (allowUnsafeOlderThan) FlagAllowUnsafeOlderThan else 0)
    native
      .removeOrphanFiles(
        metadataLocation,
        scanLocation,
        cutoff,
        maxConcurrentDeletes,
        fileIOProperties.asJava,
        objectStoreOptions.asJava,
        equalSchemes.asJava,
        equalAuthorities.asJava,
        prefixMismatchMode.toUpperCase(Locale.ROOT),
        flags)
      .toSeq
  }

  /**
   * Converts portable S3 properties exposed by Iceberg FileIO implementations to the Hadoop S3A
   * keys already consumed by Comet's native object_store implementation. Only S3 is mapped: other
   * backends fall back to Iceberg-Java. Unknown properties are ignored rather than forwarded to
   * the wrong storage backend.
   */
  private[iceberg] def icebergFileIOToObjectStoreOptions(
      properties: Map[String, String]): Map[String, String] =
    properties.flatMap { case (key, value) =>
      key match {
        case "s3.access-key-id" => Some("fs.s3a.access.key" -> value)
        case "s3.secret-access-key" => Some("fs.s3a.secret.key" -> value)
        case "s3.session-token" => Some("fs.s3a.session.token" -> value)
        case "s3.endpoint" => Some("fs.s3a.endpoint" -> value)
        case "s3.path-style-access" => Some("fs.s3a.path.style.access" -> value)
        case "s3.region" => Some("fs.s3a.endpoint.region" -> value)
        case _ => None
      }
    }
}