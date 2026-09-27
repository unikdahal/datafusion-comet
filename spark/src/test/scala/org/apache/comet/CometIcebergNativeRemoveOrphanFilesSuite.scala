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

package org.apache.comet

import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.nio.file.attribute.FileTime
import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._

import org.apache.iceberg.Table
import org.apache.spark.{CometDriverPlugin, SparkConf}
import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}

import org.apache.comet.iceberg.{CometIcebergMaintenance, CometIcebergMaintenanceExtensions, CometIcebergMaintenanceParser, IcebergReflection}

class CometIcebergNativeRemoveOrphanFilesSuite extends CometTestBase with CometIcebergTestBase {

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(
      "spark.sql.extensions",
      Seq(
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "org.apache.comet.iceberg.CometIcebergMaintenanceExtensions").mkString(","))
  }

  private def withCatalog(warehouse: java.io.File)(f: String => Unit): Unit = {
    val catalog = "orphan_cat"
    withSQLConf(
      s"spark.sql.catalog.$catalog" -> "org.apache.iceberg.spark.SparkCatalog",
      s"spark.sql.catalog.$catalog.type" -> "hadoop",
      s"spark.sql.catalog.$catalog.warehouse" -> warehouse.getAbsolutePath,
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      CometConf.COMET_ICEBERG_NATIVE_ENABLED.key -> "true",
      CometConf.COMET_ICEBERG_REMOVE_ORPHAN_FILES_NATIVE_ENABLED.key -> "true") {
      f(catalog)
    }
  }

  private def makeOld(path: Path): Unit =
    Files.setLastModifiedTime(
      path,
      FileTime.fromMillis(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(5)))

  private def sameLocalPath(reported: String, path: Path): Boolean =
    Paths.get(new URI(reported)).toAbsolutePath.normalize() == path.toAbsolutePath.normalize()

  private def loadIcebergTable(tableName: String): Table = {
    val parts = spark.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    require(parts.length >= 2, s"expected catalog.db.table, got $tableName")
    val catalog =
      spark.sessionState.catalogManager.catalog(parts.head).asInstanceOf[TableCatalog]
    val ident = Identifier.of(parts.dropRight(1).tail.toArray, parts.last)
    val sparkTable = catalog.loadTable(ident)
    IcebergReflection
      .findMethodInHierarchy(sparkTable.getClass, "table")
      .map(_.invoke(sparkTable).asInstanceOf[Table])
      .getOrElse(throw new IllegalArgumentException(s"not an Iceberg table: $tableName"))
  }

  private def contentFileLocation(file: Any): String =
    IcebergReflection
      .extractFileLocation(file)
      .getOrElse(throw new IllegalStateException("cannot read file location"))

  private def firstDataFile(tableName: String): Path = {
    val table = loadIcebergTable(tableName)
    val tasks = table.newScan().planFiles()
    try {
      val iterator = tasks.iterator()
      assert(iterator.hasNext, s"expected at least one data file for $tableName")
      Paths.get(new URI(contentFileLocation(iterator.next().file())))
    } finally {
      tasks.close()
    }
  }

  private def callOutcome(sqlText: String): Either[(String, String), Seq[Row]] =
    try {
      Right(spark.sql(sqlText).collect().toSeq)
    } catch {
      case e: Exception =>
        Left(e.getClass.getName -> Option(e.getMessage).getOrElse(""))
    }

  test("native remove_orphan_files dry-run and delete preserve reachable files") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouse =>
      withCatalog(warehouse) { catalog =>
        spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.db")
        spark.sql(s"CREATE TABLE $catalog.db.t (id INT, value STRING) USING iceberg")
        spark.sql(s"INSERT INTO $catalog.db.t VALUES (1, 'a'), (2, 'b')")

        val table = loadIcebergTable(s"$catalog.db.t")
        val tasks = table.newScan().planFiles()
        try {
          tasks.iterator().asScala.foreach { task =>
            makeOld(Paths.get(new URI(contentFileLocation(task.file()))))
          }
        } finally {
          tasks.close()
        }

        val orphan = Paths.get(new URI(table.location())).resolve("data/native-orphan.bin")
        Files.createDirectories(orphan.getParent)
        Files.write(orphan, "orphan".getBytes(UTF_8))
        makeOld(orphan)

        val dryRun = spark
          .sql(s"""CALL $catalog.system.remove_orphan_files(
               |  table => 'db.t',
               |  dry_run => true)""".stripMargin)
          .collect()

        assert(dryRun.exists(row => sameLocalPath(row.getString(0), orphan)))
        assert(Files.exists(orphan), "dry_run must not delete the orphan")

        val deleted = spark
          .sql(s"CALL $catalog.system.remove_orphan_files(table => 'db.t')")
          .collect()

        assert(deleted.exists(row => sameLocalPath(row.getString(0), orphan)))
        assert(!Files.exists(orphan), "native remove_orphan_files should delete the orphan")
        assert(
          spark.sql(s"SELECT * FROM $catalog.db.t ORDER BY id").collect().toSeq ==
            Seq(Row(1, "a"), Row(2, "b")))

        spark.sql(s"DROP TABLE $catalog.db.t")
      }
    }
  }

  test("native remove_orphan_files preserves files reachable only from an older valid snapshot") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouse =>
      withCatalog(warehouse) { catalog =>
        val tableName = s"$catalog.db.snapshot_history"
        spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.db")
        spark.sql(s"CREATE TABLE $tableName (id INT) USING iceberg")
        spark.sql(s"INSERT INTO $tableName VALUES (1)")
        val historicalDataFile = firstDataFile(tableName)

        spark.sql(s"INSERT OVERWRITE $tableName VALUES (2)")
        makeOld(historicalDataFile)

        val result = spark
          .sql(s"""CALL $catalog.system.remove_orphan_files(
               |  table => 'db.snapshot_history',
               |  dry_run => true)""".stripMargin)
          .collect()

        assert(
          !result.exists(row => sameLocalPath(row.getString(0), historicalDataFile)),
          s"historical reachable file was incorrectly classified as orphan: $historicalDataFile")
        assert(Files.exists(historicalDataFile), "a valid historical snapshot still references it")
        spark.sql(s"DROP TABLE $tableName")
      }
    }
  }

  test("native remove_orphan_files does not delete files newer than cutoff") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouse =>
      withCatalog(warehouse) { catalog =>
        spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.db")
        spark.sql(s"CREATE TABLE $catalog.db.t (id INT) USING iceberg")
        spark.sql(s"INSERT INTO $catalog.db.t VALUES (1)")

        val table = loadIcebergTable(s"$catalog.db.t")
        val recent = Paths.get(new URI(table.location())).resolve("data/recent-orphan.bin")
        Files.createDirectories(recent.getParent)
        Files.write(recent, Array[Byte](1, 2, 3))

        val result = spark
          .sql(s"CALL $catalog.system.remove_orphan_files(table => 'db.t')")
          .collect()

        assert(!result.exists(row => sameLocalPath(row.getString(0), recent)))
        assert(Files.exists(recent))
        spark.sql(s"DROP TABLE $catalog.db.t")
      }
    }
  }

  test("native remove_orphan_files matches the installed Iceberg procedure parameter surface") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouse =>
      withCatalog(warehouse) { catalog =>
        spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.db")
        spark.sql(s"CREATE TABLE $catalog.db.t (id INT) USING iceberg")

        if (icebergVersionAtLeast(1, 10)) {
          val prefixSql = s"""CALL $catalog.system.remove_orphan_files(
               |  table => 'db.t',
               |  dry_run => true,
               |  prefix_listing => true)""".stripMargin
          val intercepted = callOutcome(prefixSql)
          val canonical = withSQLConf(
            CometConf.COMET_ICEBERG_REMOVE_ORPHAN_FILES_NATIVE_ENABLED.key -> "false") {
            callOutcome(prefixSql)
          }
          assert(intercepted == canonical)
        } else {
          val error = intercept[IllegalArgumentException] {
            spark
              .sql(s"""CALL $catalog.system.remove_orphan_files(
                   |  table => 'db.t',
                   |  prefix_listing => true)""".stripMargin)
              .collect()
          }
          assert(error.getMessage.contains("prefix_listing"))
        }

        if (icebergVersionAtLeast(1, 11)) {
          spark
            .sql(s"""CALL $catalog.system.remove_orphan_files(
                 |  table => 'db.t',
                 |  stream_results => true)""".stripMargin)
            .collect()
        } else {
          val streamError = intercept[IllegalArgumentException] {
            spark
              .sql(s"""CALL $catalog.system.remove_orphan_files(
                   |  table => 'db.t',
                   |  stream_results => true)""".stripMargin)
              .collect()
          }
          assert(streamError.getMessage.contains("stream_results"))
        }

        spark.sql(s"DROP TABLE $catalog.db.t")
      }
    }
  }

  test("native remove_orphan_files falls back for JVM-specific file_list_view without mutating SQLConf") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouse =>
      withCatalog(warehouse) { catalog =>
        spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.db")
        spark.sql(s"CREATE TABLE $catalog.db.t (id INT) USING iceberg")

        val confKey = CometConf.COMET_ICEBERG_REMOVE_ORPHAN_FILES_NATIVE_ENABLED.key
        val before = spark.sessionState.conf.getConfString(confKey)
        val fileListError = intercept[Exception] {
          spark
            .sql(s"""CALL $catalog.system.remove_orphan_files(
                 |  table => 'db.t',
                 |  file_list_view => 'some_view')""".stripMargin)
            .collect()
        }
        assert(fileListError.getMessage != null && fileListError.getMessage.nonEmpty)
        assert(spark.sessionState.conf.getConfString(confKey) == before)

        spark.sql(s"DROP TABLE $catalog.db.t")
      }
    }
  }

  test("native remove_orphan_files validates concurrency and argument types without truncation") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouse =>
      withCatalog(warehouse) { catalog =>
        spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.db")
        spark.sql(s"CREATE TABLE $catalog.db.t (id INT) USING iceberg")

        val zero = intercept[IllegalArgumentException] {
          spark
            .sql(s"""CALL $catalog.system.remove_orphan_files(
                 |  table => 'db.t',
                 |  max_concurrent_deletes => 0)""".stripMargin)
            .collect()
        }
        assert(zero.getMessage.contains("max_concurrent_deletes"))

        val wrongIntegerType = intercept[IllegalArgumentException] {
          spark
            .sql(s"""CALL $catalog.system.remove_orphan_files(
                 |  table => 'db.t',
                 |  max_concurrent_deletes => CAST(2147483648 AS BIGINT))""".stripMargin)
            .collect()
        }
        assert(wrongIntegerType.getMessage.contains("max_concurrent_deletes"))

        val wrongMapType = intercept[IllegalArgumentException] {
          spark
            .sql(s"""CALL $catalog.system.remove_orphan_files(
                 |  table => 'db.t',
                 |  equal_schemes => map(1, 2))""".stripMargin)
            .collect()
        }
        assert(wrongMapType.getMessage.contains("equal_schemes"))

        spark
          .sql(s"""CALL $catalog.system.remove_orphan_files(
               |  table => 'db.t',
               |  dry_run => true,
               |  max_concurrent_deletes => 2147483647)""".stripMargin)
          .collect()

        spark.sql(s"DROP TABLE $catalog.db.t")
      }
    }
  }

  test("native remove_orphan_files accepts epoch and pre-epoch timestamp cutoffs") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouse =>
      withCatalog(warehouse) { catalog =>
        spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.db")
        spark.sql(s"CREATE TABLE $catalog.db.t (id INT) USING iceberg")

        spark
          .sql(s"""CALL $catalog.system.remove_orphan_files(
               |  table => 'db.t',
               |  older_than => TIMESTAMP '1970-01-01 00:00:00',
               |  dry_run => true)""".stripMargin)
          .collect()
        spark
          .sql(s"""CALL $catalog.system.remove_orphan_files(
               |  table => 'db.t',
               |  older_than => TIMESTAMP '1960-01-01 00:00:00',
               |  dry_run => true)""".stripMargin)
          .collect()

        spark.sql(s"DROP TABLE $catalog.db.t")
      }
    }
  }

  test("TIMESTAMP_NTZ older_than follows the canonical procedure binder") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouse =>
      withCatalog(warehouse) { catalog =>
        spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.db")
        spark.sql(s"CREATE TABLE $catalog.db.t (id INT) USING iceberg")

        withSQLConf("spark.sql.session.timeZone" -> "Asia/Kolkata") {
          val sqlText = s"""CALL $catalog.system.remove_orphan_files(
               |  table => 'db.t',
               |  older_than => CAST('1960-01-01 00:00:00' AS TIMESTAMP_NTZ),
               |  dry_run => true)""".stripMargin
          val intercepted = callOutcome(sqlText)
          val canonical = withSQLConf(
            CometConf.COMET_ICEBERG_REMOVE_ORPHAN_FILES_NATIVE_ENABLED.key -> "false") {
            callOutcome(sqlText)
          }
          assert(intercepted == canonical)
        }

        spark.sql(s"DROP TABLE $catalog.db.t")
      }
    }
  }

  test("native remove_orphan_files rejects null table") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouse =>
      withCatalog(warehouse) { catalog =>
        val error = intercept[Exception] {
          spark
            .sql(s"CALL $catalog.system.remove_orphan_files(table => CAST(NULL AS STRING))")
            .collect()
        }
        assert(error.getMessage != null && error.getMessage.nonEmpty)
      }
    }
  }

  test("native remove_orphan_files honors gc.enabled=false") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouse =>
      withCatalog(warehouse) { catalog =>
        spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.db")
        spark.sql(s"""CREATE TABLE $catalog.db.t (id INT) USING iceberg
             |TBLPROPERTIES ('gc.enabled' = 'false')""".stripMargin)

        val table = loadIcebergTable(s"$catalog.db.t")
        val orphan = Paths.get(new URI(table.location())).resolve("data/orphan.bin")
        Files.createDirectories(orphan.getParent)
        Files.write(orphan, Array[Byte](1))
        makeOld(orphan)

        val error = intercept[RuntimeException] {
          spark
            .sql(s"CALL $catalog.system.remove_orphan_files(table => 'db.t')")
            .collect()
        }
        assert(error.getMessage.contains("gc.enabled is false"))
        assert(Files.exists(orphan), "GC-disabled tables must never delete files")

        spark.sql(s"DROP TABLE $catalog.db.t")
      }
    }
  }

  test("native remove_orphan_files rejects short retention intervals") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouse =>
      withCatalog(warehouse) { catalog =>
        spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.db")
        spark.sql(s"CREATE TABLE $catalog.db.t (id INT) USING iceberg")
        spark.sql(s"INSERT INTO $catalog.db.t VALUES (1)")

        val error = intercept[Exception] {
          spark
            .sql(s"""CALL $catalog.system.remove_orphan_files(
                 |  table => 'db.t',
                 |  older_than => CURRENT_TIMESTAMP)""".stripMargin)
            .collect()
        }
        assert(error.getMessage.contains("24 hours"))

        spark.sql(s"DROP TABLE $catalog.db.t")
      }
    }
  }

  test("native fallback returns Iceberg-Java rows for stream_results") {
    assume(icebergAvailable, "Iceberg not available in classpath")

    withTempIcebergDir { warehouse =>
      withCatalog(warehouse) { catalog =>
        spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.db")
        spark.sql(s"CREATE TABLE $catalog.db.t (id INT) USING iceberg")
        spark.sql(s"INSERT INTO $catalog.db.t VALUES (1)")

        val table = loadIcebergTable(s"$catalog.db.t")
        val orphan = Paths.get(new URI(table.location())).resolve("data/fallback-orphan.bin")
        Files.createDirectories(orphan.getParent)
        Files.write(orphan, Array[Byte](1))
        makeOld(orphan)

        val rows = spark
          .sql(s"""CALL $catalog.system.remove_orphan_files(
               |  table => 'db.t',
               |  dry_run => true,
               |  stream_results => true)""".stripMargin)
          .collect()
        assert(rows.exists(row => sameLocalPath(row.getString(0), orphan)))
        assert(Files.exists(orphan), "dry_run must not delete the orphan")

        spark.sql(s"DROP TABLE $catalog.db.t")
      }
    }
  }

  test("maintenance fallback bypass is thread-local and nesting-safe") {
    implicit val executionContext: ExecutionContext = ExecutionContext.global
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)

    val worker = Future {
      CometIcebergMaintenanceParser.withNativeMaintenanceBypassed {
        assert(CometIcebergMaintenanceParser.nativeMaintenanceBypassed)
        CometIcebergMaintenanceParser.withNativeMaintenanceBypassed {
          assert(CometIcebergMaintenanceParser.nativeMaintenanceBypassed)
        }
        assert(CometIcebergMaintenanceParser.nativeMaintenanceBypassed)
        entered.countDown()
        assert(release.await(30, TimeUnit.SECONDS))
      }
      assert(!CometIcebergMaintenanceParser.nativeMaintenanceBypassed)
    }

    assert(entered.await(30, TimeUnit.SECONDS))
    assert(!CometIcebergMaintenanceParser.nativeMaintenanceBypassed)
    release.countDown()
    Await.result(worker, 30.seconds)
  }

  test("native maintenance FileIO admission is exact and scheme scoped") {
    val hadoopFileIO = "org.apache.iceberg.hadoop.HadoopFileIO"
    val s3FileIO = "org.apache.iceberg.aws.s3.S3FileIO"

    assert(CometIcebergMaintenance.isMaintenanceFileIOClass(hadoopFileIO, "file:///tmp/t"))
    assert(!CometIcebergMaintenance.isMaintenanceFileIOClass("example.CustomHadoopFileIO", "file:///tmp/t"))
    assert(!CometIcebergMaintenance.isMaintenanceFileIOClass("org.apache.iceberg.io.ResolvingFileIO", "file:///tmp/t"))
    assert(CometIcebergMaintenance.isMaintenanceFileIOClass(s3FileIO, "s3://bucket/t"))
    assert(!CometIcebergMaintenance.isMaintenanceFileIOClass(hadoopFileIO, "s3://bucket/t"))
    assert(!CometIcebergMaintenance.isMaintenanceFileIOClass(s3FileIO, "file:///tmp/t"))
  }

  test("driver plugin keeps the maintenance parser outermost without duplication") {
    val conf = new SparkConf(false)
    val extensionKey = "spark.sql.extensions"
    val cometExtension = classOf[CometSparkSessionExtensions].getName
    val maintenanceExtension = classOf[CometIcebergMaintenanceExtensions].getName
    conf.set(extensionKey, s"example.First,$maintenanceExtension,example.Last")

    CometDriverPlugin.registerCometSessionExtension(conf)
    assert(
      conf.get(extensionKey).split(',').toSeq ==
        Seq("example.First", "example.Last", cometExtension, maintenanceExtension))

    val once = conf.get(extensionKey)
    CometDriverPlugin.registerCometSessionExtension(conf)
    assert(conf.get(extensionKey) == once)
  }
}