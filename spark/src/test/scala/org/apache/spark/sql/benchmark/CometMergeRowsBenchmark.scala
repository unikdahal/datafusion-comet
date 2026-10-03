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

package org.apache.spark.sql.benchmark

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.collection.mutable

import org.apache.spark.{CometListenerBusUtils, SparkConf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTableCatalog
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

/**
 * Spark SQL benchmark for MergeRows.
 *
 * The three execution arms share one SparkSession and their order rotates each measured round:
 * Spark only, Comet with JVM MergeRows, and Comet with native MergeRows. The target is rebuilt
 * outside the timer before every run. Plans and output are verified before timing.
 *
 * The in-memory row-level catalog intentionally removes storage IO from this layer. The native
 * Criterion benchmark remains the lower-level base-vs-head attribution benchmark.
 */
object CometMergeRowsBenchmark extends CometBenchmarkBase {
  private def catalog: String = "benchmark_rowlevel"
  private val namespace = "default"

  private case class Arm(
      id: String,
      label: String,
      confs: Seq[(String, String)],
      expectAnyComet: Option[Boolean],
      expectNativeMergeRows: Boolean)

  private case class Workload(name: String, clauses: Int, payloadColumns: Int)
  private case class WorkloadResult(workload: Workload, measurementsMs: Map[String, Seq[Double]])

  private val arms = Seq(
    Arm(
      "spark",
      "Spark only",
      Seq(
        CometConf.COMET_ENABLED.key -> "false",
        CometConf.COMET_EXEC_ENABLED.key -> "false",
        CometConf.COMET_EXEC_MERGE_ROWS_ENABLED.key -> "false"),
      expectAnyComet = Some(false),
      expectNativeMergeRows = false),
    Arm(
      "comet_jvm",
      "Comet + JVM MergeRows",
      Seq(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_EXEC_MERGE_ROWS_ENABLED.key -> "false"),
      expectAnyComet = None,
      expectNativeMergeRows = false),
    Arm(
      "comet_native",
      "Comet + native MergeRows",
      Seq(
        CometConf.COMET_ENABLED.key -> "true",
        CometConf.COMET_EXEC_ENABLED.key -> "true",
        CometConf.COMET_EXEC_MERGE_ROWS_ENABLED.key -> "true"),
      expectAnyComet = Some(true),
      expectNativeMergeRows = true))

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometMergeRowsBenchmark")
      .set("spark.master", "local[4]")
      .setIfMissing("spark.driver.memory", "6g")
      .setIfMissing("spark.executor.memory", "6g")
      .set("spark.sql.catalog." + catalog, classOf[InMemoryRowLevelOperationTableCatalog].getName)
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.shuffle.partitions", "8")
      .set("spark.sql.adaptive.enabled", "false")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
      .set("spark.comet.exec.onHeap.enabled", "true")

    val session = SparkSession
      .builder()
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()

    session.conf.set(CometConf.COMET_ENABLED.key, "false")
    session.conf.set(CometConf.COMET_EXEC_ENABLED.key, "false")
    session.conf.set(CometConf.COMET_EXEC_MERGE_ROWS_ENABLED.key, "false")
    session
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    val rows = envInt("COMET_MERGEROWS_ROWS", 131072)
    val rounds = envInt("COMET_MERGEROWS_ROUNDS", 7)
    val warmups = envInt("COMET_MERGEROWS_WARMUPS", 2)

    require(rows >= 1024, "COMET_MERGEROWS_ROWS must be >= 1024, got " + rows)
    require(rounds >= 3, "COMET_MERGEROWS_ROUNDS must be >= 3, got " + rounds)
    require(warmups >= 1, "COMET_MERGEROWS_WARMUPS must be >= 1, got " + warmups)

    val workloads = Seq(
      Workload("clauses-2-width-12", 2, 12),
      Workload("clauses-4-width-12", 4, 12),
      Workload("clauses-8-width-12", 8, 12),
      Workload("clauses-16-width-12", 16, 12),
      Workload("clauses-8-width-4", 8, 4),
      Workload("clauses-8-width-32", 8, 32))

    // scalastyle:off println
    println(
      "MergeRows Spark benchmark: rows=" + rows +
        ", measuredRounds=" + rounds +
        ", warmups=" + warmups +
        ", workloads=" + workloads.size)
    // scalastyle:on println

    val results = workloads.map(runWorkload(_, rows, rounds, warmups))
    val json = renderJson(rows, rounds, warmups, results)
    val outputPath =
      sys.env.getOrElse("COMET_MERGEROWS_BENCH_JSON", "/tmp/comet-mergerows-spark-benchmark.json")
    Files.write(Paths.get(outputPath), json.getBytes(StandardCharsets.UTF_8))

    // scalastyle:off println
    println("MERGEROWS_BENCHMARK_JSON=" + outputPath)
    println(json)
    // scalastyle:on println
  }

  private def runWorkload(
      workload: Workload,
      rows: Int,
      rounds: Int,
      warmups: Int): WorkloadResult = {
    val prefix = workload.name.replace('-', '_')
    val target = catalog + "." + namespace + "." + prefix + "_target"
    val seed = catalog + "." + namespace + "." + prefix + "_seed"
    val source = catalog + "." + namespace + "." + prefix + "_source"
    val schema = tableSchema(workload.payloadColumns)
    val merge = mergeSql(target, source, workload.clauses, workload.payloadColumns)

    def dropTables(): Unit =
      Seq(target, seed, source)
        .foreach(name => spark.sql("DROP TABLE IF EXISTS " + name).collect())

    def createTable(name: String): Unit =
      spark.sql("CREATE TABLE " + name + " (" + schema + ") USING parquet").collect()

    def populate(name: String, multiplier: Long): Unit = {
      val payload = (0 until workload.payloadColumns)
        .map(i => "(id * " + multiplier + " + " + i + ") AS p" + i)
        .mkString(", ")
      val insert =
        "INSERT INTO " + name +
          " SELECT id, CAST(PMOD(id, 100) AS INT) AS bucket, " + payload +
          " FROM range(" + rows + ")"
      spark.sql(insert).collect()
    }

    def resetTarget(): Unit = {
      spark.sql("DROP TABLE IF EXISTS " + target).collect()
      createTable(target)
      spark.sql("INSERT INTO " + target + " SELECT * FROM " + seed).collect()
    }

    try {
      dropTables()
      createTable(seed)
      createTable(source)
      populate(seed, 11L)
      populate(source, 17L)
      resetTarget()

      arms.foreach(arm => verifyArm(arm, merge, target, rows, resetTarget _))

      arms.foreach { arm =>
        var i = 0
        while (i < warmups) {
          resetTarget()
          executeMerge(arm, merge)
          i += 1
        }
      }

      val measured = arms.map(a => a.id -> mutable.ArrayBuffer.empty[Double]).toMap
      val rotations = Seq(
        Seq("spark", "comet_jvm", "comet_native"),
        Seq("comet_native", "spark", "comet_jvm"),
        Seq("comet_jvm", "comet_native", "spark"))
      val byId = arms.map(a => a.id -> a).toMap

      var round = 0
      while (round < rounds) {
        rotations(round % rotations.size).foreach { armId =>
          val arm = byId(armId)
          resetTarget()
          val start = System.nanoTime()
          executeMerge(arm, merge)
          val elapsedMs = (System.nanoTime() - start) / 1000000.0
          measured(arm.id) += elapsedMs
          // scalastyle:off println
          println(
            workload.name + " round=" + (round + 1) + " " + arm.label +
              " " + f"$elapsedMs%.3f" + " ms")
        // scalastyle:on println
        }
        round += 1
      }

      WorkloadResult(workload, measured.map { case (key, value) => key -> value.toSeq })
    } finally {
      dropTables()
    }
  }

  private def executeMerge(arm: Arm, merge: String): Unit =
    withSQLConf(arm.confs: _*) {
      spark.sql(merge).collect()
    }

  private def verifyArm(
      arm: Arm,
      merge: String,
      target: String,
      rows: Int,
      resetTarget: () => Unit): Unit = {
    resetTarget()
    val captured = mutable.ArrayBuffer.empty[QueryExecution]
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
        captured += qe
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
        ()
    }

    spark.listenerManager.register(listener)
    try {
      executeMerge(arm, merge)
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    } finally {
      spark.listenerManager.unregister(listener)
    }

    val mergedPlan = captured.map(_.executedPlan.toString).mkString("\n-- query boundary --\n")
    val hasAnyComet = mergedPlan.contains("Comet")
    val hasNativeMergeRows = mergedPlan.contains("CometMergeRows")
    val hasMergeRows = mergedPlan.contains("MergeRows")

    arm.expectAnyComet.foreach { expected =>
      if (hasAnyComet != expected) {
        throw new IllegalStateException(
          arm.label + ": expected Comet presence=" + expected +
            ", got " + hasAnyComet + ".\n" + mergedPlan)
      }
    }
    if (hasNativeMergeRows != arm.expectNativeMergeRows) {
      throw new IllegalStateException(
        arm.label + ": expected native MergeRows=" + arm.expectNativeMergeRows +
          ", got " + hasNativeMergeRows + ".\n" + mergedPlan)
    }
    if (!hasMergeRows) {
      throw new IllegalStateException(
        arm.label + ": no MergeRows operator in executed plan.\n" + mergedPlan)
    }

    val aggregate = spark.sql("SELECT count(*) AS c, sum(p0) AS s FROM " + target).head()
    val actualRows = aggregate.getLong(0)
    val actualSum = aggregate.getLong(1)
    val expectedSum = 17L * rows.toLong * (rows.toLong - 1L) / 2L
    if (actualRows != rows.toLong || actualSum != expectedSum) {
      throw new IllegalStateException(
        arm.label + ": incorrect MERGE result: rows=" + actualRows + "/" + rows +
          ", sum(p0)=" + actualSum + "/" + expectedSum)
    }

    // scalastyle:off println
    println(
      "Verified " + arm.label + ": anyComet=" + hasAnyComet +
        ", nativeMergeRows=" + hasNativeMergeRows +
        ", rows=" + actualRows + ", sum(p0)=" + actualSum)
    // scalastyle:on println
  }

  private def tableSchema(payloadColumns: Int): String =
    (Seq("id BIGINT", "bucket INT") ++
      (0 until payloadColumns).map(i => "p" + i + " BIGINT")).mkString(", ")

  private def mergeSql(
      target: String,
      source: String,
      clauses: Int,
      payloadColumns: Int): String = {
    require(clauses >= 2)
    val assignments = (0 until payloadColumns).map(i => "t.p" + i + " = s.p" + i).mkString(", ")
    val conditional = (1 until clauses).map { index =>
      val threshold = index * 100 / clauses
      "WHEN MATCHED AND s.bucket < " + threshold + " THEN UPDATE SET " + assignments
    }
    (Seq("MERGE INTO " + target + " t USING " + source + " s ON t.id = s.id") ++
      conditional ++
      Seq("WHEN MATCHED THEN UPDATE SET " + assignments)).mkString("\n")
  }

  private def envInt(name: String, default: Int): Int =
    sys.env.get(name).map(_.toInt).getOrElse(default)

  private def quote(value: String): String =
    "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

  private def renderJson(
      rows: Int,
      rounds: Int,
      warmups: Int,
      results: Seq[WorkloadResult]): String = {
    val armsJson = arms
      .map(a => "{\"id\":" + quote(a.id) + ",\"label\":" + quote(a.label) + "}")
      .mkString("[", ",", "]")
    val workloadsJson = results
      .map { result =>
        val measurements = arms
          .map { arm =>
            val values =
              result.measurementsMs(arm.id).map(v => f"$v%.6f").mkString("[", ",", "]")
            quote(arm.id) + ":" + values
          }
          .mkString("{", ",", "}")
        "{\"name\":" + quote(result.workload.name) +
          ",\"clauses\":" + result.workload.clauses +
          ",\"payload_columns\":" + result.workload.payloadColumns +
          ",\"measurements_ms\":" + measurements + "}"
      }
      .mkString("[", ",", "]")

    "{\"rows\":" + rows +
      ",\"rounds\":" + rounds +
      ",\"warmups\":" + warmups +
      ",\"arms\":" + armsJson +
      ",\"workloads\":" + workloadsJson + "}"
  }
}
