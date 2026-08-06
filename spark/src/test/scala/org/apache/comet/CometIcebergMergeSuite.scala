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

import java.io.File

import scala.collection.mutable

import org.apache.spark.{CometListenerBusUtils, SparkConf}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometMergeRowsExec
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus

/**
 * Differential correctness coverage for the native copy-on-write MERGE path
 * ([[CometMergeRowsExec]]).
 *
 * Every case runs the same MERGE twice over identical tables -- once with Comet enabled and once
 * with Comet fully disabled -- and asserts the committed rows are identical. Spark's own result
 * is the oracle, so no expected-value tables are hand-maintained and a semantic divergence in the
 * native operator shows up as a row difference rather than as a test nobody updated.
 *
 * Each case additionally asserts that the native operator *actually engaged*. Without that a
 * silent fallback would make both arms run the same JVM code and every assertion would pass
 * vacuously.
 */
class CometIcebergMergeSuite
    extends CometTestBase
    with AdaptiveSparkPlanHelper
    with CometIcebergTestBase {

  private val catalog = "cat"
  private val ns = "db"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.key, "true")
      .set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .set("spark.sql.shuffle.partitions", "4")
      // Force a shuffle-based join for the target/source join. These cases are about the native
      // operator's semantics, so they pin one plan shape rather than letting AQE pick between a
      // broadcast and a sort-merge join; `broadcast join falls back cleanly` below covers the
      // other side deliberately.
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set(CometConf.COMET_EXPLAIN_FALLBACK_ENABLED.key, "true")
      .set(CometConf.COMET_LOG_FALLBACK_REASONS.key, "true")
  }

  // --- matched / not-matched basics --------------------------------------------------------

  test("MERGE upsert matches Spark") {
    withMergeTables("upsert", targetIds = 0 until 40, sourceIds = 20 until 60) { (t, s) =>
      s"""MERGE INTO $t t USING $s s ON t.id = s.id
         |WHEN MATCHED THEN UPDATE SET t.amount = s.amount, t.region = s.region
         |WHEN NOT MATCHED THEN INSERT (id, region, amount) VALUES (s.id, s.region, s.amount)
         |""".stripMargin
    }
  }

  /**
   * A matched-only MERGE plans as a LEFT OUTER join rather than the FULL OUTER join an upsert
   * produces, and that join is broadcastable -- so this case reaches `MergeRowsExec` through a
   * different plan shape than the cases above.
   *
   * It used to fall back: the Iceberg target scan carries the `_file` metadata column, and
   * `CometScanRule.hasMetadataCol` rejected any scan projecting a metadata column, leaving a
   * row-based `BatchScan` that `requiresNativeChildren` refused. Now that `_file` is exempted
   * from that gate and `SparkCopyOnWriteScan` is on `IcebergReflection.ICEBERG_SCAN_CLASSES`, the
   * target scan is a `CometIcebergNativeScan` and the whole MERGE stays native.
   */
  test("MERGE with matched DELETE matches Spark") {
    withMergeTables("matched_delete", targetIds = 0 until 40, sourceIds = 10 until 30) { (t, s) =>
      s"""MERGE INTO $t t USING $s s ON t.id = s.id
         |WHEN MATCHED THEN DELETE
         |""".stripMargin
    }
  }

  /**
   * Every row of some input batches is discarded, which is exactly the path the empty-batch
   * suppression in `MergeRowsStream::poll_next` changed. `batchSize` is forced low so whole
   * batches -- not just some rows within a batch -- end up entirely discarded.
   */
  test("MERGE deleting whole batches matches Spark") {
    withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "16") {
      // Matched-only, so this shares the LEFT OUTER plan shape described above.
      withMergeTables("delete_all", targetIds = 0 until 200, sourceIds = 0 until 200) { (t, s) =>
        s"""MERGE INTO $t t USING $s s ON t.id = s.id
           |WHEN MATCHED THEN DELETE
           |""".stripMargin
      }
    }
  }

  /**
   * The all-discarded-batch path *is* reachable natively via a NOT MATCHED BY SOURCE DELETE:
   * adding a NOT MATCHED clause forces the FULL OUTER join that keeps the plan on Comet, while
   * the DELETE clause still produces input batches whose rows are entirely discarded -- which is
   * what the empty-batch suppression in `MergeRowsStream::poll_next` changed. A low `batchSize`
   * makes whole batches, not just some rows, disappear.
   */
  test("MERGE discarding entire batches natively matches Spark") {
    withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "16") {
      withMergeTables("discard_native", targetIds = 0 until 200, sourceIds = 150 until 350) {
        (t, s) =>
          s"""MERGE INTO $t t USING $s s ON t.id = s.id
             |WHEN MATCHED THEN DELETE
             |WHEN NOT MATCHED THEN INSERT (id, region, amount) VALUES (s.id, s.region, s.amount)
             |WHEN NOT MATCHED BY SOURCE THEN DELETE
             |""".stripMargin
      }
    }
  }

  // --- conditional clauses: the null_to_false and first-match-wins regressions ---------------

  /**
   * Regression coverage at batch scale for the NULL-condition data-loss bug. `amount` is NULL for
   * a third of the target rows, so `s.amount > 100` evaluates to NULL and Spark treats it as
   * false, falling through to the catch-all `Keep(TrueLiteral, target.output)` that
   * `RewriteMergeIntoTable` appends. Before `null_to_false`, Arrow's NULL-propagating mask
   * arithmetic dropped those rows from the rewritten data file entirely.
   */
  test("MERGE with NULL-valued clause condition matches Spark") {
    withMergeTables(
      "null_cond",
      targetIds = 0 until 60,
      sourceIds = 0 until 60,
      nullEvery = Some(3)) { (t, s) =>
      s"""MERGE INTO $t t USING $s s ON t.id = s.id
         |WHEN MATCHED AND s.amount > 100 THEN UPDATE SET t.amount = s.amount
         |WHEN NOT MATCHED AND s.amount > 100 THEN INSERT (id, region, amount)
         |  VALUES (s.id, s.region, s.amount)
         |""".stripMargin
    }
  }

  test("MERGE with multiple matched clauses picks the first match like Spark") {
    withMergeTables("first_match", targetIds = 0 until 60, sourceIds = 0 until 60) { (t, s) =>
      // The conditions overlap deliberately: id < 40 and id < 20 both hold for id < 20, so a
      // wrong evaluation order changes the committed amount.
      s"""MERGE INTO $t t USING $s s ON t.id = s.id
         |WHEN MATCHED AND s.id < 20 THEN UPDATE SET t.amount = -1.0
         |WHEN MATCHED AND s.id < 40 THEN UPDATE SET t.amount = -2.0
         |WHEN MATCHED THEN DELETE
         |WHEN NOT MATCHED THEN INSERT (id, region, amount) VALUES (s.id, s.region, s.amount)
         |""".stripMargin
    }
  }

  test("MERGE with a condition that divides by a source value matches Spark") {
    // Guards the group-scoping fix: a NOT MATCHED condition must never be evaluated against a
    // matched row. `s.amount` is 0 for some rows, so batch-wide evaluation under ANSI mode would
    // raise a divide-by-zero Spark never produces.
    withMergeTables(
      "group_scope",
      targetIds = 0 until 40,
      sourceIds = 0 until 40,
      zeroEvery = Some(4)) { (t, s) =>
      s"""MERGE INTO $t t USING $s s ON t.id = s.id
         |WHEN MATCHED THEN UPDATE SET t.amount = s.amount
         |WHEN NOT MATCHED AND 100 / s.amount > 1 THEN INSERT (id, region, amount)
         |  VALUES (s.id, s.region, s.amount)
         |""".stripMargin
    }
  }

  // --- NOT MATCHED BY SOURCE ----------------------------------------------------------------

  test("MERGE with NOT MATCHED BY SOURCE UPDATE matches Spark") {
    withMergeTables("nmbs_update", targetIds = 0 until 60, sourceIds = 30 until 90) { (t, s) =>
      s"""MERGE INTO $t t USING $s s ON t.id = s.id
         |WHEN MATCHED THEN UPDATE SET t.amount = s.amount
         |WHEN NOT MATCHED THEN INSERT (id, region, amount) VALUES (s.id, s.region, s.amount)
         |WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.amount = -99.0
         |""".stripMargin
    }
  }

  test("MERGE with NOT MATCHED BY SOURCE DELETE matches Spark") {
    withMergeTables("nmbs_delete", targetIds = 0 until 60, sourceIds = 30 until 90) { (t, s) =>
      s"""MERGE INTO $t t USING $s s ON t.id = s.id
         |WHEN MATCHED THEN UPDATE SET t.amount = s.amount
         |WHEN NOT MATCHED BY SOURCE AND t.id < 10 THEN DELETE
         |""".stripMargin
    }
  }

  // --- join-key and type edge cases ---------------------------------------------------------

  test("MERGE with NULL join keys matches Spark") {
    withMergeTables(
      "null_keys",
      targetIds = 0 until 40,
      sourceIds = 0 until 40,
      nullKeyEvery = Some(5)) { (t, s) =>
      s"""MERGE INTO $t t USING $s s ON t.id = s.id
         |WHEN MATCHED THEN UPDATE SET t.amount = s.amount
         |WHEN NOT MATCHED THEN INSERT (id, region, amount) VALUES (s.id, s.region, s.amount)
         |""".stripMargin
    }
  }

  test("MERGE spanning many batches matches Spark") {
    // Rows well beyond `batchSize` so the operator is exercised across many polls, and the
    // cross-batch cardinality state is carried the way a real MERGE carries it.
    withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "128") {
      withMergeTables("multibatch", targetIds = 0 until 5000, sourceIds = 2500 until 7500) {
        (t, s) =>
          s"""MERGE INTO $t t USING $s s ON t.id = s.id
             |WHEN MATCHED THEN UPDATE SET t.amount = s.amount, t.region = s.region
             |WHEN NOT MATCHED THEN INSERT (id, region, amount) VALUES (s.id, s.region, s.amount)
             |""".stripMargin
      }
    }
  }

  // --- partitioned targets ------------------------------------------------------------------

  test("MERGE into a partitioned table matches Spark") {
    withMergeTables(
      "partitioned",
      targetIds = 0 until 200,
      sourceIds = 100 until 300,
      partitionSpec = "PARTITIONED BY (region)") { (t, s) =>
      s"""MERGE INTO $t t USING $s s ON t.id = s.id
         |WHEN MATCHED THEN UPDATE SET t.amount = s.amount, t.region = s.region
         |WHEN NOT MATCHED THEN INSERT (id, region, amount) VALUES (s.id, s.region, s.amount)
         |""".stripMargin
    }
  }

  test("MERGE into a partitioned table with fanout writer matches Spark") {
    withSQLConf("spark.sql.iceberg.write.fanout.enabled" -> "true") {
      withMergeTables(
        "partitioned_fanout",
        targetIds = 0 until 200,
        sourceIds = 100 until 300,
        partitionSpec = "PARTITIONED BY (region)") { (t, s) =>
        s"""MERGE INTO $t t USING $s s ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET t.amount = s.amount, t.region = s.region
           |WHEN NOT MATCHED THEN INSERT (id, region, amount) VALUES (s.id, s.region, s.amount)
           |""".stripMargin
      }
    }
  }

  // --- fallback behaviour -------------------------------------------------------------------

  /**
   * Pins that the common upsert keeps its acceleration under default join settings, rather than
   * only under the forced sort-merge join the rest of the suite pins.
   */
  test("MERGE upsert still engages natively with broadcast joins enabled") {
    assumeMerge()
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "10485760") {
      withMergeTables(
        "broadcast_upsert",
        targetIds = 0 until 40,
        sourceIds = 20 until 60,
        expectNative = true) { (t, s) =>
        s"""MERGE INTO $t t USING $s s ON t.id = s.id
           |WHEN MATCHED THEN UPDATE SET t.amount = s.amount
           |WHEN NOT MATCHED THEN INSERT (id, region, amount) VALUES (s.id, s.region, s.amount)
           |""".stripMargin
      }
    }
  }

  // --- cardinality violation ----------------------------------------------------------------

  test("MERGE with duplicate source keys raises MERGE_CARDINALITY_VIOLATION") {
    assumeMerge()
    withIcebergCatalog { _ =>
      createTable("card", partitionSpec = "")
      createTable("card_src", partitionSpec = "")
      appendRows("card", idRange(0 until 20))
      // Two source rows per target id -- a cardinality violation by construction.
      appendRows("card_src", idRange(0 until 20).union(idRange(0 until 20)))

      val err = withNativeEnabled {
        intercept[Exception] {
          spark.sql(s"""MERGE INTO $catalog.$ns.card t USING $catalog.$ns.card_src s
                       |ON t.id = s.id
                       |WHEN MATCHED THEN UPDATE SET t.amount = s.amount
                       |""".stripMargin)
        }
      }
      assert(
        stackText(err).contains("MERGE_CARDINALITY_VIOLATION"),
        s"expected a cardinality violation, got: ${stackText(err)}")
    }
  }

  test("MERGE cardinality violation is detected across Arrow batches") {
    assumeMerge()
    // A tiny batch size makes the two colliding source rows for a given target id land in
    // different Arrow batches, which only trips if `seen` is carried for the whole partition
    // rather than reset per batch.
    withSQLConf(CometConf.COMET_BATCH_SIZE.key -> "8") {
      withIcebergCatalog { _ =>
        createTable("card_xb", partitionSpec = "")
        createTable("card_xb_src", partitionSpec = "")
        appendRows("card_xb", idRange(0 until 200))
        appendRows("card_xb_src", idRange(0 until 200).union(idRange(0 until 200)))

        val err = withNativeEnabled {
          intercept[Exception] {
            spark.sql(s"""MERGE INTO $catalog.$ns.card_xb t USING $catalog.$ns.card_xb_src s
                         |ON t.id = s.id
                         |WHEN MATCHED THEN UPDATE SET t.amount = s.amount
                         |""".stripMargin)
          }
        }
        assert(
          stackText(err).contains("MERGE_CARDINALITY_VIOLATION"),
          s"expected a cardinality violation, got: ${stackText(err)}")
      }
    }
  }

  // --- Helpers ------------------------------------------------------------------------------

  private def assumeMerge(): Unit = {
    assume(icebergAvailable, "Iceberg not available in classpath")
    assume(isSpark35Plus, "MergeRowsExec only exists in Spark 3.5+")
  }

  private def stackText(t: Throwable): String = {
    val sw = new java.io.StringWriter()
    t.printStackTrace(new java.io.PrintWriter(sw))
    sw.toString
  }

  /**
   * Runs `mergeSql` against two identically-populated tables -- one with Comet enabled, one with
   * Comet disabled -- and asserts the committed rows match. The Comet arm must show a
   * [[CometMergeRowsExec]] in its executed plan, otherwise both arms ran the same JVM code and
   * the comparison proves nothing.
   *
   * @param nullEvery
   *   make every n-th `amount` NULL, to exercise NULL clause conditions
   * @param zeroEvery
   *   make every n-th `amount` zero, to exercise divide-by-zero in clause conditions
   * @param nullKeyEvery
   *   make every n-th `id` NULL, to exercise NULL join keys
   */
  private def withMergeTables(
      name: String,
      targetIds: Range,
      sourceIds: Range,
      partitionSpec: String = "",
      nullEvery: Option[Int] = None,
      zeroEvery: Option[Int] = None,
      nullKeyEvery: Option[Int] = None,
      expectNative: Boolean = true)(mergeSql: (String, String) => String): Unit = {
    assumeMerge()
    withIcebergCatalog { _ =>
      val cometTable = s"${name}_comet"
      val sparkTable = s"${name}_spark"
      val sourceTable = s"${name}_src"

      Seq(cometTable, sparkTable).foreach(createTable(_, partitionSpec))
      createTable(sourceTable, partitionSpec = "")

      val targetData = idRange(targetIds, nullEvery, zeroEvery, nullKeyEvery)
      val sourceData = idRange(sourceIds, nullEvery, zeroEvery, nullKeyEvery)
      Seq(cometTable, sparkTable).foreach(appendRows(_, targetData))
      appendRows(sourceTable, sourceData)

      val plans = mutable.Buffer.empty[SparkPlan]
      val listener = new QueryExecutionListener {
        override def onSuccess(f: String, qe: QueryExecution, d: Long): Unit =
          plans += qe.executedPlan
        override def onFailure(f: String, qe: QueryExecution, e: Exception): Unit = ()
      }
      spark.listenerManager.register(listener)
      try {
        withNativeEnabled {
          spark.sql(mergeSql(s"$catalog.$ns.$cometTable", s"$catalog.$ns.$sourceTable"))
        }
        // `QueryExecutionListener` callbacks are delivered asynchronously through Spark's
        // listener bus, so unregistering as soon as `sql()` returns races the delivery and can
        // leave `plans` empty -- which would look exactly like a fallback and fail the
        // engagement assertion for the wrong reason. Drain the bus first, as
        // `CometIcebergWriteActionSuite.captureWrite` does.
        try CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
        catch { case _: java.util.concurrent.TimeoutException => () }
      } finally {
        spark.listenerManager.unregister(listener)
      }

      val merges =
        plans.flatMap(p => collectWithSubqueries(p) { case m: CometMergeRowsExec => m })
      if (expectNative) {
        if (merges.isEmpty) {
          // scalastyle:off println
          println(s"\n===== [$name] NATIVE DID NOT ENGAGE -- executed plans =====")
          plans.foreach(p => println(p.toString))
          println(s"===== end [$name] =====\n")
          // scalastyle:on println
        }
        assert(
          merges.nonEmpty,
          s"[$name] native MergeRows did not engage, so the differential comparison would be " +
            s"vacuous. Executed plans:\n${plans.mkString("\n--\n")}")
      } else {
        assert(
          merges.isEmpty,
          s"[$name] expected a JVM fallback but native MergeRows engaged. Executed plans:\n" +
            plans.mkString("\n--\n"))
      }

      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        spark.sql(mergeSql(s"$catalog.$ns.$sparkTable", s"$catalog.$ns.$sourceTable"))
      }

      val cometRows = readAll(cometTable)
      val sparkRows = readAll(sparkTable)
      assert(
        cometRows == sparkRows,
        s"[$name] native MERGE diverged from Spark.\n" +
          s"  comet (${cometRows.length} rows): ${cometRows.take(20).mkString(", ")}\n" +
          s"  spark (${sparkRows.length} rows): ${sparkRows.take(20).mkString(", ")}")
    }
  }

  private def readAll(table: String): Seq[Row] =
    spark
      .sql(s"SELECT id, region, amount FROM $catalog.$ns.$table ORDER BY id, region, amount")
      .collect()
      .toSeq

  private def idRange(
      ids: Range,
      nullEvery: Option[Int] = None,
      zeroEvery: Option[Int] = None,
      nullKeyEvery: Option[Int] = None): Seq[(Option[Long], String, Option[Double])] =
    ids.map { i =>
      val id = if (nullKeyEvery.exists(n => i % n == 0)) None else Some(i.toLong)
      val amount =
        if (nullEvery.exists(n => i % n == 0)) None
        else if (zeroEvery.exists(n => i % n == 0)) Some(0.0)
        else Some(i.toDouble * 1.5)
      (id, s"region-${i % 5}", amount)
    }

  private def appendRows(
      table: String,
      rows: Seq[(Option[Long], String, Option[Double])]): Unit = {
    val session = spark
    import session.implicits._
    val df: DataFrame = rows.toDF("id", "region", "amount")
    df.writeTo(s"$catalog.$ns.$table").append()
  }

  private def createTable(table: String, partitionSpec: String): Unit =
    spark.sql(s"""
      CREATE TABLE $catalog.$ns.$table (id BIGINT, region STRING, amount DOUBLE)
      USING iceberg
      $partitionSpec
      TBLPROPERTIES ('write.merge.mode'='copy-on-write',
                     'write.delete.mode'='copy-on-write',
                     'write.update.mode'='copy-on-write')
    """)

  private def withIcebergCatalog(f: File => Unit): Unit = withTempIcebergDir { warehouseDir =>
    withSQLConf(
      s"spark.sql.catalog.$catalog" -> "org.apache.iceberg.spark.SparkCatalog",
      s"spark.sql.catalog.$catalog.type" -> "hadoop",
      s"spark.sql.catalog.$catalog.warehouse" -> warehouseDir.getAbsolutePath,
      CometConf.COMET_ENABLED.key -> "true",
      CometConf.COMET_EXEC_ENABLED.key -> "true") {
      f(warehouseDir)
    }
  }

  /** Mirrors `CometIcebergWriteActionSuite.withNativeEnabled`. */
  private def withNativeEnabled[T](action: => T): T = {
    val session = spark
    session.sessionState.conf
      .setConfString(CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED.key, "true")
    session.sessionState.conf
      .setConfString(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key, "true")
    try action
    finally {
      session.sessionState.conf.unsetConf(CometConf.COMET_EXEC_LOCAL_TABLE_SCAN_ENABLED.key)
      session.sessionState.conf.unsetConf(CometConf.COMET_ICEBERG_NATIVE_WRITE_ENABLED.key)
    }
  }
}
