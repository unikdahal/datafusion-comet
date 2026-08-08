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

import org.apache.spark.{CometListenerBusUtils, SparkConf}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTableCatalog
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.CometSparkSessionExtensions.isSpark35Plus

/**
 * `CometMergeRowsExec` converts Spark's `MergeRowsExec` and the `MergeRows` logical node that
 * feeds it, both defined in Spark core (`execution.datasources.v2` / `catalyst.plans.logical`),
 * not in any connector module. Spark plans a `MergeRowsExec` for MERGE INTO against any
 * `SupportsRowLevelOperations` V2 table using group-based (copy-on-write) planning, independent
 * of which connector implements the table.
 *
 * This suite pins that contract against Spark's own `InMemoryRowLevelOperationTableCatalog` test
 * catalog rather than Iceberg. `InMemoryRowLevelOperationTable` selects its write shape via the
 * `supports-deltas` table property: unset (default `false`) plans through `MergeRows`; `true`
 * plans through `WriteDelta` instead, a distinct row-dispatch shape this operator does not
 * target.
 */
class CometMergeRowsGenericSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private val catalog = "generic_rowlevel"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(s"spark.sql.catalog.$catalog", classOf[InMemoryRowLevelOperationTableCatalog].getName)
      .set("spark.sql.shuffle.partitions", "4")
  }

  private def assumeMerge(): Unit = assume(isSpark35Plus, "MergeRowsExec requires Spark 3.5+")

  test("MERGE on a non-Iceberg SupportsRowLevelOperations table engages CometMergeRowsExec") {
    assumeMerge()
    val target = s"$catalog.default.rowlevel_target"
    val source = s"$catalog.default.rowlevel_source"

    def resetTables(): Unit = {
      sql(s"DROP TABLE IF EXISTS $target")
      sql(s"DROP TABLE IF EXISTS $source")
      sql(s"CREATE TABLE $target (id INT, region STRING, amount DOUBLE) USING parquet")
      sql(s"CREATE TABLE $source (id INT, region STRING, amount DOUBLE) USING parquet")
      sql(
        s"INSERT INTO $target VALUES " +
          (0 until 20).map(i => s"($i, 'r${i % 3}', ${i * 1.5})").mkString(", "))
      sql(
        s"INSERT INTO $source VALUES " +
          (10 until 30).map(i => s"($i, 's${i % 3}', ${i * 2.0})").mkString(", "))
    }

    val mergeSql =
      s"""MERGE INTO $target t USING $source s ON t.id = s.id
         |WHEN MATCHED THEN UPDATE SET t.amount = s.amount, t.region = s.region
         |WHEN NOT MATCHED THEN INSERT (id, region, amount) VALUES (s.id, s.region, s.amount)
         |""".stripMargin

    val captured = scala.collection.mutable.ArrayBuffer[QueryExecution]()
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
        captured += qe
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
        ()
    }
    spark.listenerManager.register(listener)
    try {
      resetTables()
      captured.clear()
      withSQLConf(CometConf.COMET_ENABLED.key -> "true") {
        sql(mergeSql)
      }
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
      // Tree-string rendering uses Spark's stripped nodeName ("CometMergeRows"), not the Scala
      // class name ("CometMergeRowsExec").
      val engaged = captured.exists(_.executedPlan.toString.contains("CometMergeRows"))
      val cometResult =
        sql(s"SELECT id, region, amount FROM $target ORDER BY id").collect().map(_.toString)

      resetTables()
      withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
        sql(mergeSql)
      }
      val sparkResult =
        sql(s"SELECT id, region, amount FROM $target ORDER BY id").collect().map(_.toString)

      assert(
        engaged,
        "CometMergeRowsExec did not engage against a non-Iceberg SupportsRowLevelOperations " +
          "table")
      assert(
        cometResult.toSeq == sparkResult.toSeq,
        s"native MergeRows output diverged from Spark's for a non-Iceberg source.\n" +
          s"comet: ${cometResult.mkString(", ")}\nspark: ${sparkResult.mkString(", ")}")
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }
}
