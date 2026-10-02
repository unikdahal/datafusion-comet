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

package org.apache.comet.exec

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{CometListenerBusUtils, SparkConf}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometMergeRowsExec
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryRowLevelOperationTableCatalog, InMemoryTable}
import org.apache.spark.sql.connector.write.MergeSummary
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.{InsertOnlyMergeExec, MergeRowsExec}
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.CometConf

/** Spark 4.2 coverage for the dedicated InsertOnlyMergeExec rewrite. */
class CometInsertOnlyMergeSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private val catalog = "insert_only_merge"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(s"spark.sql.catalog.$catalog", classOf[InMemoryRowLevelOperationTableCatalog].getName)
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.shuffle.partitions", "4")
  }

  private case class MergeResult(
      plans: Seq[SparkPlan],
      rows: Seq[String],
      summary: MergeSummary)

  private def resetTables(target: String, source: String, sourceRows: String): Unit = {
    sql(s"DROP TABLE IF EXISTS $catalog.default.$target")
    sql(s"DROP TABLE IF EXISTS $catalog.default.$source")
    sql(s"CREATE TABLE $catalog.default.$target (id INT, amount INT) USING parquet")
    sql(s"CREATE TABLE $catalog.default.$source (id INT, amount INT) USING parquet")
    sql(s"INSERT INTO $catalog.default.$target VALUES (1, 10), (2, 20)")
    sql(s"INSERT INTO $catalog.default.$source VALUES $sourceRows")
  }

  private def lastMergeSummary(table: String): MergeSummary = {
    val cat = spark.sessionState.catalogManager
      .catalog(catalog)
      .asInstanceOf[InMemoryRowLevelOperationTableCatalog]
    cat
      .loadTable(Identifier.of(Array("default"), table))
      .asInstanceOf[InMemoryTable]
      .commits
      .last
      .writeSummary
      .get
      .asInstanceOf[MergeSummary]
  }

  private def runMerge(
      target: String,
      mergeSql: String,
      cometEnabled: Boolean): MergeResult = {
    val captured = ArrayBuffer[QueryExecution]()
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
        captured += qe
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
        ()
    }

    spark.listenerManager.register(listener)
    try {
      withSQLConf(
        CometConf.COMET_ENABLED.key -> cometEnabled.toString,
        CometConf.COMET_EXEC_MERGE_ROWS_ENABLED.key -> "true") {
        sql(mergeSql)
      }
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    } finally {
      spark.listenerManager.unregister(listener)
    }

    val rows = sql(s"SELECT id, amount FROM $catalog.default.$target ORDER BY id, amount")
      .collect()
      .map(_.toString)
      .toSeq
    MergeResult(captured.map(_.executedPlan).toSeq, rows, lastMergeSummary(target))
  }

  private def hasInsertOnlyMerge(plans: Seq[SparkPlan]): Boolean =
    plans.exists(plan =>
      find(plan) { case _: InsertOnlyMergeExec => true; case _ => false }.nonEmpty)

  private def hasCometMergeRows(plans: Seq[SparkPlan]): Boolean =
    plans.exists(plan =>
      find(plan) { case _: CometMergeRowsExec => true; case _ => false }.nonEmpty)

  private def hasSparkMergeRows(plans: Seq[SparkPlan]): Boolean =
    plans.exists(plan =>
      find(plan) { case _: MergeRowsExec => true; case _ => false }.nonEmpty)

  private def assertInsertOnlySummary(summary: MergeSummary, inserted: Long): Unit = {
    assert(summary.numTargetRowsInserted() == inserted)
    assert(summary.numTargetRowsCopied() == 0)
    assert(summary.numTargetRowsUpdated() == 0)
    assert(summary.numTargetRowsDeleted() == 0)
    assert(summary.numTargetRowsMatchedUpdated() == 0)
    assert(summary.numTargetRowsMatchedDeleted() == 0)
    assert(summary.numTargetRowsNotMatchedBySourceUpdated() == 0)
    assert(summary.numTargetRowsNotMatchedBySourceDeleted() == 0)
  }

  test("multiple NOT MATCHED clauses use native MergeRows and preserve MergeSummary") {
    val target = "multi_target"
    val source = "multi_source"
    val sourceRows = "(2, 200), (3, 300), (3, 301), (4, 400)"
    val mergeSql =
      s"""MERGE INTO $catalog.default.$target t
         |USING $catalog.default.$source s
         |ON t.id = s.id
         |WHEN NOT MATCHED AND s.amount < 350 THEN
         |  INSERT (id, amount) VALUES (s.id, s.amount)
         |WHEN NOT MATCHED AND s.amount >= 350 THEN
         |  INSERT (id, amount) VALUES (s.id, s.amount)
         |""".stripMargin

    resetTables(target, source, sourceRows)
    val comet = runMerge(target, mergeSql, cometEnabled = true)

    assert(hasInsertOnlyMerge(comet.plans), "expected Spark 4.2 InsertOnlyMergeExec")
    assert(hasCometMergeRows(comet.plans), "insert-only MergeRows child did not execute natively")
    assert(!hasSparkMergeRows(comet.plans), "native insert-only path retained Spark MergeRowsExec")
    assertInsertOnlySummary(comet.summary, inserted = 3)

    resetTables(target, source, sourceRows)
    val sparkOnly = runMerge(target, mergeSql, cometEnabled = false)

    assert(comet.rows == sparkOnly.rows)
    assert(
      comet.rows == Seq("[1,10]", "[2,20]", "[3,300]", "[3,301]", "[4,400]"),
      s"unexpected insert-only MERGE result: ${comet.rows.mkString(", ")}")
    assertInsertOnlySummary(sparkOnly.summary, inserted = 3)
  }

  test("single NOT MATCHED clause keeps InsertOnlyMergeExec summary without MergeRows") {
    val target = "single_target"
    val source = "single_source"
    val sourceRows = "(2, 200), (3, 300)"
    val mergeSql =
      s"""MERGE INTO $catalog.default.$target t
         |USING $catalog.default.$source s
         |ON t.id = s.id
         |WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount)
         |""".stripMargin

    resetTables(target, source, sourceRows)
    val comet = runMerge(target, mergeSql, cometEnabled = true)

    assert(hasInsertOnlyMerge(comet.plans), "expected Spark 4.2 InsertOnlyMergeExec")
    assert(!hasCometMergeRows(comet.plans), "single-clause rewrite should not contain MergeRows")
    assert(!hasSparkMergeRows(comet.plans), "single-clause rewrite should not contain MergeRows")
    assert(comet.rows == Seq("[1,10]", "[2,20]", "[3,300]"))
    assertInsertOnlySummary(comet.summary, inserted = 1)

    resetTables(target, source, sourceRows)
    val sparkOnly = runMerge(target, mergeSql, cometEnabled = false)
    assert(comet.rows == sparkOnly.rows)
    assertInsertOnlySummary(sparkOnly.summary, inserted = 1)
  }
}
