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
import org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTableCatalog
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.MergeRowsExec
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.comet.CometConf

/** Spark 4.1+ compatibility and semantic-metric coverage for native MergeRowsExec. */
class CometMergeRowsSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private val catalog = "generic_rowlevel"
  private val metricNames = Seq(
    "numTargetRowsCopied",
    "numTargetRowsDeleted",
    "numTargetRowsUpdated",
    "numTargetRowsInserted",
    "numTargetRowsMatchedUpdated",
    "numTargetRowsMatchedDeleted",
    "numTargetRowsNotMatchedBySourceUpdated",
    "numTargetRowsNotMatchedBySourceDeleted")

  private case class MergeCase(
      name: String,
      targetValues: String,
      sourceValues: String,
      clause: String,
      exercisedMetric: String)

  private val cases = Seq(
    MergeCase(
      "matched update",
      "(1, 10.0), (2, 20.0)",
      "(2, 200.0)",
      "WHEN MATCHED THEN UPDATE SET t.amount = s.amount",
      "numTargetRowsMatchedUpdated"),
    MergeCase(
      "matched delete",
      "(1, 10.0), (2, 20.0)",
      "(2, 200.0)",
      "WHEN MATCHED THEN DELETE",
      "numTargetRowsMatchedDeleted"),
    MergeCase(
      "not matched insert",
      "(1, 10.0)",
      "(2, 20.0)",
      "WHEN NOT MATCHED THEN INSERT (id, amount) VALUES (s.id, s.amount)",
      "numTargetRowsInserted"),
    MergeCase(
      "not matched by source update",
      "(1, 10.0), (2, 20.0)",
      "(2, 200.0)",
      "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.amount = t.amount + 100.0",
      "numTargetRowsNotMatchedBySourceUpdated"),
    MergeCase(
      "not matched by source delete",
      "(1, 10.0), (2, 20.0)",
      "(2, 200.0)",
      "WHEN NOT MATCHED BY SOURCE THEN DELETE",
      "numTargetRowsNotMatchedBySourceDeleted"))

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(s"spark.sql.catalog.$catalog", classOf[InMemoryRowLevelOperationTableCatalog].getName)
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.shuffle.partitions", "4")
  }

  private def captureMergeNode(action: => Unit): SparkPlan = {
    val captured = ArrayBuffer[QueryExecution]()
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
        captured += qe
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
        captured += qe
    }

    spark.listenerManager.register(listener)
    try {
      action
      CometListenerBusUtils.waitUntilEmpty(spark.sparkContext)
    } finally {
      spark.listenerManager.unregister(listener)
    }

    captured.reverseIterator
      .flatMap { qe =>
        find(qe.executedPlan) {
          case _: MergeRowsExec => true
          case _: CometMergeRowsExec => true
          case _ => false
        }
      }
      .toSeq
      .headOption
      .getOrElse(fail(
        s"MERGE plan not captured. Plans:\n${captured.map(_.executedPlan).mkString("\n--\n")}"))
  }

  private def metricValues(node: SparkPlan): Map[String, Long] = {
    val metrics = node match {
      case m: MergeRowsExec => m.metrics
      case m: CometMergeRowsExec => m.metrics
      case other => fail(s"unexpected MERGE node: ${other.getClass.getName}")
    }
    metricNames.map(name => name -> metrics(name).value).toMap
  }

  cases.foreach { mergeCase =>
    test(s"native MergeRows matches Spark for ${mergeCase.name}") {
      val suffix = mergeCase.name.replace(' ', '_')
      val sparkTarget = s"$catalog.default.spark_$suffix"
      val cometTarget = s"$catalog.default.comet_$suffix"
      val source = s"$catalog.default.source_$suffix"
      val tables = Seq(sparkTarget, cometTarget, source)

      tables.foreach(table => sql(s"DROP TABLE IF EXISTS $table"))
      try {
        sql(s"CREATE TABLE $sparkTarget (id INT, amount DOUBLE) USING parquet")
        sql(s"CREATE TABLE $cometTarget (id INT, amount DOUBLE) USING parquet")
        sql(s"CREATE TABLE $source (id INT, amount DOUBLE) USING parquet")
        sql(s"INSERT INTO $sparkTarget VALUES ${mergeCase.targetValues}")
        sql(s"INSERT INTO $cometTarget VALUES ${mergeCase.targetValues}")
        sql(s"INSERT INTO $source VALUES ${mergeCase.sourceValues}")

        def merge(target: String): Unit = {
          sql(s"""MERGE INTO $target t USING $source s ON t.id = s.id
                 |${mergeCase.clause}
                 |""".stripMargin)
        }

        val sparkNode = captureMergeNode {
          withSQLConf(CometConf.COMET_ENABLED.key -> "false") {
            merge(sparkTarget)
          }
        }
        assert(sparkNode.isInstanceOf[MergeRowsExec])

        val cometNode = captureMergeNode {
          withSQLConf(
            CometConf.COMET_ENABLED.key -> "true",
            CometConf.COMET_EXEC_MERGE_ROWS_ENABLED.key -> "true") {
            merge(cometTarget)
          }
        }
        assert(cometNode.isInstanceOf[CometMergeRowsExec])

        val sparkMetrics = metricValues(sparkNode)
        val cometMetrics = metricValues(cometNode)
        assert(
          cometMetrics == sparkMetrics,
          s"${mergeCase.name} metric mismatch: Comet=$cometMetrics Spark=$sparkMetrics")
        assert(
          cometMetrics(mergeCase.exercisedMetric) > 0L,
          s"${mergeCase.exercisedMetric} was not exercised: $cometMetrics")

        val sparkRows =
          sql(s"SELECT id, amount FROM $sparkTarget ORDER BY id").collect().toSeq
        val cometRows =
          sql(s"SELECT id, amount FROM $cometTarget ORDER BY id").collect().toSeq
        assert(cometRows == sparkRows, s"${mergeCase.name} result mismatch")
      } finally {
        tables.foreach(table => sql(s"DROP TABLE IF EXISTS $table"))
      }
    }
  }
}
