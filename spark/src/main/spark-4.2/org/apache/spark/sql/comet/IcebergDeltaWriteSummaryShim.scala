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

package org.apache.spark.sql.comet

import org.apache.spark.sql.connector.write.{BatchWrite, DeleteSummaryImpl, MergeSummaryImpl, UpdateSummaryImpl, WriterCommitMessage}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.MergeRowsExec
import org.apache.spark.sql.execution.metric.SQLMetric

import org.apache.comet.iceberg.{DeltaCommand, DeltaDelete, DeltaMerge, DeltaUpdate, IcebergSemanticMetricsShim}

/** Command-aware Spark 4.2 summary and retry-aware metric handling. */
private[comet] object IcebergDeltaWriteSummaryShim extends AdaptiveSparkPlanHelper {
  def commit(
      batchWrite: BatchWrite,
      messages: Array[WriterCommitMessage],
      query: SparkPlan,
      command: Option[DeltaCommand]): Boolean = command match {
    case Some(DeltaUpdate) => deltaWriterMetrics(query).exists { metrics =>
      batchWrite.commit(
        messages,
        UpdateSummaryImpl(value(metrics, "numUpdatedRows"), value(metrics, "numCopiedRows")))
      true
    }
    case Some(DeltaDelete) => deltaWriterMetrics(query).exists { metrics =>
      batchWrite.commit(
        messages,
        DeleteSummaryImpl(value(metrics, "numDeletedRows"), value(metrics, "numCopiedRows")))
      true
    }
    case Some(DeltaMerge) =>
      val mergeMetrics = collectFirst(query) {
        case merge: MergeRowsExec => merge.metrics
        case merge: CometMergeRowsExec => merge.metrics
      }
      mergeMetrics match {
        case Some(metrics) =>
          batchWrite.commit(messages, mergeSummary(metrics))
          true
        case None =>
          deltaWriterMetrics(query).flatMap(_.get("numOutputRows")).exists { rows =>
            batchWrite.commit(messages, MergeSummaryImpl(0L, 0L, 0L, value(rows), 0L, 0L, 0L, 0L))
            true
          }
      }
    case _ => false
  }

  private def deltaWriterMetrics(query: SparkPlan): Option[Map[String, SQLMetric]] =
    collectFirst(query) {
      case writer: CometIcebergDeltaWriteExec => writer.metrics
      case writer: IcebergWriteExec => writer.metrics
      case writer: CometIcebergWriteExec => writer.metrics
    }

  private def value(metrics: Map[String, SQLMetric], name: String): Long =
    metrics.get(name).map(IcebergSemanticMetricsShim.value).getOrElse(-1L)

  private def value(metric: SQLMetric): Long = IcebergSemanticMetricsShim.value(metric)

  private def mergeSummary(metrics: Map[String, SQLMetric]): MergeSummaryImpl =
    MergeSummaryImpl(
      value(metrics, "numTargetRowsCopied"),
      value(metrics, "numTargetRowsDeleted"),
      value(metrics, "numTargetRowsUpdated"),
      value(metrics, "numTargetRowsInserted"),
      value(metrics, "numTargetRowsMatchedUpdated"),
      value(metrics, "numTargetRowsMatchedDeleted"),
      value(metrics, "numTargetRowsNotMatchedBySourceUpdated"),
      value(metrics, "numTargetRowsNotMatchedBySourceDeleted"))
}
