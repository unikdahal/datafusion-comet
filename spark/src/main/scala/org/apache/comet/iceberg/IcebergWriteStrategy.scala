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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic, ReplaceData}
import org.apache.spark.sql.comet.{IcebergCommitExec, IcebergWriteExec}
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isCometLoaded

/**
 * Spark strategy for Comet's split Iceberg V2 writer. WriteDelta is modeled and dispatched here,
 * but position-delta rows are still executed by Iceberg's JVM DeltaWriter.
 */
case class IcebergWriteStrategy(session: SparkSession) extends SparkStrategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val conf = session.sessionState.conf
    if (!isCometLoaded(conf) || !CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.get(conf)) {
      return Nil
    }
    if (CometConf.COMET_EXPLAIN_PLAN_ONLY_ENABLED.get(conf)) {
      return Nil
    }

    plan match {
      case ad: AppendData =>
        matchedSparkWrite(ad.table, ad.write, ad.query, PlainIcebergWrite).toList
      case obe: OverwriteByExpression =>
        matchedSparkWrite(obe.table, obe.write, obe.query, PlainIcebergWrite).toList
      case opd: OverwritePartitionsDynamic =>
        matchedSparkWrite(opd.table, opd.write, opd.query, PlainIcebergWrite).toList
      case rd: ReplaceData =>
        matchedSparkWrite(
          rd.originalTable,
          rd.write,
          rd.query,
          IcebergReplaceDataShim
            .extractProjections(rd)
            .map(ReplaceDataWrite)
            .getOrElse(PlainIcebergWrite)).toList
      case replace if IcebergReflection.isReplaceIcebergData(replace) =>
        IcebergReflection
          .extractReplaceIcebergDataFields(replace)
          .flatMap { case (_, query, originalTable, write) =>
            matchedSparkWrite(
              originalTable.asInstanceOf[org.apache.spark.sql.catalyst.analysis.NamedRelation],
              write.asInstanceOf[Option[Write]],
              query.asInstanceOf[LogicalPlan],
              PlainIcebergWrite)
          }
          .toList
      case l @ IcebergWriteLogical(child, batchWrite, dispatch) =>
        Seq(IcebergWriteExec(batchWrite, l.output, planLater(child), dispatch))
      case delta =>
        IcebergDeltaLogicalShim.extract(delta)
          .flatMap { fields =>
            fields.command.flatMap { command =>
              fields.write.flatMap { deltaWrite =>
                if (!IcebergReflection.isIcebergPositionDeltaWrite(deltaWrite)) {
                  None
                } else {
                  WriteDeltaDispatchInfo
                    .build(
                      fields.projections,
                      fields.query.output,
                      IcebergDeltaWriterShim.OperationCodes,
                      Some(command))
                    .flatMap { info =>
                      buildDeltaTwoOp(
                        deltaWrite,
                        fields.originalTable,
                        fields.query,
                        PositionDeltaWrite(info),
                        Some(command),
                        fields.tableName)
                    }
                }
              }
            }
          }
          .toList
    }
  }

  private def matchedSparkWrite(
      table: org.apache.spark.sql.catalyst.analysis.NamedRelation,
      write: Option[Write],
      query: LogicalPlan,
      dispatch: IcebergWriteDispatch): Option[SparkPlan] = {
    table match {
      case rel: DataSourceV2Relation =>
        write.flatMap { w =>
          if (IcebergReflection.isIcebergSparkWrite(w)) {
            buildTwoOp(w, rel, query, dispatch)
          } else {
            None
          }
        }
      case _ => None
    }
  }

  private def buildTwoOp(
      write: Write,
      rel: DataSourceV2Relation,
      query: LogicalPlan,
      dispatch: IcebergWriteDispatch): Option[SparkPlan] = {
    val batchWrite = write.toBatch
    if (batchWrite.useCommitCoordinator()) {
      return None
    }
    val refresh: () => Unit = () => IcebergRefreshCacheShim.refreshCache(session, rel)
    Some(
      IcebergCommitPlanShim.wrap(
        IcebergCommitExec(
          batchWrite,
          write,
          refresh,
          planLater(IcebergWriteLogical(query, batchWrite, dispatch)))))
  }

  private def buildDeltaTwoOp(
      write: Write,
      table: org.apache.spark.sql.catalyst.analysis.NamedRelation,
      query: LogicalPlan,
      dispatch: IcebergWriteDispatch,
      command: Option[DeltaCommand],
      tableName: Option[String]): Option[SparkPlan] = {
    table match {
      case rel: DataSourceV2Relation =>
        try {
          val batchWrite = write.toBatch
          if (!IcebergReflection.isIcebergPositionDeltaBatchWrite(batchWrite) ||
            batchWrite.useCommitCoordinator()) {
            None
          } else {
            val refresh: () => Unit = () => IcebergRefreshCacheShim.refreshCache(session, rel)
            val commit = IcebergCommitExec(
              batchWrite,
              write,
              refresh,
              planLater(IcebergWriteLogical(query, batchWrite, dispatch)),
              command,
              tableName.orElse(Some(rel.name)))
            Some(IcebergCommitPlanShim.wrap(commit))
          }
        } catch {
          case scala.util.control.NonFatal(_) => None
        }
      case _ => None
    }
  }
}
