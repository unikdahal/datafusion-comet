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
import org.apache.spark.sql.comet.{
  IcebergCommitExec,
  IcebergDeltaWriterShim,
  IcebergWriteExec,
  PositionDeltaCreatedFiles
}
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.isCometLoaded

/**
 * Spark Strategy that intercepts Iceberg V2 copy-on-write logical writes and emits Comet's
 * two-operator physical tree.
 */
case class IcebergWriteStrategy(session: SparkSession) extends SparkStrategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val conf = session.sessionState.conf
    // Planner strategies run whether or not Comet is enabled, so check it here too: with Comet
    // off, Spark must plan its own V2 write operator.
    if (!isCometLoaded(conf) || !CometConf.COMET_ICEBERG_WRITE_SPLIT_OPERATOR_ENABLED.get(conf)) {
      return Nil
    }

    plan match {
      case plan if IcebergInsertOnlyMergeShim.extract(plan).isDefined =>
        IcebergInsertOnlyMergeShim.extract(plan)
          .flatMap(fields =>
            matchedSparkWrite(
              fields.table,
              fields.write,
              fields.query,
              PlainIcebergWrite,
              Some(DeltaMerge),
              Some(fields.tableName)))
          .toList
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
      case plan if IcebergReflection.isReplaceIcebergData(plan) =>
        IcebergReflection
          .extractReplaceIcebergDataFields(plan)
          .flatMap { case (_, query, originalTable, write) =>
            matchedSparkWrite(
              originalTable.asInstanceOf[org.apache.spark.sql.catalyst.analysis.NamedRelation],
              write.asInstanceOf[Option[Write]],
              query.asInstanceOf[LogicalPlan],
              PlainIcebergWrite)
          }
          .toList
      case deltaPlan if IcebergDeltaLogicalShim.extract(deltaPlan).isDefined =>
        IcebergDeltaLogicalShim
          .extract(deltaPlan)
          .flatMap { fields =>
            fields.write.flatMap { deltaWrite =>
              if (!IcebergReflection.isIcebergPositionDeltaWrite(deltaWrite)) {
                None
              } else {
                val dispatch = WriteDeltaDispatchInfo.build(
                  fields.projections,
                  fields.query.output,
                  IcebergDeltaWriterShim.OperationCodes,
                  fields.command)
                dispatch.flatMap { info =>
                  buildDeltaTwoOp(
                    deltaWrite,
                    fields.originalTable,
                    fields.query,
                    PositionDeltaWrite(info),
                    fields.command,
                    fields.tableName)
                }
              }
            }
          }
          .toList
      // Hit by AQE.
      case l @ IcebergWriteLogical(child, batchWrite, dispatch) =>
        Seq(IcebergWriteExec(batchWrite, l.output, planLater(child), dispatch))
      case _ => Nil
    }
  }

  private def matchedSparkWrite(
      table: org.apache.spark.sql.catalyst.analysis.NamedRelation,
      write: Option[Write],
      query: LogicalPlan,
      dispatch: IcebergWriteDispatch,
      command: Option[DeltaCommand] = None,
      tableName: Option[String] = None): Option[SparkPlan] = {
    table match {
      case rel: DataSourceV2Relation =>
        write.flatMap { w =>
          if (IcebergReflection.isIcebergSparkWrite(w)) {
            buildTwoOp(w, rel, query, dispatch, command, tableName.orElse(Some(rel.name)))
          } else {
            None
          }
        }
      case _ => None
    }
  }

  /**
   * Builds the two-op tree. The committer and writer share one `BatchWrite` (also reused across
   * AQE re-plans): `toBatch()` returns a fresh instance per call, but the committer's commit-time
   * validation must see the same instance the writer wrote through, hence we store it. The
   * writer's child is wrapped in [[IcebergWriteLogical]] so AQE re-emits only the data-writing
   * operator on each re-plan as opposed to multiple new commit operators.
   *
   * Iceberg's `SparkWrite` never asks for Spark's commit coordinator, so the
   * `useCommitCoordinator` fallback below is defensive coverage in case a future Iceberg version
   * changes that; the split writer's per-task commit protocol does not use it.
   */
  private def buildTwoOp(
      write: Write,
      rel: DataSourceV2Relation,
      query: LogicalPlan,
      dispatch: IcebergWriteDispatch,
      command: Option[DeltaCommand],
      tableName: Option[String]): Option[SparkPlan] = {
    val batchWrite = write.toBatch
    if (batchWrite.useCommitCoordinator()) {
      return None
    }
    // To mirror Spark ReplaceData semantics we invalidate our cache of the state of
    // `originalTable`.
    val refresh: () => Unit = () => IcebergRefreshCacheShim.refreshCache(session, rel)
    val commit = IcebergCommitExec(
      batchWrite,
      write,
      refresh,
      planLater(IcebergWriteLogical(query, batchWrite, dispatch)),
      command = command,
      tableName = tableName)
    Some(if (command.isDefined) IcebergCommitPlanShim.wrap(commit) else commit)
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
              PositionDeltaCreatedFiles,
              command,
              tableName.orElse(fieldsTableName(table)))
            Some(if (command.isDefined) IcebergCommitPlanShim.wrap(commit) else commit)
          }
        } catch {
          case scala.util.control.NonFatal(e) =>
            None
        }
      case _ => None
    }
  }

  private def fieldsTableName(
      table: org.apache.spark.sql.catalyst.analysis.NamedRelation): Option[String] =
    table match {
      case rel: DataSourceV2Relation => Some(rel.name)
      case _ => None
    }
}
