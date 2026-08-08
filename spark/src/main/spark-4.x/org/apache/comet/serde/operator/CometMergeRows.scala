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

package org.apache.comet.serde.operator

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.MergeRows
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.comet.{CometMergeRowsExec, SerializedPlan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.MergeRowsExec

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.ConfigEntry
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.{MergeInstruction, MergeOutputRow, Operator}
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}

/**
 * Serde for Spark's `MergeRowsExec` (the row-level MERGE dispatch operator moved from Iceberg
 * extensions into Spark core in Iceberg 1.4.0 / SPARK-52403). Only exists on Spark 3.5+, hence
 * this file lives in a version-gated source root rather than the shared `serde/operator` package.
 *
 * Spark's real shape (verified against the 4.1.1 / 3.5.7 bytecode) is simpler than it might look:
 * `isSourceRowPresent` / `isTargetRowPresent` are boolean-valued `Expression`s (not column
 * ordinals), and each `MergeRows.Instruction` (Keep / Discard / Split) is uniformly a `condition:
 * Expression` plus `outputs: Seq[Seq[Expression]]` -- zero output projections means Discard, one
 * means Keep, two means Split. There is no separate instruction-kind enum on the Spark side, so
 * none is introduced on the wire either.
 */
object CometMergeRows extends CometOperatorSerde[MergeRowsExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_MERGE_ROWS_ENABLED)

  override def getSupportLevel(op: MergeRowsExec): SupportLevel = {
    if (op.checkCardinality && rowIdOrdinal(op).isEmpty) {
      Unsupported(
        Some(s"MERGE cardinality check requires a resolvable '${MergeRows.ROW_ID}' column"))
    } else {
      Compatible(None)
    }
  }

  override def convert(
      op: MergeRowsExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[Operator] = {
    val input = op.child.output

    def convertInstruction(instr: MergeRows.Instruction): Option[MergeInstruction] = {
      val condition = exprToProto(instr.condition, input)
      val outputs = instr.outputs.map { row =>
        val exprs = row.map(exprToProto(_, input))
        if (exprs.forall(_.isDefined)) {
          Some(MergeOutputRow.newBuilder().addAllExprs(exprs.map(_.get).asJava).build())
        } else {
          None
        }
      }

      if (condition.isDefined && outputs.forall(_.isDefined)) {
        Some(
          MergeInstruction
            .newBuilder()
            .setCondition(condition.get)
            .addAllOutputs(outputs.map(_.get).asJava)
            .build())
      } else {
        None
      }
    }

    val matched = op.matchedInstructions.map(convertInstruction)
    val notMatched = op.notMatchedInstructions.map(convertInstruction)
    val notMatchedBySource = op.notMatchedBySourceInstructions.map(convertInstruction)

    val isSourcePresent = exprToProto(op.isSourceRowPresent, input)
    val isTargetPresent = exprToProto(op.isTargetRowPresent, input)

    val outputTypes = op.output.map(a => serializeDataType(a.dataType))

    // `rowIdOrd` is only required when Spark asked for a cardinality check; otherwise it stays
    // unset on the wire (see `MergeRows.row_id_ordinal`, now `optional`).
    val rowIdOrd: Option[Int] = if (op.checkCardinality) rowIdOrdinal(op) else None
    val cardinalityCheckSatisfied = !op.checkCardinality || rowIdOrd.isDefined

    // `childOp` is empty when the child did not itself convert to a native operator --
    // `CometExecRule.convertToComet` still calls `convert` in that case. Without this guard a
    // childless `MergeRows` reaches the planner, where `assert_eq!(children.len(), 1)` panics
    // instead of falling back to the JVM operator.
    if (childOp.nonEmpty && matched.forall(_.isDefined) && notMatched.forall(_.isDefined) &&
      notMatchedBySource.forall(_.isDefined) && isSourcePresent.isDefined &&
      isTargetPresent.isDefined && outputTypes.forall(_.isDefined) && cardinalityCheckSatisfied) {
      val mergeBuilder = OperatorOuterClass.MergeRows
        .newBuilder()
        .setIsSourceRowPresent(isSourcePresent.get)
        .setIsTargetRowPresent(isTargetPresent.get)
        .addAllMatchedInstructions(matched.map(_.get).asJava)
        .addAllNotMatchedInstructions(notMatched.map(_.get).asJava)
        .addAllNotMatchedBySourceInstructions(notMatchedBySource.map(_.get).asJava)
        .addAllOutputTypes(outputTypes.map(_.get).asJava)
      rowIdOrd.foreach(mergeBuilder.setRowIdOrdinal)
      Some(builder.setMergeRows(mergeBuilder).build())
    } else {
      // Re-derive the sub-expressions for fallback-reason rollup only on this (uncommon) path,
      // rather than accumulating them on every conversion regardless of outcome. Includes the
      // child so that when the fallback is caused by an unconverted child (empty `childOp`)
      // rather than an unsupported expression, the child's own reason rolls up here instead of
      // the operator reporting no reason at all -- same as `CometFilterExec`.
      val allExprs: Seq[Expression] =
        (op.matchedInstructions ++ op.notMatchedInstructions ++ op.notMatchedBySourceInstructions)
          .flatMap(instr => instr.condition +: instr.outputs.flatten) :+
          op.isSourceRowPresent :+ op.isTargetRowPresent
      val fallbackNodes: Seq[TreeNode[_]] = allExprs :+ op.child
      withFallbackReason(op, fallbackNodes: _*)
      None
    }
  }

  override def createExec(nativeOp: Operator, op: MergeRowsExec): CometMergeRowsExec = {
    CometMergeRowsExec(nativeOp, op, op.output, op.child, SerializedPlan(None))
  }

  /**
   * Locates the ordinal of Iceberg's target row-id column (`MergeRows.ROW_ID`) in the child
   * output. `MergeRowsExec` tracks this internally (via its private `BitmapCardinalityValidator`)
   * for the cardinality check but doesn't expose the ordinal publicly, so it's re-derived here
   * from the well-known column name.
   */
  private def rowIdOrdinal(op: MergeRowsExec): Option[Int] = {
    val idx = op.child.output.indexWhere(_.name == MergeRows.ROW_ID)
    if (idx >= 0) Some(idx) else None
  }
}
