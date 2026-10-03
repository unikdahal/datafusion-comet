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
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.logical.MergeRows
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.{Insert, Keep}
import org.apache.spark.sql.comet.{CometMergeRowsExec, SerializedPlan}
import org.apache.spark.sql.execution.datasources.v2.MergeRowsExec
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.ConfigEntry
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.{MergeInstruction, MergeOutputRow, Operator}
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}

/**
 * Spark 4.2-only serde for the MergeRows child of InsertOnlyMergeExec.
 *
 * InsertOnlyMergeExec owns the V2 MergeSummary and counts every row produced by its query as an
 * insert. That makes its optional MergeRows child safe to execute natively without the eight
 * MergeRows metrics required by general Spark 4.1+ MERGE plans. The shape check below is
 * intentionally strict so a general MergeRowsExec cannot cross this compatibility boundary.
 */
object CometInsertOnlyMergeRows extends CometOperatorSerde[MergeRowsExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_MERGE_ROWS_ENABLED)

  override def getSupportLevel(op: MergeRowsExec): SupportLevel = {
    if (!isInsertOnlyShape(op)) {
      Unsupported(Some(insertOnlyFallbackReason))
    } else if (!instructionShapesSatisfied(op)) {
      Unsupported(Some(instructionShapeFallbackReason))
    } else {
      Compatible(Some(compatibilityNote))
    }
  }

  override def convert(
      op: MergeRowsExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[Operator] = {
    val input = op.child.output
    val expectedOutputTypes = op.output.map(_.dataType)

    def convertInstruction(instr: MergeRows.Instruction): Option[MergeInstruction] = {
      if (!instructionShapeSatisfied(instr, expectedOutputTypes)) {
        return None
      }

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

    val notMatched = op.notMatchedInstructions.map(convertInstruction)
    val isSourcePresent = exprToProto(op.isSourceRowPresent, input)
    val isTargetPresent = exprToProto(op.isTargetRowPresent, input)
    val outputTypes = op.output.map(a => serializeDataType(a.dataType))

    if (childOp.nonEmpty && isInsertOnlyShape(op) && instructionShapesSatisfied(op) &&
      notMatched.forall(_.isDefined) && isSourcePresent.isDefined && isTargetPresent.isDefined &&
      outputTypes.forall(_.isDefined)) {
      val mergeBuilder = OperatorOuterClass.MergeRows
        .newBuilder()
        .setIsSourceRowPresent(isSourcePresent.get)
        .setIsTargetRowPresent(isTargetPresent.get)
        .addAllNotMatchedInstructions(notMatched.map(_.get).asJava)
        .addAllOutputTypes(outputTypes.map(_.get).asJava)
      Some(builder.setMergeRows(mergeBuilder).build())
    } else if (childOp.isEmpty) {
      withFallbackReason(op, "No child operator")
      None
    } else if (!isInsertOnlyShape(op)) {
      withFallbackReason(op, insertOnlyFallbackReason)
      None
    } else if (!instructionShapesSatisfied(op)) {
      withFallbackReason(op, instructionShapeFallbackReason)
      None
    } else {
      withFallbackReason(op, "Unsupported expression in insert-only MERGE instructions")
      None
    }
  }

  override def createExec(nativeOp: Operator, op: MergeRowsExec): CometMergeRowsExec =
    CometMergeRowsExec(
      nativeOp,
      op,
      op.output,
      op.isSourceRowPresent,
      op.isTargetRowPresent,
      Seq.empty,
      op.notMatchedInstructions.map(i => i: Expression),
      Seq.empty,
      checkCardinality = false,
      rowIdOrdinal = None,
      op.child,
      SerializedPlan(None))

  /**
   * This is the exact MergeRows shape emitted inside Spark 4.2 InsertOnlyMergeExec when there are
   * multiple NOT MATCHED clauses. It deliberately excludes all general row-level MERGE plans.
   */
  private def isInsertOnlyShape(op: MergeRowsExec): Boolean =
    !op.checkCardinality &&
      op.matchedInstructions.isEmpty &&
      op.notMatchedBySourceInstructions.isEmpty &&
      op.notMatchedInstructions.size > 1 &&
      op.notMatchedInstructions.forall {
        case Keep(Insert, _, _) => true
        case _ => false
      } &&
      op.isSourceRowPresent.semanticEquals(TrueLiteral) &&
      op.isTargetRowPresent.semanticEquals(FalseLiteral)

  private val insertOnlyFallbackReason: String =
    "Spark 4.2 native MergeRows is limited to InsertOnlyMergeExec rewrites"

  private val instructionShapeFallbackReason: String =
    "Insert-only MERGE instruction output must match the MergeRows plan schema"

  private val compatibilityNote: String =
    "Native insert-only MERGE preserves row values but may differ from Spark in physical output ordering"

  private def instructionShapesSatisfied(op: MergeRowsExec): Boolean = {
    val outputTypes = op.output.map(_.dataType)
    op.notMatchedInstructions.forall(instructionShapeSatisfied(_, outputTypes))
  }

  private def instructionShapeSatisfied(
      instruction: MergeRows.Instruction,
      outputTypes: Seq[DataType]): Boolean =
    instruction.outputs.size == 1 && instruction.outputs.forall { row =>
      row.size == outputTypes.size && row.zip(outputTypes).forall { case (expr, expected) =>
        dataTypeCompatible(expr.dataType, expected)
      }
    }

  /** Spark-compatible type comparison that permits only safe nested-nullability widening. */
  private def dataTypeCompatible(actual: DataType, expected: DataType): Boolean =
    (actual, expected) match {
      case (ArrayType(actualElement, actualNulls), ArrayType(expectedElement, expectedNulls)) =>
        (expectedNulls || !actualNulls) && dataTypeCompatible(actualElement, expectedElement)
      case (
            MapType(actualKey, actualValue, actualNulls),
            MapType(expectedKey, expectedValue, expectedNulls)) =>
        (expectedNulls || !actualNulls) &&
        dataTypeCompatible(actualKey, expectedKey) &&
        dataTypeCompatible(actualValue, expectedValue)
      case (StructType(actualFields), StructType(expectedFields)) =>
        actualFields.length == expectedFields.length &&
        actualFields.zip(expectedFields).forall { case (actualField, expectedField) =>
          actualField.name == expectedField.name &&
          (expectedField.nullable || !actualField.nullable) &&
          dataTypeCompatible(actualField.dataType, expectedField.dataType)
        }
      case _ => actual == expected
    }
}
