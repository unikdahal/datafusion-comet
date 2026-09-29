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

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.comet.{CometMergeRowsExec, CometNativeExec, SerializedPlan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.LongType

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withFallbackReason
import org.apache.comet.ConfigEntry
import org.apache.comet.iceberg.{Iceberg34MergeRowsFields, Iceberg34MergeRowsReflection}
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.{MergeInstruction, MergeOutputRow, Operator}
import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}

/** Serde for the reflective Iceberg 1.8/Spark 3.4 MergeRowsExec shape. */
object CometIceberg34MergeRows extends CometOperatorSerde[SparkPlan] {
  override def enabledConfig: Option[ConfigEntry[Boolean]] =
    Some(CometConf.COMET_EXEC_MERGE_ROWS_ENABLED)

  override def getSupportLevel(op: SparkPlan): SupportLevel =
    Iceberg34MergeRowsReflection.extract(op) match {
      case Left(reason) => Unsupported(Some(reason))
      case Right(fields) =>
        validate(fields) match {
          case Some(reason) => Unsupported(Some(reason))
          case None => Compatible(Some(compatibilityNote))
        }
    }

  override def convert(
      op: SparkPlan,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[Operator] = {
    if (childOp.isEmpty) {
      withFallbackReason(op, "No child operator")
      return None
    }
    val fields = Iceberg34MergeRowsReflection.extract(op) match {
      case Left(reason) =>
        withFallbackReason(op, reason)
        return None
      case Right(fields) => fields
    }
    validate(fields) match {
      case Some(reason) =>
        withFallbackReason(op, reason)
        return None
      case None =>
    }

    val input = fields.child.output
    val targetFallback = if (fields.emitNotMatchedTargetRows) {
      Seq(Literal.TrueLiteral -> Seq(fields.targetOutput))
    } else {
      Seq.empty
    }
    val matched = fields.matchedConditions.zip(fields.matchedOutputs) ++ targetFallback
    val notMatchedBySource = targetFallback

    def instruction(
        condition: Expression,
        outputs: Seq[Seq[Expression]]): Option[MergeInstruction] = {
      val conditionProto = exprToProto(condition, input)
      val outputProtos = outputs.map { row =>
        val exprs = row.map(exprToProto(_, input))
        if (exprs.forall(_.isDefined)) {
          Some(MergeOutputRow.newBuilder().addAllExprs(exprs.map(_.get).asJava).build())
        } else {
          None
        }
      }
      if (conditionProto.isDefined && outputProtos.forall(_.isDefined)) {
        Some(
          MergeInstruction
            .newBuilder()
            .setCondition(conditionProto.get)
            .addAllOutputs(outputProtos.map(_.get).asJava)
            .build())
      } else {
        None
      }
    }

    val matchedProto = matched.map { case (condition, outputs) =>
      instruction(condition, outputs)
    }
    val notMatchedProto = fields.notMatchedConditions.zip(fields.notMatchedOutputs).map {
      case (condition, output) => instruction(condition, Seq(output))
    }
    val notMatchedBySourceProto = notMatchedBySource.map { case (condition, output) =>
      instruction(condition, output)
    }
    val sourcePresent = exprToProto(fields.isSourceRowPresent, input)
    val targetPresent = exprToProto(fields.isTargetRowPresent, input)
    val outputTypes = fields.output.map(attribute => serializeDataType(attribute.dataType))

    if (matchedProto.forall(_.isDefined) && notMatchedProto.forall(_.isDefined) &&
      notMatchedBySourceProto.forall(_.isDefined) && sourcePresent.isDefined &&
      targetPresent.isDefined && outputTypes.forall(_.isDefined)) {
      val merge = OperatorOuterClass.MergeRows
        .newBuilder()
        .setIsSourceRowPresent(sourcePresent.get)
        .setIsTargetRowPresent(targetPresent.get)
        .addAllMatchedInstructions(matchedProto.map(_.get).asJava)
        .addAllNotMatchedInstructions(notMatchedProto.map(_.get).asJava)
        .addAllNotMatchedBySourceInstructions(notMatchedBySourceProto.map(_.get).asJava)
        .addAllOutputTypes(outputTypes.map(_.get).asJava)
      if (fields.performCardinalityCheck) {
        rowIdOrdinal(fields).foreach(merge.setRowIdOrdinal)
      }
      Some(builder.setMergeRows(merge).build())
    } else {
      withFallbackReason(op, "Unsupported expression in Iceberg 1.8 MERGE instructions")
      None
    }
  }

  override def createExec(nativeOp: Operator, op: SparkPlan): CometNativeExec = {
    val fields = Iceberg34MergeRowsReflection
      .extract(op)
      .fold(reason => throw new IllegalStateException(reason), identity)
    val matchedExpressions =
      fields.matchedConditions ++ fields.matchedOutputs.flatten.flatten ++
        (if (fields.emitNotMatchedTargetRows) fields.targetOutput else Seq.empty)
    val notMatchedExpressions = fields.notMatchedConditions ++ fields.notMatchedOutputs.flatten
    val notMatchedBySourceExpressions =
      if (fields.emitNotMatchedTargetRows) fields.targetOutput else Seq.empty
    CometMergeRowsExec(
      nativeOp,
      op,
      fields.output,
      fields.isSourceRowPresent,
      fields.isTargetRowPresent,
      matchedExpressions,
      notMatchedExpressions,
      notMatchedBySourceExpressions,
      fields.performCardinalityCheck,
      if (fields.performCardinalityCheck) rowIdOrdinal(fields) else None,
      fields.child,
      SerializedPlan(None))
  }

  private def validate(fields: Iceberg34MergeRowsFields): Option[String] = {
    if (fields.matchedConditions.size != fields.matchedOutputs.size) {
      return Some("Iceberg 1.8 MERGE matched condition/output counts disagree")
    }
    if (fields.notMatchedConditions.size != fields.notMatchedOutputs.size) {
      return Some("Iceberg 1.8 MERGE not-matched condition/output counts disagree")
    }
    if (fields.matchedOutputs.exists(_.size > 2)) {
      return Some("Iceberg 1.8 MERGE matched action has more than two output rows")
    }
    if (fields.matchedOutputs.flatten.exists(_.size != fields.output.size) ||
      fields.notMatchedOutputs.exists(_.size != fields.output.size)) {
      return Some("Iceberg 1.8 MERGE action output width disagrees with MergeRowsExec.output")
    }
    if (fields.emitNotMatchedTargetRows &&
      (fields.targetOutput.isEmpty || fields.targetOutput.size != fields.output.size)) {
      return Some("Iceberg 1.8 MERGE target-copy output is incompatible with its output schema")
    }
    if (fields.performCardinalityCheck && rowIdOrdinal(fields).isEmpty) {
      return Some("Iceberg 1.8 MERGE cardinality check requires a Long __row_id column")
    }
    None
  }

  private val compatibilityNote =
    "Native Iceberg 1.8 MERGE preserves row values but may differ in physical output ordering"

  private def rowIdOrdinal(fields: Iceberg34MergeRowsFields): Option[Int] = {
    val ordinal = fields.child.output.indexWhere(attribute =>
      attribute.name == "__row_id" && attribute.dataType == LongType)
    if (ordinal >= 0) Some(ordinal) else None
  }
}
