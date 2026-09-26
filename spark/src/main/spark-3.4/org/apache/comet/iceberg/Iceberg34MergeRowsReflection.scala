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

import java.lang.ref.WeakReference
import java.lang.reflect.Method
import java.util.WeakHashMap

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.SparkPlan

final case class Iceberg34MergeRowsFields(
    isSourceRowPresent: Expression,
    isTargetRowPresent: Expression,
    matchedConditions: Seq[Expression],
    matchedOutputs: Seq[Seq[Seq[Expression]]],
    notMatchedConditions: Seq[Expression],
    notMatchedOutputs: Seq[Seq[Expression]],
    targetOutput: Seq[Expression],
    performCardinalityCheck: Boolean,
    emitNotMatchedTargetRows: Boolean,
    output: Seq[Attribute],
    child: SparkPlan)

/** Reflection bridge for Iceberg 1.8's Spark 3.4-only physical MergeRowsExec. */
private[comet] object Iceberg34MergeRowsReflection {
  private val className = "org.apache.spark.sql.execution.datasources.v2.MergeRowsExec"

  private final case class Handles(
      isSourceRowPresent: Method,
      isTargetRowPresent: Method,
      matchedConditions: Method,
      matchedOutputs: Method,
      notMatchedConditions: Method,
      notMatchedOutputs: Method,
      targetOutput: Method,
      performCardinalityCheck: Method,
      emitNotMatchedTargetRows: Method,
      output: Method,
      child: Method)

  private val handlesByLoader = new WeakHashMap[ClassLoader, WeakReference[Handles]]()

  def mergeRowsExecClass: Option[Class[_ <: SparkPlan]] =
    try {
      Some(IcebergReflection.loadClass(className).asSubclass(classOf[SparkPlan]))
    } catch {
      case NonFatal(_) => None
    }

  def isMergeRowsExec(plan: SparkPlan): Boolean =
    plan != null && plan.getClass.getName == className

  def extract(plan: SparkPlan): Either[String, Iceberg34MergeRowsFields] =
    try {
      if (!isMergeRowsExec(plan)) {
        return Left(s"${plan.getClass.getName} is not Iceberg 1.8 MergeRowsExec")
      }
      val methods = handles(plan.getClass)
      Right(
        Iceberg34MergeRowsFields(
          methods.isSourceRowPresent.invoke(plan).asInstanceOf[Expression],
          methods.isTargetRowPresent.invoke(plan).asInstanceOf[Expression],
          methods.matchedConditions.invoke(plan).asInstanceOf[Seq[Expression]],
          methods.matchedOutputs.invoke(plan).asInstanceOf[Seq[Seq[Seq[Expression]]]],
          methods.notMatchedConditions.invoke(plan).asInstanceOf[Seq[Expression]],
          methods.notMatchedOutputs.invoke(plan).asInstanceOf[Seq[Seq[Expression]]],
          methods.targetOutput.invoke(plan).asInstanceOf[Seq[Expression]],
          methods.performCardinalityCheck.invoke(plan).asInstanceOf[Boolean],
          methods.emitNotMatchedTargetRows.invoke(plan).asInstanceOf[Boolean],
          methods.output.invoke(plan).asInstanceOf[Seq[Attribute]],
          methods.child.invoke(plan).asInstanceOf[SparkPlan]))
    } catch {
      case NonFatal(error) =>
        Left(s"Could not resolve Iceberg 1.8 MergeRowsExec fields: ${error.getMessage}")
    }

  private def handles(clazz: Class[_]): Handles = synchronized {
    val loader = clazz.getClassLoader
    Option(handlesByLoader.get(loader)).flatMap(ref => Option(ref.get())) match {
      case Some(cached) => cached
      case None =>
        def method(name: String): Method = clazz.getMethod(name)
        val resolved = Handles(
          method("isSourceRowPresent"),
          method("isTargetRowPresent"),
          method("matchedConditions"),
          method("matchedOutputs"),
          method("notMatchedConditions"),
          method("notMatchedOutputs"),
          method("targetOutput"),
          method("performCardinalityCheck"),
          method("emitNotMatchedTargetRows"),
          method("output"),
          method("child"))
        handlesByLoader.put(loader, new WeakReference(resolved))
        resolved
    }
  }
}
