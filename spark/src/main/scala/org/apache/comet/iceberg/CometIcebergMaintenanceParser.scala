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

import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

import org.apache.comet.CometConf

/**
 * Parser wrapper for the Iceberg maintenance procedures implemented by Comet.
 *
 * This wrapper never parses CALL syntax itself. It delegates to the parser chain assembled by
 * Spark (including Iceberg's parser on Spark 3.x), then recognizes only the parsed logical plan
 * for `CALL <catalog>.system.remove_orphan_files(...)`. This keeps SQL grammar, identifier
 * quoting, named-argument syntax, parameter handling, and expression parsing owned by
 * Spark/Iceberg.
 *
 * Interception happens here rather than in an analyzer rule because Spark 4 invokes bound
 * procedures before third-party resolution rules. Replacing the parsed CALL prevents
 * Iceberg-Java's RemoveOrphanFilesProcedure from ever being invoked when the native path is
 * active.
 */
private[comet] final class CometIcebergMaintenanceParser(
    session: SparkSession,
    override protected val delegate: ParserInterface)
    extends ParserInterface
    with ShimCometIcebergMaintenanceParser {

  override def parsePlan(sqlText: String): LogicalPlan =
    transformParsedPlan(delegate.parsePlan(sqlText), Some(sqlText))

  override def parseQuery(sqlText: String): LogicalPlan =
    transformParsedPlan(delegate.parseQuery(sqlText), Some(sqlText))

  override def parseExpression(sqlText: String): Expression = delegate.parseExpression(sqlText)
  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)
  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    delegate.parseMultipartIdentifier(sqlText)
  override def parseTableSchema(sqlText: String): StructType = delegate.parseTableSchema(sqlText)
  override def parseDataType(sqlText: String): DataType = delegate.parseDataType(sqlText)

  // Spark 4.x adds this ParserInterface method. Omitting `override` keeps this common source valid
  // on Spark 3.x while still satisfying the Spark 4.x interface.
  def parseRoutineParam(sqlText: String): StructType =
    try {
      delegate.getClass
        .getMethod("parseRoutineParam", classOf[String])
        .invoke(delegate, sqlText)
        .asInstanceOf[StructType]
    } catch {
      case _: NoSuchMethodException => delegate.parseTableSchema(sqlText)
    }

  /** Compatibility hook for Spark-version shims that choose to transform a parsed plan. */
  override protected def transformParsedPlan(plan: LogicalPlan): LogicalPlan =
    transformParsedPlan(plan, None)

  private[iceberg] def transformParsedPlan(
      plan: LogicalPlan,
      originalSql: Option[String]): LogicalPlan = {
    if (!nativeMaintenanceEnabled) {
      return plan
    }

    val parsed =
      try {
        ParsedProcedureCall.from(plan)
      } catch {
        case NonFatal(_) => return plan
      }
    parsed match {
      case Some(call)
          if isRemoveOrphanFiles(call.nameParts) && isIcebergCatalog(call.nameParts.head) =>
        CometRemoveOrphanFilesCommand(
          call.nameParts.head,
          call.argumentNames,
          call.argumentExpressions,
          originalSql)
      case _ => plan
    }
  }

  private def nativeMaintenanceEnabled: Boolean = {
    val conf = session.sessionState.conf
    !CometIcebergMaintenanceParser.nativeMaintenanceBypassed &&
    CometConf.COMET_ENABLED.get(conf) &&
    CometConf.COMET_EXEC_ENABLED.get(conf) &&
    CometConf.COMET_ICEBERG_NATIVE_ENABLED.get(conf) &&
    CometConf.COMET_ICEBERG_REMOVE_ORPHAN_FILES_NATIVE_ENABLED.get(conf)
  }

  private def isRemoveOrphanFiles(parts: Seq[String]): Boolean =
    parts.length == 3 &&
      parts(1).equalsIgnoreCase("system") &&
      parts(2).equalsIgnoreCase("remove_orphan_files")

  /**
   * Avoid hijacking a same-named procedure owned by a non-Iceberg catalog on Spark 4, where CALL
   * is parsed by Spark core. Standard Iceberg Spark catalogs live under org.apache.iceberg.
   */
  private def isIcebergCatalog(catalogName: String): Boolean =
    try {
      var clazz: Class[_] = session.sessionState.catalogManager.catalog(catalogName).getClass
      while (clazz != null) {
        if (clazz.getName.startsWith("org.apache.iceberg.")) {
          return true
        }
        clazz = clazz.getSuperclass
      }
      false
    } catch {
      case NonFatal(_) => false
    }
}

private[comet] object CometIcebergMaintenanceParser {
  // Fallback reparses the original SQL so Iceberg retains ownership of binding and errors. A
  // thread-local bypass prevents that nested parse from being intercepted again without mutating
  // session-global SQLConf or affecting concurrent statements using the same SparkSession.
  private val bypassDepth = new ThreadLocal[Int] {
    override def initialValue(): Int = 0
  }

  private[comet] def withNativeMaintenanceBypassed[T](body: => T): T = {
    val previousDepth = bypassDepth.get()
    bypassDepth.set(previousDepth + 1)
    try {
      body
    } finally {
      if (previousDepth == 0) {
        bypassDepth.remove()
      } else {
        bypassDepth.set(previousDepth)
      }
    }
  }

  private[comet] def nativeMaintenanceBypassed: Boolean = bypassDepth.get() > 0
}

private[iceberg] object ParsedProcedureCall {
  private val CallStatementClass = "org.apache.spark.sql.catalyst.plans.logical.CallStatement"
  private val CallClass = "org.apache.spark.sql.catalyst.plans.logical.Call"
  private val NamedArgumentClass = "org.apache.spark.sql.catalyst.plans.logical.NamedArgument"
  private val PositionalArgumentClass =
    "org.apache.spark.sql.catalyst.plans.logical.PositionalArgument"
  private val NamedArgumentExpressionClass =
    "org.apache.spark.sql.catalyst.expressions.NamedArgumentExpression"

  final case class Parsed(
      nameParts: Seq[String],
      argumentNames: Seq[Option[String]],
      argumentExpressions: Seq[Expression])

  def from(plan: LogicalPlan): Option[Parsed] =
    plan.getClass.getName match {
      // Spark 3.x: Iceberg's extension parser produces CallStatement + CallArgument wrappers.
      case CallStatementClass => fromSpark3CallStatement(plan)
      // Spark 4.x: Spark core produces Call(UnresolvedProcedure, Seq[Expression], ...).
      case CallClass => fromSpark4Call(plan)
      case _ => None
    }

  private def fromSpark3CallStatement(plan: LogicalPlan): Option[Parsed] = {
    val nameParts =
      try {
        invokeSeq[String](plan, "name")
      } catch {
        case NonFatal(_) => return None
      }
    val rawArguments =
      try {
        invokeSeq[AnyRef](plan, "args")
      } catch {
        case NonFatal(_) => return None
      }
    val parsedArguments = rawArguments.map { argument =>
      argument.getClass.getName match {
        case NamedArgumentClass =>
          try {
            Some(invoke[String](argument, "name")) -> invoke[Expression](argument, "expr")
          } catch {
            case NonFatal(_) => return None
          }
        case PositionalArgumentClass =>
          try {
            None -> invoke[Expression](argument, "expr")
          } catch {
            case NonFatal(_) => return None
          }
        case _ =>
          // Unknown future representation: fall through to Iceberg-Java.
          return None
      }
    }
    Some(Parsed(nameParts, parsedArguments.map(_._1), parsedArguments.map(_._2)))
  }

  private def fromSpark4Call(plan: LogicalPlan): Option[Parsed] = {
    val procedure =
      try {
        invoke[AnyRef](plan, "procedure")
      } catch {
        case NonFatal(_) => return None
      }
    // Only unresolved parser output is eligible. A bound procedure must never be invoked or
    // inspected reflectively here because that would move maintenance semantics back into Java.
    if (!procedure.getClass.getName.endsWith(".UnresolvedProcedure")) {
      return None
    }

    val nameParts =
      try {
        invokeSeq[String](procedure, "nameParts")
      } catch {
        case NonFatal(_) => return None
      }
    val expressions =
      try {
        invokeSeq[Expression](plan, "args")
      } catch {
        case NonFatal(_) => return None
      }
    val parsedArguments = expressions.map { expression =>
      if (expression.getClass.getName == NamedArgumentExpressionClass) {
        try {
          Some(invoke[String](expression, "key")) -> invoke[Expression](expression, "value")
        } catch {
          case NonFatal(_) => return None
        }
      } else {
        None -> expression
      }
    }
    Some(Parsed(nameParts, parsedArguments.map(_._1), parsedArguments.map(_._2)))
  }

  private def invoke[T](target: AnyRef, method: String): T =
    target.getClass.getMethod(method).invoke(target).asInstanceOf[T]

  private def invokeSeq[T](target: AnyRef, method: String): Seq[T] =
    invoke[Seq[T]](target, method)
}

private[iceberg] object CometRemoveOrphanFilesParameters {
  private val BaseNames: IndexedSeq[String] = IndexedSeq(
    "table",
    "older_than",
    "location",
    "dry_run",
    "max_concurrent_deletes",
    "file_list_view",
    "equal_schemes",
    "equal_authorities",
    "prefix_mismatch_mode")

  /**
   * Match the parameter surface of the Iceberg runtime on the classpath. Iceberg 1.10 added
   * `prefix_listing`; 1.11 added `stream_results`. Interception happens before Iceberg's
   * procedure binder, so accepting a parameter unavailable in that runtime would otherwise change
   * SQL semantics. If the version cannot be read, use the oldest common surface rather than
   * guessing.
   */
  def supportedNames: IndexedSeq[String] =
    icebergMajorMinor match {
      case Some((major, minor)) if major > 1 || (major == 1 && minor >= 11) =>
        BaseNames ++ IndexedSeq("prefix_listing", "stream_results")
      case Some((1, minor)) if minor >= 10 => BaseNames :+ "prefix_listing"
      case _ => BaseNames
    }

  private def icebergMajorMinor: Option[(Int, Int)] =
    try {
      val version = IcebergReflection
        .loadClass("org.apache.iceberg.IcebergBuild")
        .getMethod("version")
        .invoke(null)
        .toString
      version.split("[.-]", 3) match {
        case Array(major, minor, _*) => Some(major.toInt -> minor.toInt)
        case _ => None
      }
    } catch {
      case NonFatal(_) => None
    }
}