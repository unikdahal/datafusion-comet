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

import org.apache.spark.sql.catalyst.parser.{ParameterContext, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Preserve Spark 4.1+'s explicit parameter context.
 *
 * Parameterized CALLs are not intercepted yet: fallback would need the bound parameter values,
 * not just the SQL text with markers. Let Iceberg-Java handle them until plan-based fallback
 * exists.
 */
private[iceberg] trait ShimCometIcebergMaintenanceParser { self: ParserInterface =>
  protected def delegate: ParserInterface
  protected def transformParsedPlan(plan: LogicalPlan): LogicalPlan

  override def parsePlanWithParameters(
      sqlText: String,
      parameterContext: ParameterContext): LogicalPlan =
    delegate.parsePlanWithParameters(sqlText, parameterContext)
}
