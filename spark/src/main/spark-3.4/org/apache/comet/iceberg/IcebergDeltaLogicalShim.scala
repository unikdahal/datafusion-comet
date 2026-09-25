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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/** Spark 3.4 has a stock WriteDelta for DELETE and Iceberg's WriteIcebergDelta for UPDATE/MERGE. */
private[iceberg] object IcebergDeltaLogicalShim extends IcebergDeltaLogicalShim {
  private val supportedPlans = Set(
    "org.apache.spark.sql.catalyst.plans.logical.WriteDelta",
    "org.apache.spark.sql.catalyst.plans.logical.WriteIcebergDelta")

  override def extract(plan: LogicalPlan): Option[DeltaLogicalFields] =
    if (supportedPlans.contains(plan.getClass.getName)) {
      IcebergReflection.extractDeltaLogicalFields(plan)
    } else {
      None
    }
}
