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

import org.apache.spark.sql.catalyst.plans.logical.{InsertOnlyMerge, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.comet.{IcebergCommitExec, IcebergTransactionalCommitExec}

/** Spark 4.2 injects transactions only into physical TransactionalExec nodes. */
private[iceberg] object IcebergCommitPlanShim {
  def wrap(commit: IcebergCommitExec): SparkPlan = IcebergTransactionalCommitExec(commit)
}

/** Extracts Spark 4.2's insert-only MERGE rewrite before DataSourceV2Strategy plans it. */
private[iceberg] object IcebergInsertOnlyMergeShim extends IcebergInsertOnlyMergeShim {
  override def extract(plan: LogicalPlan): Option[InsertOnlyMergeFields] = plan match {
    case InsertOnlyMerge(table: DataSourceV2Relation, query, write, _) =>
      Some(InsertOnlyMergeFields(table, query, write, table.name))
    case _ => None
  }
}
