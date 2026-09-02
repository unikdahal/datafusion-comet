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

package org.apache.comet.rules.shims

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.MergeRowsExec

import org.apache.comet.serde.CometOperatorSerde
import org.apache.comet.serde.operator.CometMergeRows

/**
 * Registers native `MergeRowsExec` only when doing so preserves Spark's V2 write contract.
 *
 * Spark 4.0, like Spark 3.5, commits V2 writes through the summary-less `BatchWrite.commit`
 * overload, so replacing `MergeRowsExec` is safe. Starting in Spark 4.1,
 * `V2ExistingTableWriteExec` discovers the concrete Spark `MergeRowsExec` in the write query,
 * builds a `MergeSummary` from its metrics, and passes that summary to the summary-aware
 * `BatchWrite.commit` overload. Replacing the Spark node with the peer `CometMergeRowsExec` makes
 * that discovery fail and silently changes the data-source commit contract.
 *
 * Keep Spark 4.1+ on the JVM until Comet can preserve `MergeSummary` end-to-end. Do not broaden
 * this gate based only on row-output parity: the writer-side summary is externally observable.
 */
object ShimCometMergeRows {
  val nativeExecs: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] =
    if (SPARK_VERSION.startsWith("4.0.")) {
      Map(classOf[MergeRowsExec] -> CometMergeRows)
    } else {
      Map.empty
    }
}
