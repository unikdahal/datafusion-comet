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

package org.apache.spark.sql.comet

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf

class CometBroadcastSlicedOffsetsSuite extends CometTestBase {

  test("broadcast hash join with distinct string keys matches Spark result") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "10485760",
      SQLConf.SHUFFLE_PARTITIONS.key -> "4",
      CometConf.COMET_EXEC_ENABLED.key -> "true",
      "spark.comet.expression.Cast.allowIncompatible" -> "true") {
      withTempView("t") {
        spark
          .range(0, 200000, 1, 4)
          .selectExpr("concat('k', lpad(cast(id as string), 10, '0')) AS id")
          .createOrReplaceTempView("t")

        val df = sql(
          "SELECT /*+ BROADCAST(d) */ count(*) FROM t JOIN (SELECT DISTINCT concat('k', lpad(cast(id as string),10,'0')) AS id FROM range(0, 200000, 1, 4)) d ON t.id = d.id")
        checkSparkAnswer(df)
      }
    }
  }
}
