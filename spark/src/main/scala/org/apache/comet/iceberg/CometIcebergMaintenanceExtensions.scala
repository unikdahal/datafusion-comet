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

import org.apache.spark.sql.SparkSessionExtensions

/**
 * Comet-plugin-scoped parser extension for native Iceberg maintenance procedures.
 *
 * [[org.apache.spark.CometDriverPlugin]] appends this extension after the normal Comet extension,
 * so it wraps the complete parser chain (including Iceberg's Spark 3.x extension) without relying
 * on global ServiceLoader discovery.
 */
class CometIcebergMaintenanceExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { case (session, parser) =>
      new CometIcebergMaintenanceParser(session, parser)
    }
  }
}
