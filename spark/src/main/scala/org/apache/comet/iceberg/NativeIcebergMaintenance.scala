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

import java.util.{Map => JMap}

import org.apache.comet.NativeBase

/** JNI surface for Iceberg maintenance operations implemented entirely in native code. */
private[iceberg] final class NativeIcebergMaintenance extends NativeBase {

  /**
   * Finds and optionally removes orphan files without invoking Iceberg-Java actions or Spark
   * jobs.
   *
   * The Java/Scala side only supplies resolved table metadata and storage configuration. Metadata
   * parsing, manifest traversal, object-store listing, age filtering, comparison, and deletion
   * all execute in Rust.
   *
   * @param flags
   *   bitmask: bit 0 = dry run, bit 1 = allow unsafe older_than interval
   */
  @native def removeOrphanFiles(
      metadataLocation: String,
      scanLocation: String,
      olderThanMillis: Long,
      maxConcurrentDeletes: Int,
      fileIOProperties: JMap[String, String],
      objectStoreOptions: JMap[String, String],
      equalSchemes: JMap[String, String],
      equalAuthorities: JMap[String, String],
      prefixMismatchMode: String,
      flags: Int): Array[String]
}
