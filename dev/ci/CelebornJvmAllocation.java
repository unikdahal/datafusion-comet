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

package org.apache.comet.benchmark;

import java.lang.management.ManagementFactory;

import com.sun.management.ThreadMXBean;

/** Public JVM allocation counters for complete query measurements. */
public final class CelebornJvmAllocation {
  static {
    String library = System.getProperty("celeborn.benchmark.nativeLibrary");
    if (library != null) {
      System.load(library);
    }
  }

  private CelebornJvmAllocation() {}

  /** Arena bytes, live arena bytes, free arena bytes, mmap bytes, and top free bytes. */
  public static native long[] nativeAllocatorSnapshot();

  /** Release unused glibc pages after all measurements; never called during a timed query. */
  public static native boolean trimNativeAllocator();

  public static String snapshot() {
    ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    if (!bean.isThreadAllocatedMemorySupported()) {
      throw new UnsupportedOperationException("JVM allocation counters are unavailable");
    }
    if (!bean.isThreadAllocatedMemoryEnabled()) {
      bean.setThreadAllocatedMemoryEnabled(true);
    }
    long[] ids = bean.getAllThreadIds();
    long[] allocated = bean.getThreadAllocatedBytes(ids);
    StringBuilder json =
        new StringBuilder("{\"total_started\":")
            .append(bean.getTotalStartedThreadCount())
            .append(",\"threads\":{");
    boolean first = true;
    for (int i = 0; i < ids.length; i++) {
      if (allocated[i] < 0) {
        continue;
      }
      if (!first) {
        json.append(',');
      }
      first = false;
      json.append('"').append(ids[i]).append("\":").append(allocated[i]);
    }
    return json.append("}}").toString();
  }
}
