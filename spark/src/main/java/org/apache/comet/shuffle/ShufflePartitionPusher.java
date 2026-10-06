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

package org.apache.comet.shuffle;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Receives complete encoded shuffle blocks from a native partition writer.
 *
 * <p>Instances belong to one Spark task. Implementations must be safe to invoke from native worker
 * threads, which do not inherit the Spark task thread's thread-local context.
 */
@FunctionalInterface
public interface ShufflePartitionPusher {

  /** Reserves an upper bound before a native worker starts encoding a shuffle frame. */
  default void reservePartitionData(int maxLength) throws IOException {}

  /**
   * Acknowledges that native encoding buffers and JNI local references have been released, on both
   * success and failure. Implementations may retain admission until asynchronous pushes also
   * finish.
   */
  default void releasePartitionDataReservation() {}

  /** Returns the largest encoding reservation this callback can admit before allocating buffers. */
  default int maxReservationBytes() {
    return Integer.MAX_VALUE;
  }

  /** Returns the largest complete frame that this callback can safely accept. */
  default int maxFrameBytes() {
    return Integer.MAX_VALUE - 8;
  }

  /** Pushes one complete, length-prefixed Arrow IPC block for the given output partition. */
  void pushPartitionData(int partitionId, byte[] data, int length) throws IOException;

  /**
   * Returns how many frame-sized buffers can be alive at once for one pushed frame, counting the
   * native encoder's output. Native writers admit that many frames before encoding one.
   */
  default int frameCopies() {
    return 3;
  }

  /** Returns whether native writers should push frames through {@link #pushNativeFrame}. */
  default boolean acceptsNativeFrames() {
    return false;
  }

  /**
   * Pushes one complete frame that remains in native memory, taking ownership of it.
   *
   * <p>{@code frame} is a direct buffer over the native frame. Implementations must call {@link
   * NativeShuffleFrames#release} with {@code releaseHandle} exactly once, after the last reference
   * to {@code frame} is gone, including when this method throws. The frame then keeps {@code
   * retainedBytes} of native memory, which an implementation that bounds in-flight memory should
   * account for until it is released.
   */
  default void pushNativeFrame(
      int partitionId, ByteBuffer frame, long releaseHandle, int retainedBytes) throws IOException {
    try {
      byte[] data = new byte[frame.remaining()];
      frame.duplicate().get(data);
      pushPartitionData(partitionId, data, data.length);
    } finally {
      NativeShuffleFrames.release(releaseHandle);
    }
  }
}
