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
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Sends complete Comet shuffle frames through Celeborn's {@code ShuffleClient#pushRawData}.
 *
 * <p>That API frames a caller-owned buffer without copying it and runs a release callback exactly
 * once, after the client and its transport no longer reference the buffer. This pusher relies on
 * that contract alone: each frame's bytes stay admitted against the executor's in-flight limit, and
 * native frames stay alive, until their callback runs. Native frames reach Celeborn through direct
 * buffers over native memory, so no copy is made after encoding.
 *
 * <p>Celeborn is an optional dependency, so its client is accepted as an {@link Object} and its
 * public methods are resolved reflectively.
 */
public final class CelebornRawPushPartitionPusher implements CelebornMapOutputPusher {

  private static final String BASE_CLIENT_CLASS = "org.apache.celeborn.client.ShuffleClient";
  private static final int CELEBORN_BATCH_HEADER_BYTES = 4 * Integer.BYTES;
  private static final int MINIMUM_COMET_FRAME_BYTES = 2 * Long.BYTES;
  private static final int MAX_JVM_ARRAY_BYTES = Integer.MAX_VALUE - 8;
  private static final Class<?>[] PUSH_RAW_DATA_PARAMETERS = {
    int.class,
    int.class,
    int.class,
    int.class,
    ByteBuffer.class,
    int.class,
    int.class,
    Runnable.class
  };
  private static final MethodType PUSH_RAW_DATA_TYPE =
      MethodType.methodType(
          int.class,
          Object.class,
          int.class,
          int.class,
          int.class,
          int.class,
          ByteBuffer.class,
          int.class,
          int.class,
          Runnable.class);

  private final Object shuffleClient;
  private final MethodHandle pushRawData;
  private final Method mapperEnd;
  private final Method cleanup;
  private final ExecutorShufflePushAdmission admission;
  private final boolean nativeFrames;
  private final int shuffleId;
  private final int mapId;
  private final int encodedAttemptId;
  private final int numMappers;
  private final int numPartitions;
  private final int maxFrameBytes;
  private final int maxReservationBytes;
  private final AtomicLongArray partitionLengths;
  private final ThreadLocal<Reservation> encodingReservation = new ThreadLocal<>();
  private final Object lock = new Object();

  private State state = State.OPEN;
  private int activePushes;
  private int activeEncoders;
  // Native worker threads currently inside the client. Spark's task kill does not reach them, so
  // abort interrupts them: Celeborn can otherwise wait for in-flight capacity for minutes.
  private final Set<Thread> pushingThreads = new HashSet<>();
  private boolean cleanupPending;

  private enum State {
    OPEN,
    FINISHING,
    FINISHED,
    ABORTED
  }

  /** Admission acquired by one native worker for the frame it is encoding. */
  private static final class Reservation {
    private final int bytes;
    private int claimed;

    private Reservation(int bytes) {
      this.bytes = bytes;
    }
  }

  /**
   * Returns why the given Celeborn client class cannot push caller-owned buffers, or null if it
   * can.
   */
  public static String unavailableReason(Class<?> shuffleClientClass) {
    final Method method;
    try {
      method = shuffleClientClass.getMethod("pushRawData", PUSH_RAW_DATA_PARAMETERS);
    } catch (NoSuchMethodException missing) {
      return "Native Celeborn shuffle requires a Celeborn client that provides "
          + "ShuffleClient#pushRawData to push caller-owned buffers";
    } catch (SecurityException failure) {
      return "Cannot inspect the Celeborn client's push API: " + failure.getMessage();
    }
    if (method.getReturnType() != int.class || Modifier.isStatic(method.getModifiers())) {
      return "The Celeborn client's pushRawData API has an incompatible signature";
    }
    if (method.getDeclaringClass().getName().equals(BASE_CLIENT_CLASS)) {
      return "The Celeborn client "
          + shuffleClientClass.getName()
          + " does not implement pushRawData";
    }
    return null;
  }

  /**
   * Binds one Spark map attempt to an executor's Celeborn client.
   *
   * @param shuffleClient the application's existing Celeborn shuffle client
   * @param shuffleId the Celeborn shuffle identifier for this task
   * @param mapId the logical map partition
   * @param encodedAttemptId the Celeborn-encoded stage and task attempt
   * @param numMappers the total number of map partitions
   * @param numPartitions the total number of reduce partitions
   * @param configuredMaxFrameBytes the largest complete frame to accept
   * @param maxInFlightBytes the executor-wide limit on admitted bytes for this client
   * @param nativeFrames whether native writers hand over frames in native memory
   */
  public CelebornRawPushPartitionPusher(
      Object shuffleClient,
      int shuffleId,
      int mapId,
      int encodedAttemptId,
      int numMappers,
      int numPartitions,
      int configuredMaxFrameBytes,
      int maxInFlightBytes,
      boolean nativeFrames) {
    if (shuffleClient == null) {
      throw new IllegalArgumentException("Celeborn shuffle client must not be null");
    }
    if (shuffleId < 0 || mapId < 0 || encodedAttemptId < 0) {
      throw new IllegalArgumentException("Celeborn shuffle identifiers must not be negative");
    }
    if (numMappers <= 0 || mapId >= numMappers) {
      throw new IllegalArgumentException("Celeborn map ID is outside the mapper count");
    }
    if (numPartitions <= 0) {
      throw new IllegalArgumentException("Celeborn partition count must be positive");
    }
    if (configuredMaxFrameBytes < MINIMUM_COMET_FRAME_BYTES) {
      throw new IllegalArgumentException("Celeborn maximum frame size must fit one Comet frame");
    }
    String unavailable = unavailableReason(shuffleClient.getClass());
    if (unavailable != null) {
      throw new UnsupportedOperationException(unavailable);
    }
    int frameCopies = nativeFrames ? 1 : 2;
    if (maxInFlightBytes < frameCopies * MINIMUM_COMET_FRAME_BYTES) {
      throw new IllegalArgumentException(
          "Celeborn in-flight byte limit must fit all copies of one Comet frame");
    }

    try {
      Method method = shuffleClient.getClass().getMethod("pushRawData", PUSH_RAW_DATA_PARAMETERS);
      this.pushRawData = MethodHandles.publicLookup().unreflect(method).asType(PUSH_RAW_DATA_TYPE);
    } catch (ReflectiveOperationException | SecurityException failure) {
      throw new IllegalArgumentException("Cannot resolve the Celeborn pushRawData API", failure);
    }
    Method mapperEndMethod =
        optionalMethod(
            shuffleClient, "mapperEnd", int.class, int.class, int.class, int.class, int.class);
    if (mapperEndMethod == null) {
      mapperEndMethod =
          optionalMethod(shuffleClient, "mapperEnd", int.class, int.class, int.class, int.class);
    }
    if (mapperEndMethod == null) {
      throw new IllegalArgumentException("Celeborn shuffle client does not provide mapperEnd");
    }
    this.mapperEnd = mapperEndMethod;
    this.cleanup = optionalMethod(shuffleClient, "cleanup", int.class, int.class, int.class);
    this.shuffleClient = shuffleClient;
    this.admission = ExecutorShufflePushAdmission.forClient(shuffleClient, maxInFlightBytes);
    this.nativeFrames = nativeFrames;
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.encodedAttemptId = encodedAttemptId;
    this.numMappers = numMappers;
    this.numPartitions = numPartitions;
    this.maxReservationBytes = maxInFlightBytes;
    this.maxFrameBytes =
        Math.min(
            Math.min(configuredMaxFrameBytes, MAX_JVM_ARRAY_BYTES), maxInFlightBytes / frameCopies);
    this.partitionLengths = new AtomicLongArray(numPartitions);
  }

  private static Method optionalMethod(Object client, String name, Class<?>... parameterTypes) {
    final Method method;
    try {
      method = client.getClass().getMethod(name, parameterTypes);
    } catch (NoSuchMethodException missing) {
      return null;
    }
    if (method.getReturnType() != void.class || Modifier.isStatic(method.getModifiers())) {
      throw new IllegalArgumentException(
          "Celeborn " + name + " API must be an instance method returning void");
    }
    return method;
  }

  @Override
  public int frameCopies() {
    // A native frame is the only copy. Otherwise the native frame and its JVM array overlap
    // while the array is handed to Celeborn, which then keeps only the array.
    return nativeFrames ? 1 : 2;
  }

  @Override
  public boolean acceptsNativeFrames() {
    return nativeFrames;
  }

  @Override
  public int maxFrameBytes() {
    return maxFrameBytes;
  }

  @Override
  public int maxReservationBytes() {
    return maxReservationBytes;
  }

  @Override
  public int numPartitions() {
    return numPartitions;
  }

  @Override
  public void reservePartitionData(int maxLength) throws IOException {
    if (maxLength <= 0 || maxLength > maxReservationBytes) {
      throw new IOException("Celeborn native frame reservation exceeds its byte limit");
    }
    if (encodingReservation.get() != null) {
      throw new IOException("Celeborn native frame already has a reservation on this thread");
    }
    admission.acquire(maxLength, this::isAborted);
    synchronized (lock) {
      if (state != State.OPEN) {
        admission.release(maxLength);
        throw new IOException("Celeborn shuffle map attempt no longer accepts frame encoding");
      }
      activeEncoders++;
    }
    encodingReservation.set(new Reservation(maxLength));
  }

  @Override
  public void releasePartitionDataReservation() {
    Reservation reservation = encodingReservation.get();
    if (reservation == null) {
      return;
    }
    encodingReservation.remove();
    // Bytes claimed by pushed frames stay admitted until Celeborn releases those frames.
    admission.release(reservation.bytes - reservation.claimed);
    synchronized (lock) {
      activeEncoders--;
      lock.notifyAll();
    }
  }

  @Override
  public void pushPartitionData(int partitionId, byte[] data, int length) throws IOException {
    if (data == null || length < 0 || length > data.length) {
      throw new IOException("Celeborn shuffle frame length must describe one complete frame");
    }
    // Celeborn references the array until it releases it, so the array stays admitted until then.
    push(partitionId, ByteBuffer.wrap(data, 0, length), data.length, () -> {});
  }

  @Override
  public void pushNativeFrame(
      int partitionId, ByteBuffer frame, long releaseHandle, int retainedBytes) throws IOException {
    push(partitionId, frame, retainedBytes, () -> NativeShuffleFrames.release(releaseHandle));
  }

  /**
   * Pushes one frame that keeps {@code retained} bytes alive until Celeborn releases it. {@code
   * free} runs exactly once whatever happens, once nothing references the frame any more.
   */
  private void push(int partitionId, ByteBuffer frame, int retained, Runnable free)
      throws IOException {
    boolean admitted = false;
    boolean handedOver = false;
    boolean pushing = false;
    try {
      claim(retained);
      admitted = true;
      int length = frame.remaining();
      validateFrame(partitionId, frame, length);
      beginPush();
      pushing = true;
      Runnable release =
          new ReleaseOnce(
              () -> {
                try {
                  free.run();
                } finally {
                  admission.release(retained);
                }
              });
      // From here on Celeborn runs the release exactly once, even if the call throws.
      handedOver = true;
      int accepted;
      try {
        accepted =
            (int)
                pushRawData.invokeExact(
                    shuffleClient,
                    shuffleId,
                    mapId,
                    encodedAttemptId,
                    partitionId,
                    frame,
                    numMappers,
                    numPartitions,
                    release);
      } catch (IOException | RuntimeException | Error failure) {
        throw failure;
      } catch (Throwable failure) {
        throw new IOException("Celeborn raw shuffle push failed", failure);
      }
      if (accepted < length + CELEBORN_BATCH_HEADER_BYTES) {
        // Celeborn returns 0 once another attempt of this map has ended the mapper.
        throw new IOException(
            "Celeborn raw shuffle push accepted "
                + accepted
                + " bytes; expected at least "
                + (length + CELEBORN_BATCH_HEADER_BYTES)
                + " including its transport header");
      }
      partitionLengths.addAndGet(partitionId, length);
    } catch (IOException | RuntimeException | Error failure) {
      abortAndSuppress(failure);
      throw failure;
    } finally {
      if (!handedOver) {
        try {
          free.run();
        } finally {
          if (admitted) {
            admission.release(retained);
          }
        }
      }
      if (pushing) {
        endPush();
      }
    }
  }

  /** Moves bytes from this thread's encoding reservation to a pushed frame. */
  private void claim(int bytes) throws IOException {
    Reservation reservation = encodingReservation.get();
    if (reservation == null) {
      admission.acquire(bytes, this::isAborted);
      return;
    }
    if (bytes > reservation.bytes - reservation.claimed) {
      // Admit only what the native writer reserved; never let a frame exceed it silently.
      admission.acquire(bytes, this::isAborted);
      return;
    }
    reservation.claimed += bytes;
  }

  private void validateFrame(int partitionId, ByteBuffer frame, int length) throws IOException {
    if (partitionId < 0 || partitionId >= numPartitions) {
      throw new IOException("Celeborn output partition is outside this task's partition count");
    }
    if (length < MINIMUM_COMET_FRAME_BYTES) {
      throw new IOException("Celeborn shuffle frame length must describe one complete frame");
    }
    if (length > maxFrameBytes) {
      throw new IOException("Celeborn shuffle frame exceeds its configured maximum frame size");
    }
    long declaredBodyLength = frame.duplicate().order(ByteOrder.LITTLE_ENDIAN).getLong();
    if (declaredBodyLength != (long) length - Long.BYTES) {
      throw new IOException(
          "Celeborn shuffle frame declares "
              + declaredBodyLength
              + " body bytes, but contains "
              + (length - Long.BYTES));
    }
  }

  private void beginPush() throws IOException {
    synchronized (lock) {
      if (state != State.OPEN) {
        throw new IOException("Celeborn shuffle map attempt no longer accepts partition data");
      }
      activePushes++;
      pushingThreads.add(Thread.currentThread());
    }
  }

  private void endPush() throws IOException {
    boolean cleanupNow;
    synchronized (lock) {
      activePushes--;
      pushingThreads.remove(Thread.currentThread());
      if (state == State.ABORTED) {
        // Do not leak an abort interrupt into whatever the native worker thread runs next.
        Thread.interrupted();
      }
      lock.notifyAll();
      cleanupNow = cleanupPending && activePushes == 0;
      if (cleanupNow) {
        cleanupPending = false;
      }
    }
    if (cleanupNow) {
      cleanupAttempt();
    }
  }

  @Override
  public long[] finish() throws IOException {
    try {
      synchronized (lock) {
        if (state == State.FINISHED) {
          return snapshotPartitionLengths();
        }
        if (state != State.OPEN) {
          throw new IOException("Celeborn shuffle map attempt is not available for completion");
        }
        state = State.FINISHING;
        while ((activePushes != 0 || activeEncoders != 0) && state == State.FINISHING) {
          lock.wait();
        }
        if (state != State.FINISHING) {
          throw new IOException("Celeborn shuffle map attempt was aborted before completion");
        }
      }
      // mapperEnd waits until Celeborn has acknowledged every batch and reports any failure.
      if (mapperEnd.getParameterCount() == 5) {
        mapperEnd.invoke(
            shuffleClient, shuffleId, mapId, encodedAttemptId, numMappers, numPartitions);
      } else {
        mapperEnd.invoke(shuffleClient, shuffleId, mapId, encodedAttemptId, numMappers);
      }
      synchronized (lock) {
        if (state != State.FINISHING) {
          throw new IOException("Celeborn shuffle map attempt was aborted during completion");
        }
        state = State.FINISHED;
      }
      return snapshotPartitionLengths();
    } catch (InterruptedException cause) {
      Thread.currentThread().interrupt();
      IOException failure = new IOException("Interrupted while draining Celeborn pushes", cause);
      abortAndSuppress(failure);
      throw failure;
    } catch (ReflectiveOperationException cause) {
      Throwable failure =
          cause instanceof java.lang.reflect.InvocationTargetException && cause.getCause() != null
              ? cause.getCause()
              : cause;
      IOException wrapped =
          failure instanceof IOException
              ? (IOException) failure
              : new IOException("Celeborn shuffle map completion failed", failure);
      abortAndSuppress(wrapped);
      throw wrapped;
    } catch (IOException | RuntimeException | Error failure) {
      abortAndSuppress(failure);
      throw failure;
    }
  }

  @Override
  public void abort() throws IOException {
    boolean cleanupNow;
    synchronized (lock) {
      if (state == State.FINISHED || state == State.ABORTED) {
        return;
      }
      state = State.ABORTED;
      for (Thread thread : pushingThreads) {
        if (thread != Thread.currentThread()) {
          thread.interrupt();
        }
      }
      lock.notifyAll();
      // Let in-progress client calls return before Celeborn discards this attempt's push state.
      cleanupNow = activePushes == 0;
      cleanupPending = !cleanupNow;
    }
    if (cleanupNow) {
      cleanupAttempt();
    }
  }

  private void cleanupAttempt() throws IOException {
    if (cleanup == null) {
      return;
    }
    try {
      cleanup.invoke(shuffleClient, shuffleId, mapId, encodedAttemptId);
    } catch (java.lang.reflect.InvocationTargetException failure) {
      Throwable cause = failure.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException("Celeborn shuffle map cleanup failed", cause);
    } catch (IllegalAccessException failure) {
      throw new IOException("Cannot invoke the Celeborn cleanup API", failure);
    }
  }

  private boolean isAborted() {
    synchronized (lock) {
      return state == State.ABORTED;
    }
  }

  private void abortAndSuppress(Throwable failure) {
    try {
      abort();
    } catch (Throwable cleanupFailure) {
      if (cleanupFailure != failure) {
        failure.addSuppressed(cleanupFailure);
      }
    }
  }

  private long[] snapshotPartitionLengths() {
    long[] sizes = new long[numPartitions];
    for (int partition = 0; partition < numPartitions; partition++) {
      sizes[partition] = partitionLengths.get(partition);
    }
    return sizes;
  }

  /** Runs a release action at most once, guarding against a client that calls it twice. */
  private static final class ReleaseOnce implements Runnable {
    private final AtomicBoolean released = new AtomicBoolean();
    private final Runnable release;

    private ReleaseOnce(Runnable release) {
      this.release = release;
    }

    @Override
    public void run() {
      if (released.compareAndSet(false, true)) {
        release.run();
      }
    }
  }
}
