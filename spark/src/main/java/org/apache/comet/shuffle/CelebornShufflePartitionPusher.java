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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLongArray;

/** Adapts complete Comet frames to Celeborn's public caller-owned buffer contract. */
public final class CelebornShufflePartitionPusher implements ShufflePartitionPusher {
  private static final int HEADER_BYTES = 16;
  private static final int MIN_FRAME_BYTES = 16;
  private static final int MAX_BUFFER_BYTES = Integer.MAX_VALUE - 8;
  private final Object client;
  private final Method push, supports, mapperEnd, cleanup;
  private final ExecutorShufflePushAdmission admission;
  private final int shuffleId, mapId, attemptId, numMappers, numPartitions;
  private final int maxFrameBytes, maxReservationBytes;
  private final AtomicLongArray partitionLengths;
  private final boolean direct;
  private final Object lock = new Object();
  private final Object submissionLock = new Object();
  private final ThreadLocal<Reservation> encoding = new ThreadLocal<>();
  private State state = State.OPEN;
  private int encoders, submissions, pending;
  private boolean cleanupClaimed;
  private IOException failure;

  private enum State {
    OPEN,
    FINISHING,
    FINISHED,
    ABORTED
  }

  private static final class Reservation {
    final int bytes;
    boolean claimed, nativeReleased, transportReleased, released;

    Reservation(int bytes, boolean nativeOwned) {
      this.bytes = bytes;
      nativeReleased = !nativeOwned;
    }
  }

  public CelebornShufflePartitionPusher(
      Object client, int shuffleId, int mapId, int attemptId, int numMappers, int numPartitions) {
    this(
        client,
        shuffleId,
        mapId,
        attemptId,
        numMappers,
        numPartitions,
        MAX_BUFFER_BYTES,
        512 * 1024 * 1024);
  }

  public CelebornShufflePartitionPusher(
      Object client,
      int shuffleId,
      int mapId,
      int attemptId,
      int numMappers,
      int numPartitions,
      int maxInFlightBytes) {
    this(
        client,
        shuffleId,
        mapId,
        attemptId,
        numMappers,
        numPartitions,
        MAX_BUFFER_BYTES,
        maxInFlightBytes);
  }

  public CelebornShufflePartitionPusher(
      Object client,
      int shuffleId,
      int mapId,
      int attemptId,
      int numMappers,
      int numPartitions,
      int configuredMaxFrameBytes,
      int maxInFlightBytes) {
    this(
        client,
        shuffleId,
        mapId,
        attemptId,
        numMappers,
        numPartitions,
        configuredMaxFrameBytes,
        maxInFlightBytes,
        false);
  }

  public CelebornShufflePartitionPusher(
      Object client,
      int shuffleId,
      int mapId,
      int attemptId,
      int numMappers,
      int numPartitions,
      int configuredMaxFrameBytes,
      int maxInFlightBytes,
      boolean direct) {
    this.direct = direct;
    if (client == null
        || shuffleId < 0
        || mapId < 0
        || attemptId < 0
        || numMappers <= mapId
        || numMappers <= 0
        || numPartitions <= 0
        || configuredMaxFrameBytes < MIN_FRAME_BYTES
        || maxInFlightBytes < MIN_FRAME_BYTES * (direct ? 1 : 2) + HEADER_BYTES) {
      throw new IllegalArgumentException("Invalid Celeborn task identity or buffer limits");
    }
    String unavailable = nativePushCompletionUnavailableReason(client.getClass());
    if (unavailable != null) {
      throw new UnsupportedOperationException(unavailable);
    }
    this.client = client;
    this.push =
        publicMethod(
            client.getClass(),
            "pushDataAsync",
            CompletionStage.class,
            int.class,
            int.class,
            int.class,
            int.class,
            ByteBuffer.class,
            int.class,
            int.class);
    this.supports = publicMethod(client.getClass(), "supportsBufferPush", boolean.class);
    this.cleanup =
        publicMethod(client.getClass(), "cleanup", void.class, int.class, int.class, int.class);
    Method end;
    try {
      end =
          publicMethod(
              client.getClass(),
              "mapperEnd",
              void.class,
              int.class,
              int.class,
              int.class,
              int.class,
              int.class);
    } catch (IllegalArgumentException absent) {
      end =
          publicMethod(
              client.getClass(),
              "mapperEnd",
              void.class,
              int.class,
              int.class,
              int.class,
              int.class);
    }
    this.mapperEnd = end;
    requireBufferSupport();
    this.admission = ExecutorShufflePushAdmission.forClient(client, maxInFlightBytes);
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.attemptId = attemptId;
    this.numMappers = numMappers;
    this.numPartitions = numPartitions;
    this.maxReservationBytes = maxInFlightBytes - HEADER_BYTES;
    this.maxFrameBytes =
        Math.min(
            Math.min(configuredMaxFrameBytes, MAX_BUFFER_BYTES),
            maxReservationBytes / (direct ? 1 : 2));
    this.partitionLengths = new AtomicLongArray(numPartitions);
  }

  private static Method publicMethod(
      Class<?> owner, String name, Class<?> result, Class<?>... arguments) {
    try {
      Method method = owner.getMethod(name, arguments);
      if (Modifier.isStatic(method.getModifiers())
          || !result.isAssignableFrom(method.getReturnType())) {
        throw new IllegalArgumentException("Incompatible public Celeborn " + name + " API");
      }
      return method;
    } catch (ReflectiveOperationException | SecurityException failure) {
      throw new IllegalArgumentException("Missing public Celeborn " + name + " API", failure);
    }
  }

  /** A missing contract selects delegated Spark/Celeborn shuffle during planning. */
  public static String nativePushCompletionUnavailableReason(Class<?> owner) {
    try {
      publicMethod(
          owner,
          "pushDataAsync",
          CompletionStage.class,
          int.class,
          int.class,
          int.class,
          int.class,
          ByteBuffer.class,
          int.class,
          int.class);
      publicMethod(owner, "supportsBufferPush", boolean.class);
      publicMethod(owner, "cleanup", void.class, int.class, int.class, int.class);
      return null;
    } catch (IllegalArgumentException failure) {
      return "Native Celeborn shuffle requires the public caller-owned buffer push API: "
          + failure.getMessage();
    }
  }

  private void requireBufferSupport() {
    try {
      if (!(boolean) supports.invoke(client)) {
        throw new UnsupportedOperationException(
            "Native Celeborn buffer shuffle is unavailable with the client's security settings");
      }
    } catch (ReflectiveOperationException failure) {
      throw new IllegalArgumentException("Cannot inspect public Celeborn buffer support", failure);
    }
  }

  @Override
  public boolean supportsDirectPush() {
    return direct;
  }

  @Override
  public int frameCopies() {
    return direct ? 1 : 2;
  }

  @Override
  public int maxFrameBytes() {
    return maxFrameBytes;
  }

  @Override
  public int maxReservationBytes() {
    return maxReservationBytes;
  }

  public int numPartitions() {
    return numPartitions;
  }

  private boolean aborted() {
    synchronized (lock) {
      return state == State.ABORTED;
    }
  }

  private void checkOpen() throws IOException {
    if (failure != null) {
      throw failure;
    }
    if (state != State.OPEN) {
      throw new IOException("Celeborn map attempt no longer accepts frames");
    }
  }

  @Override
  public void reservePartitionData(int bytes) throws IOException {
    requireBufferSupport();
    if (bytes <= 0 || bytes > maxReservationBytes || encoding.get() != null) {
      throw new IOException("Invalid or duplicate Celeborn encoding reservation");
    }
    int charged = bytes + HEADER_BYTES;
    admission.acquire(charged, this::aborted);
    synchronized (lock) {
      try {
        checkOpen();
      } catch (IOException failure) {
        admission.release(charged);
        throw failure;
      }
      encoders++;
      encoding.set(new Reservation(charged, true));
    }
  }

  private void releaseIfRetired(Reservation reservation) {
    int bytes = 0;
    synchronized (lock) {
      if (!reservation.released && reservation.nativeReleased && reservation.transportReleased) {
        reservation.released = true;
        bytes = reservation.bytes;
      }
    }
    admission.release(bytes);
  }

  @Override
  public void releasePartitionDataReservation() {
    Reservation reservation = encoding.get();
    if (reservation == null) {
      return;
    }
    encoding.remove();
    synchronized (lock) {
      reservation.nativeReleased = true;
      if (!reservation.claimed) {
        reservation.transportReleased = true;
      }
      encoders--;
      lock.notifyAll();
    }
    releaseIfRetired(reservation);
  }

  private void validate(int partition, ByteBuffer bytes, int length) throws IOException {
    if (partition < 0
        || partition >= numPartitions
        || bytes == null
        || length < MIN_FRAME_BYTES
        || length > bytes.remaining()
        || length > maxFrameBytes) {
      throw new IOException("Invalid complete Celeborn shuffle frame or output partition");
    }
    long declared = bytes.duplicate().order(ByteOrder.LITTLE_ENDIAN).getLong();
    if (declared != (long) length - Long.BYTES) {
      throw new IOException("Celeborn shuffle frame length does not match its header");
    }
  }

  @Override
  public void pushPartitionData(int partition, byte[] data, int length) throws IOException {
    if (data == null || length < 0 || length > data.length) {
      throw new IOException("Invalid Celeborn heap frame");
    }
    submit(partition, ByteBuffer.wrap(data, 0, length), length, 2);
  }

  /**
   * Borrows native memory only for this invocation. On every exit, including cancellation, all
   * Celeborn owners have retired, so JNI's caller may release the backing native frame.
   */
  @Override
  public void pushPartitionDataDirect(int partition, ByteBuffer data, int length)
      throws IOException {
    if (data == null || !data.isDirect()) {
      throw new IOException("Expected a direct frame");
    }
    CompletableFuture<Integer> completion = submit(partition, data, length, 1);
    boolean interrupted = false;
    IOException cancellationFailure = null;
    try {
      for (; ; ) {
        try {
          completion.get();
          break;
        } catch (InterruptedException cause) {
          interrupted = true;
          // Do not unwind across JNI while Netty can still read the native allocation.
          try {
            abort();
          } catch (IOException cleanupFailure) {
            cancellationFailure = cleanupFailure;
          }
        } catch (ExecutionException cause) {
          throw asIOException("Celeborn buffer push failed", cause.getCause());
        }
      }
      if (interrupted) {
        throw new IOException("Interrupted during Celeborn buffer push", cancellationFailure);
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private CompletableFuture<Integer> submit(int partition, ByteBuffer data, int length, int copies)
      throws IOException {
    requireBufferSupport();
    validate(partition, data, length);
    synchronized (lock) {
      checkOpen();
      submissions++;
    }
    Reservation reservation = null;
    boolean transferred = false;
    boolean counted = false;
    boolean standalone = false;
    Throwable submissionFailure = null;
    try {
      int bytes = Math.addExact(Math.multiplyExact(length, copies), HEADER_BYTES);
      reservation = encoding.get();
      if (reservation == null) {
        admission.acquire(bytes, this::aborted);
        reservation = new Reservation(bytes, false);
        standalone = true;
      }
      synchronized (lock) {
        if (reservation.claimed || bytes > reservation.bytes) {
          throw new IOException("Celeborn push exceeds its encoding reservation");
        }
        checkOpen();
        reservation.claimed = true;
        pending++;
        counted = true;
      }
      final Reservation owned = reservation;
      // Allocate the observer before the client may publish borrowed native memory.
      CompletableFuture<Integer> result = new CompletableFuture<>();
      final CompletionStage<Integer> stage;
      synchronized (submissionLock) {
        synchronized (lock) {
          checkOpen();
        }
        ByteBuffer payload = data.slice();
        payload.limit(length);
        stage =
            (CompletionStage<Integer>)
                push.invoke(
                    client,
                    shuffleId,
                    mapId,
                    attemptId,
                    partition,
                    payload,
                    numMappers,
                    numPartitions);
        if (stage == null) {
          throw new IOException("Celeborn returned no completion stage");
        }
        transferred = true;
      }
      try {
        stage.whenComplete(
            (accepted, cause) -> {
              IOException error =
                  cause == null && accepted != null && accepted == length + HEADER_BYTES
                      ? null
                      : asIOException("Celeborn did not accept the complete framed push", cause);
              synchronized (lock) {
                if (error == null) {
                  partitionLengths.addAndGet(partition, length);
                } else if (failure == null && state != State.ABORTED) {
                  failure = error;
                }
                owned.transportReleased = true;
                pending--;
                lock.notifyAll();
              }
              releaseIfRetired(owned);
              if (error == null) {
                result.complete(accepted);
              } else {
                result.completeExceptionally(error);
              }
            });
      } catch (RuntimeException | Error observerFailure) {
        boolean interrupted = false;
        try {
          CompletableFuture<Integer> lifetime = stage.toCompletableFuture();
          for (; ; ) {
            try {
              lifetime.get();
              break;
            } catch (InterruptedException ignored) {
              interrupted = true;
            } catch (ExecutionException retiredFailure) {
              break;
            }
          }
          synchronized (lock) {
            if (!owned.transportReleased) {
              owned.transportReleased = true;
              pending--;
              lock.notifyAll();
            }
          }
          releaseIfRetired(owned);
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
        throw observerFailure;
      }
      return result;
    } catch (InvocationTargetException cause) {
      Throwable original = cause.getCause();
      submissionFailure = original;
      failSubmission(original);
      if (original instanceof RuntimeException) {
        throw (RuntimeException) original;
      }
      if (original instanceof Error) {
        throw (Error) original;
      }
      IOException error = asIOException("Celeborn buffer submission failed", original);
      submissionFailure = error;
      throw error;
    } catch (IllegalAccessException cause) {
      IOException error = asIOException("Cannot invoke public Celeborn buffer API", cause);
      submissionFailure = error;
      failSubmission(error);
      throw error;
    } catch (IOException | RuntimeException | Error cause) {
      submissionFailure = cause;
      failSubmission(cause);
      throw cause;
    } finally {
      if (!transferred && reservation != null && (counted || standalone)) {
        synchronized (lock) {
          if (counted) {
            pending--;
          }
          reservation.transportReleased = true;
          lock.notifyAll();
        }
        releaseIfRetired(reservation);
      }
      synchronized (lock) {
        submissions--;
        lock.notifyAll();
      }
      try {
        cleanupIfReady();
      } catch (IOException cleanupFailure) {
        // A transferred direct frame must still drain its lifetime promise. Reporting cleanup
        // failure from this finally block would unwind across JNI while owners remain live.
        if (!transferred) {
          if (submissionFailure == null) {
            throw cleanupFailure;
          }
          if (submissionFailure != cleanupFailure) {
            submissionFailure.addSuppressed(cleanupFailure);
          }
        }
        synchronized (lock) {
          if (failure == null) {
            failure = cleanupFailure;
          }
        }
      }
    }
  }

  private void failSubmission(Throwable cause) {
    synchronized (lock) {
      if (failure == null) {
        failure = asIOException("Celeborn submission failed", cause);
      }
      state = State.ABORTED;
      lock.notifyAll();
    }
  }

  /** Waits for every push and native encoder before committing mapperEnd. */
  public long[] finish() throws IOException {
    try {
      synchronized (lock) {
        if (state == State.FINISHED) {
          return lengths();
        }
        checkOpen();
        state = State.FINISHING;
        while (state == State.FINISHING && (pending != 0 || submissions != 0 || encoders != 0)) {
          lock.wait();
        }
        if (failure != null) {
          throw failure;
        }
        if (state != State.FINISHING) {
          throw new IOException("Celeborn map attempt aborted");
        }
      }
      synchronized (submissionLock) {
        synchronized (lock) {
          if (state != State.FINISHING) {
            throw new IOException("Celeborn map attempt aborted");
          }
        }
        if (mapperEnd.getParameterCount() == 5) {
          mapperEnd.invoke(client, shuffleId, mapId, attemptId, numMappers, numPartitions);
        } else {
          mapperEnd.invoke(client, shuffleId, mapId, attemptId, numMappers);
        }
        synchronized (lock) {
          if (state != State.FINISHING) {
            throw new IOException("Celeborn map attempt aborted");
          }
          state = State.FINISHED;
        }
      }
      return lengths();
    } catch (InterruptedException cause) {
      Thread.currentThread().interrupt();
      IOException error = asIOException("Interrupted draining Celeborn pushes", cause);
      abortWithSuppression(error);
      throw error;
    } catch (ReflectiveOperationException cause) {
      IOException error =
          asIOException(
              "Celeborn mapperEnd failed",
              cause instanceof InvocationTargetException
                  ? ((InvocationTargetException) cause).getCause()
                  : cause);
      abortWithSuppression(error);
      throw error;
    } catch (IOException cause) {
      abortWithSuppression(cause);
      throw cause;
    }
  }

  private void abortWithSuppression(IOException original) {
    try {
      abort();
    } catch (IOException cancellation) {
      if (cancellation != original) {
        original.addSuppressed(cancellation);
      }
    }
  }

  public void abort() throws IOException {
    synchronized (lock) {
      if (state == State.FINISHED) {
        return;
      }
      state = State.ABORTED;
      lock.notifyAll();
    }
    cleanupIfReady();
  }

  private void cleanupIfReady() throws IOException {
    synchronized (lock) {
      if (state != State.ABORTED || submissions != 0 || cleanupClaimed) {
        return;
      }
      cleanupClaimed = true;
    }
    synchronized (submissionLock) {
      try {
        cleanup.invoke(client, shuffleId, mapId, attemptId);
      } catch (ReflectiveOperationException cause) {
        throw asIOException("Celeborn cleanup failed", cause);
      }
    }
  }

  private long[] lengths() {
    long[] lengths = new long[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      lengths[i] = partitionLengths.get(i);
    }
    return lengths;
  }

  private static IOException asIOException(String message, Throwable cause) {
    return cause instanceof IOException ? (IOException) cause : new IOException(message, cause);
  }
}
