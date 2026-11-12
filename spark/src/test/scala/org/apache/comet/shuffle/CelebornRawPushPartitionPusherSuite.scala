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

package org.apache.comet.shuffle

import java.io.IOException
import java.nio.{ByteBuffer, ByteOrder}
import java.util.Optional
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite

class CelebornRawPushPartitionPusherSuite extends AnyFunSuite {

  private def frame(bodyLength: Int = java.lang.Long.BYTES): Array[Byte] = {
    val buffer = ByteBuffer
      .allocate(java.lang.Long.BYTES + bodyLength)
      .order(ByteOrder.LITTLE_ENDIAN)
      .putLong(bodyLength.toLong)
    (0 until bodyLength).foreach(index => buffer.put((index + 1).toByte))
    buffer.array()
  }

  private def pusher(
      client: AnyRef,
      maxInFlightBytes: Int = 1 << 20,
      nativeFrames: Boolean = true): CelebornRawPushPartitionPusher =
    new CelebornRawPushPartitionPusher(
      client,
      19,
      3,
      7,
      12,
      9,
      Int.MaxValue - 16,
      maxInFlightBytes,
      nativeFrames)

  test("only clients that implement pushRawData are accepted") {
    assert(
      CelebornRawPushPartitionPusher
        .unavailableReason(classOf[RecordingCelebornPushClient])
        .contains("pushRawData"))
    assert(
      CelebornRawPushPartitionPusher.unavailableReason(
        classOf[RecordingCelebornRawPushClient]) == null)
    intercept[UnsupportedOperationException] {
      pusher(new RecordingCelebornPushClient)
    }
  }

  test("encrypted raw-push clients are rejected before reserving or pushing frames") {
    val client = new RecordingCelebornRawPushClient
    val handler = new RecordingCelebornCryptoHandler
    client.cryptoHandler = Optional.of(handler)
    intercept[UnsupportedOperationException] {
      pusher(client)
    }

    client.cryptoHandler = Optional.empty()
    val adapter = pusher(client, maxInFlightBytes = 1024)
    client.cryptoHandler = Optional.of(handler)
    intercept[UnsupportedOperationException] {
      adapter.reservePartitionData(512)
    }

    client.cryptoHandler = Optional.empty()
    adapter.reservePartitionData(512)
    client.cryptoHandler = Optional.of(handler)
    try {
      val bytes = frame()
      intercept[UnsupportedOperationException] {
        adapter.pushPartitionData(0, bytes, bytes.length)
      }
    } finally {
      adapter.releasePartitionDataReservation()
    }

    assert(client.pushes.isEmpty)
    assert(handler.encryptionCount == 0)
    assert(client.cryptoHandler.get() eq handler)
    client.cryptoHandler = Optional.empty()
    val after = pusher(client, maxInFlightBytes = 1024)
    after.reservePartitionData(1024)
    after.releasePartitionDataReservation()
    after.abort()
  }

  test("a frame exceeding its encoding reservation fails without waiting on its own bytes") {
    val client = new RecordingCelebornRawPushClient
    val adapter = pusher(client, maxInFlightBytes = 64)
    val failure = new AtomicReference[Throwable]()
    val done = new CountDownLatch(1)
    val pushing = new Thread(() => {
      try {
        adapter.reservePartitionData(32)
        val bytes = frame(56)
        adapter.pushPartitionData(0, bytes, bytes.length)
      } catch {
        case error: Throwable => failure.set(error)
      } finally {
        adapter.releasePartitionDataReservation()
        done.countDown()
      }
    })
    pushing.setDaemon(true)
    pushing.start()
    try {
      assert(done.await(5, TimeUnit.SECONDS), "a frame must fit its existing reservation")
      assert(failure.get().isInstanceOf[IOException])
      assert(failure.get().getMessage.contains("exceeds its encoding reservation"))
      assert(client.pushes.isEmpty)
    } finally {
      adapter.abort()
      pushing.join(5000)
    }

    val after = pusher(client, maxInFlightBytes = 64)
    after.reservePartitionData(64)
    after.releasePartitionDataReservation()
    after.abort()
  }

  test("abort interrupts mapperEnd and waits for it to return before cleanup") {
    val client = new BlockingRecordingCelebornRawPushClient
    val adapter = pusher(client)
    val failure = new AtomicReference[Throwable]()
    val interruptLeaked = new AtomicBoolean()
    val finishing = new Thread(() => {
      try adapter.finish()
      catch {
        case error: Throwable => failure.set(error)
      } finally {
        interruptLeaked.set(Thread.currentThread().isInterrupted)
      }
    })
    finishing.setDaemon(true)
    finishing.start()
    try {
      assert(client.entered.await(5, TimeUnit.SECONDS))
      adapter.abort()
      assert(client.interrupted.await(5, TimeUnit.SECONDS))
      assert(client.cleanupCalls.get() == 0, "cleanup must wait until mapperEnd returns")
    } finally {
      adapter.abort()
      client.allowExit.countDown()
      finishing.join(5000)
    }
    assert(!finishing.isAlive)
    assert(failure.get().isInstanceOf[IOException])
    assert(!interruptLeaked.get())
    assert(client.cleanupCalls.get() == 1)
    adapter.abort()
    assert(client.cleanupCalls.get() == 1)
  }

  test("pushed bytes stay admitted until Celeborn releases them") {
    val client = new RecordingCelebornRawPushClient
    client.deferReleases = true
    val first = pusher(client, maxInFlightBytes = 256)
    val second = pusher(client, maxInFlightBytes = 256)

    first.reservePartitionData(200)
    val bytes = frame(56)
    first.pushPartitionData(4, bytes, bytes.length)
    first.releasePartitionDataReservation()
    assert(client.pushes.size == 1)
    assert(client.pushes.peek().partitionId == 4)
    assert(client.pushes.peek().bytes.sameElements(bytes))

    // 64 bytes are still referenced by Celeborn, so only 192 of 256 bytes are available.
    val admitted = new CountDownLatch(1)
    val waiter = new Thread(() => {
      second.reservePartitionData(256)
      admitted.countDown()
      second.releasePartitionDataReservation()
    })
    waiter.start()
    assert(!admitted.await(200, TimeUnit.MILLISECONDS))
    client.releaseAll()
    assert(admitted.await(5, TimeUnit.SECONDS))
    waiter.join()
    assert(first.finish().toSeq == Seq(0L, 0L, 0L, 0L, bytes.length.toLong, 0L, 0L, 0L, 0L))
    second.abort()
  }

  test("every frame is released exactly once whatever the push outcome") {
    val client = new RecordingCelebornRawPushClient
    val limit = 1024
    val rejected = pusher(client, maxInFlightBytes = limit)

    // An invalid frame never reaches Celeborn, but its admission is still returned.
    rejected.reservePartitionData(512)
    intercept[IOException] {
      rejected.pushPartitionData(0, Array.fill[Byte](32)(1), 32)
    }
    rejected.releasePartitionDataReservation()
    assert(client.pushes.isEmpty)

    // A failing client still runs the release callback, as its contract requires.
    val failing = pusher(client, maxInFlightBytes = limit)
    client.failure = new IOException("push failed")
    failing.reservePartitionData(512)
    val bytes = frame()
    val failure = intercept[IOException] {
      failing.pushPartitionData(0, bytes, bytes.length)
    }
    failing.releasePartitionDataReservation()
    assert(failure eq client.failure)
    assert(client.releases.get() == 1)
    assert(client.cleanupCalls.get() == 2)
    client.failure = null

    // A mapper that already ended accepts nothing, which fails the attempt.
    val ended = pusher(client, maxInFlightBytes = limit)
    client.acceptNothing = true
    ended.reservePartitionData(512)
    intercept[IOException] {
      ended.pushPartitionData(0, bytes, bytes.length)
    }
    ended.releasePartitionDataReservation()
    assert(client.releases.get() == 2)

    // All admission was returned: the full limit can be reserved again.
    val after = pusher(client, maxInFlightBytes = limit)
    after.reservePartitionData(limit)
    after.releasePartitionDataReservation()
    after.abort()
  }

  test("abort interrupts a push blocked inside the client") {
    val client = new RecordingCelebornRawPushClient
    val entered = new CountDownLatch(1)
    client.blockUntilInterrupted = entered
    val blocked = pusher(client)
    val failure = new java.util.concurrent.atomic.AtomicReference[Throwable]()
    val pushing = new Thread(() => {
      try {
        blocked.reservePartitionData(1024)
        val bytes = frame()
        blocked.pushPartitionData(0, bytes, bytes.length)
      } catch {
        case error: Throwable => failure.set(error)
      } finally {
        blocked.releasePartitionDataReservation()
      }
    })
    pushing.start()
    assert(entered.await(5, TimeUnit.SECONDS))
    val start = System.nanoTime()
    blocked.abort()
    pushing.join(5000)
    assert(!pushing.isAlive, "an aborted push must not stay blocked in the client")
    assert(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 5000)
    assert(failure.get().isInstanceOf[IOException])
    assert(client.releases.get() == 1)
    assert(client.cleanupCalls.get() == 1)
  }

  test("push failures preserve the cause when deferred cleanup also fails") {
    val client = new RecordingCelebornRawPushClient
    val expected = new IOException("push failed")
    val cleanupFailure = new IOException("cleanup failed")
    client.failure = expected
    client.cleanupFailure = cleanupFailure
    val adapter = pusher(client)
    adapter.reservePartitionData(512)

    try {
      val bytes = frame()
      val actual = intercept[IOException] {
        adapter.pushPartitionData(0, bytes, bytes.length)
      }
      assert(actual eq expected)
      assert(actual.getSuppressed.toSeq == Seq(cleanupFailure))
      assert(client.releases.get() == 1)
      assert(client.cleanupCalls.get() == 1)
    } finally {
      adapter.releasePartitionDataReservation()
    }
  }

  test("mapperEnd failures preserve the cause when deferred cleanup also fails") {
    val client = new RecordingCelebornRawPushClient
    val expected = new IOException("mapperEnd failed")
    val cleanupFailure = new IOException("cleanup failed")
    client.mapperEndFailure = expected
    client.cleanupFailure = cleanupFailure
    val adapter = pusher(client)

    val actual = intercept[IOException] {
      adapter.finish()
    }
    assert(actual eq expected)
    assert(actual.getSuppressed.toSeq == Seq(cleanupFailure))
    assert(client.cleanupCalls.get() == 1)
  }

  test("finish commits through mapperEnd and abort cleans up") {
    val client = new RecordingCelebornRawPushClient
    val committed = pusher(client)
    val bytes = frame(24)
    committed.reservePartitionData(1024)
    committed.pushPartitionData(2, bytes, bytes.length)
    committed.releasePartitionDataReservation()
    assert(committed.finish()(2) == bytes.length)
    assert(client.mapperEndCalls.get() == 1)
    assert(client.lastMapperEnd == ((19, 3, 7, 12, 9)))
    assert(client.cleanupCalls.get() == 0)

    val aborted = pusher(client)
    aborted.abort()
    assert(client.cleanupCalls.get() == 1)
    intercept[IOException] {
      aborted.reservePartitionData(64)
    }
  }
}

/** Holds mapperEnd until cancellation, then delays its exit to check cleanup ordering. */
class BlockingRecordingCelebornRawPushClient extends RecordingCelebornRawPushClient {
  val entered = new CountDownLatch(1)
  val interrupted = new CountDownLatch(1)
  val allowExit = new CountDownLatch(1)

  @throws[IOException]
  override def mapperEnd(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      numMappers: Int,
      numPartitions: Int): Unit = {
    entered.countDown()
    try {
      allowExit.await()
    } catch {
      case cause: InterruptedException =>
        interrupted.countDown()
        try {
          if (!allowExit.await(5, TimeUnit.SECONDS)) {
            throw new IOException("mapperEnd was not allowed to exit")
          }
        } finally {
          Thread.currentThread().interrupt()
        }
        throw new IOException("mapperEnd interrupted", cause)
    }
  }
}

/** A client exposing the caller-owned push API, recording each push and its release. */
class RecordingCelebornRawPushClient extends RecordingCelebornPushClient {
  val pushes: ConcurrentLinkedQueue[RecordedRawPush] =
    new ConcurrentLinkedQueue[RecordedRawPush]()
  val releases: AtomicInteger = new AtomicInteger()
  private val pendingReleases = new ConcurrentLinkedQueue[Runnable]()
  @volatile var deferReleases: Boolean = false
  @volatile var acceptNothing: Boolean = false
  @volatile var blockUntilInterrupted: CountDownLatch = _

  @throws[IOException]
  def pushRawData(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      data: ByteBuffer,
      numMappers: Int,
      numPartitions: Int,
      releaseCallback: Runnable): Int = {
    val release: Runnable = () => {
      releases.incrementAndGet()
      releaseCallback.run()
    }
    var interrupted = false
    try {
      if (blockUntilInterrupted != null) {
        // Like Celeborn waiting for in-flight capacity, which stops when interrupted.
        blockUntilInterrupted.countDown()
        try Thread.sleep(60000)
        catch {
          case _: InterruptedException =>
            interrupted = true
            throw new IOException("interrupted while waiting for in-flight capacity")
        }
      }
      if (failure != null) {
        throw failure
      }
      if (acceptNothing) {
        return 0
      }
      val bytes = new Array[Byte](data.remaining())
      data.duplicate().get(bytes)
      pushes.add(RecordedRawPush(partitionId, bytes, data.isDirect))
      if (deferReleases) {
        pendingReleases.add(release)
        return bytes.length + 16
      }
      bytes.length + 16
    } finally {
      if (!deferReleases || failure != null || acceptNothing || interrupted) {
        release.run()
      }
    }
  }

  /** Runs the release callbacks that a transport would run once its writes finish. */
  def releaseAll(): Unit = {
    var release = pendingReleases.poll()
    while (release != null) {
      release.run()
      release = pendingReleases.poll()
    }
  }

  def pushedPartitions: Seq[Int] = pushes.asScala.map(_.partitionId).toSeq
}

final case class RecordedRawPush(partitionId: Int, bytes: Array[Byte], direct: Boolean)
