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
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

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
