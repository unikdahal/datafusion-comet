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
import java.util.{Arrays, Optional}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf

class CelebornShufflePartitionPusherSuite extends AnyFunSuite {
  private val encodedAttemptId = 7
  private def clientArguments: Array[AnyRef] = Array[AnyRef](
    "native-celeborn-application",
    "localhost",
    Int.box(9097),
    new RecordingCelebornClientConf,
    new RecordingCelebornUserIdentifier,
    Array[Byte](1, 2, 3))

  private def frame(): Array[Byte] = ByteBuffer
    .allocate(24)
    .order(ByteOrder.LITTLE_ENDIAN)
    .putLong(16L)
    .putLong(1L)
    .putLong(2L)
    .array()
  private def pusher(client: AnyRef, limit: Int = 512): CelebornShufflePartitionPusher =
    new CelebornShufflePartitionPusher(client, 19, 3, 7, 12, 9, 128, limit)

  test("Spark IO encryption is rejected before creating a native task pusher") {
    val client = new RecordingCelebornPushClient
    val conf = new SparkConf(false).set("spark.io.encryption.enabled", "true")
    val failure = intercept[IllegalArgumentException] {
      CelebornShufflePusherFactory.create(conf, client, 19, 12, 9, null)
    }
    assert(failure.getMessage.contains("Encrypted native Celeborn shuffle is not supported"))
    assert(client.pushCount == 0)
  }

  test("crypto-aware client acquisition preserves encryption for ordinary Spark shuffle") {
    val conf = new SparkConf(false).set("spark.io.encryption.enabled", "true")
    val cryptoHandler = new RecordingCelebornCryptoHandler
    val bytes = frame()
    RecordingCryptoAwareCelebornClientFactory.reset()
    var observedConf: SparkConf = null

    val client = CelebornShufflePusherFactory
      .resolveClient(
        conf,
        classOf[RecordingCryptoAwareCelebornClientFactory],
        classOf[RecordingCelebornClientConf],
        classOf[RecordingCelebornUserIdentifier],
        clientArguments,
        sparkConf => {
          observedConf = sparkConf
          Optional.of(cryptoHandler)
        })
      .asInstanceOf[RecordingCelebornPushClient]

    // The shared application client must keep its real crypto handler even though native RSS
    // cannot currently bound that handler's extra allocations and retained high-water buffer.
    client.pushOrMergeData(19, 3, encodedAttemptId, 6, bytes, 0, bytes.length, 12, 9, true, true)

    assert(observedConf eq conf)
    assert(RecordingCryptoAwareCelebornClientFactory.cryptoAwareCalls.get() == 1)
    assert(RecordingCryptoAwareCelebornClientFactory.legacyCalls.get() == 0)
    assert(cryptoHandler.encryptionCount == 1)
    assert(cryptoHandler.plaintext.sameElements(bytes))
    assert(cryptoHandler.encryptedLength == bytes.length + 20)
  }

  test("older Celeborn clients retain their six-argument client acquisition API") {
    val conf = new SparkConf(false)
    var cryptoHandlerResolved = false
    RecordingLegacyCelebornClientFactory.calls.set(0)

    val client = CelebornShufflePusherFactory.resolveClient(
      conf,
      classOf[RecordingLegacyCelebornClientFactory],
      classOf[RecordingCelebornClientConf],
      classOf[RecordingCelebornUserIdentifier],
      clientArguments,
      _ => {
        cryptoHandlerResolved = true
        Optional.empty[AnyRef]()
      })

    assert(client.isInstanceOf[RecordingCelebornPushClient])
    assert(RecordingLegacyCelebornClientFactory.calls.get() == 1)
    assert(!cryptoHandlerResolved)
  }

  test("crypto-handler failures never fall back to an unencrypted Celeborn client") {
    val conf = new SparkConf(false).set("spark.io.encryption.enabled", "true")
    val expected = new IllegalStateException("Spark shuffle encryption key was unavailable")
    RecordingCryptoAwareCelebornClientFactory.reset()

    val actual = intercept[IllegalStateException] {
      CelebornShufflePusherFactory.resolveClient(
        conf,
        classOf[RecordingCryptoAwareCelebornClientFactory],
        classOf[RecordingCelebornClientConf],
        classOf[RecordingCelebornUserIdentifier],
        clientArguments,
        _ => throw expected)
    }

    assert(actual eq expected)
    assert(RecordingCryptoAwareCelebornClientFactory.cryptoAwareCalls.get() == 0)
    assert(RecordingCryptoAwareCelebornClientFactory.legacyCalls.get() == 0)
  }

  test(
    "encryption enabled after binding is rejected before reservation or integrity accounting") {
    val client = new RecordingCelebornPushClient
    val cryptoHandler = new RecordingCelebornCryptoHandler
    val bytes = frame()
    val adapter = pusher(client)
    client.cryptoHandler = Optional.of(cryptoHandler)

    intercept[UnsupportedOperationException] {
      adapter.reservePartitionData(3 * bytes.length)
    }
    intercept[UnsupportedOperationException] {
      adapter.pushPartitionData(4, bytes, bytes.length)
    }

    assert(cryptoHandler.encryptionCount == 0)
    assert(client.pushCount == 0)
    assert(client.cryptoHandler.get() eq cryptoHandler)
  }

  test("public buffer push preserves task identity, complete frame and partition lengths") {
    val client = new RecordingCelebornPushClient
    val push = pusher(client)
    val bytes = frame()
    push.pushPartitionData(6, bytes, bytes.length)
    assert(client.lastPush.shuffleId == 19)
    assert(client.lastPush.mapId == 3)
    assert(client.lastPush.attemptId == 7)
    assert(client.lastPush.partitionId == 6)
    assert(client.lastPush.bytes.sameElements(bytes))
    assert(push.finish()(6) == 24)
    assert(client.mapperEndCalls.get() == 1)
    assert(push.finish()(6) == 24)
  }
  test("public submission preserves unchecked failures and cleans up its map attempt") {
    val client = new RecordingCelebornPushClient
    val expected = new IllegalStateException("submission rejected")
    client.failure = expected
    val push = pusher(client)
    val bytes = frame()
    assert(
      intercept[IllegalStateException](
        push.pushPartitionData(0, bytes, bytes.length)) eq expected)
    assert(client.cleanupCalls.get() == 1)
  }
  test("missing public contract falls back without modifying private transport state") {
    assert(
      CelebornShufflePartitionPusher.nativePushCompletionUnavailableReason(
        classOf[Object]) != null)
    intercept[UnsupportedOperationException](pusher(new Object))
    assert(
      CelebornShufflePartitionPusher.nativePushCompletionUnavailableReason(
        classOf[RecordingCelebornPushClient]) == null)
    assert(
      CelebornShufflePartitionPusher.nativePushCompletionUnavailableReason(
        classOf[BufferCompletionTestClient]) == null)
  }
  test("encryption fails before acquiring or submitting a frame") {
    val client = new RecordingCelebornPushClient
    client.cryptoHandler = Optional.of(new RecordingCelebornCryptoHandler)
    intercept[UnsupportedOperationException](pusher(client))
    assert(client.pushCount == 0)
  }
  test("malformed frames and oversized reservations cannot reach Celeborn") {
    val client = new RecordingCelebornPushClient
    val push = pusher(client)
    intercept[IOException](push.pushPartitionData(9, frame(), 24))
    intercept[IOException](push.pushPartitionData(0, frame(), 23))
    intercept[IOException](push.reservePartitionData(1000))
    assert(client.pushCount == 0)
  }
  test("transport completion cannot release a native encoder reservation early") {
    val client = new BufferCompletionTestClient
    val first = pusher(client, 64)
    val second = pusher(client, 64)
    first.reservePartitionData(48)
    first.pushPartitionData(0, frame(), 24)
    client.done.complete(Int.box(40))
    val admitted = new CountDownLatch(1)
    val worker = new Thread(() => {
      second.reservePartitionData(24)
      admitted.countDown()
      second.releasePartitionDataReservation()
    })
    worker.start()
    assert(!admitted.await(100, TimeUnit.MILLISECONDS))
    first.releasePartitionDataReservation()
    assert(admitted.await(5, TimeUnit.SECONDS))
    worker.join(5000)
    first.abort(); second.abort()
    ExecutorShufflePushAdmission.releaseClient(client)
  }
  test("abort keeps shared admission until the public lifetime completion retires") {
    val client = new BufferCompletionTestClient
    val first = pusher(client, 64)
    val second = pusher(client, 64)
    first.pushPartitionData(0, frame(), 24)
    first.abort()
    val admitted = new CountDownLatch(1)
    val worker = new Thread(() => {
      second.reservePartitionData(24)
      admitted.countDown()
      second.releasePartitionDataReservation()
    })
    worker.start()
    assert(!admitted.await(100, TimeUnit.MILLISECONDS))
    client.done.completeExceptionally(new IOException("cancelled after transport retirement"))
    assert(admitted.await(5, TimeUnit.SECONDS))
    worker.join(5000)
    first.abort(); second.abort()
    assert(client.cleanupCalls.get() == 2)
    ExecutorShufflePushAdmission.releaseClient(client)
  }
  test("duplicate submission cannot return another live frame's reservation") {
    val client = new BufferCompletionTestClient
    val first = pusher(client, 64)
    val second = pusher(client, 64)
    first.reservePartitionData(48)
    first.pushPartitionData(0, frame(), 24)
    intercept[IOException](first.pushPartitionData(0, frame(), 24))
    first.releasePartitionDataReservation()
    val admitted = new CountDownLatch(1)
    val worker = new Thread(() => {
      second.reservePartitionData(24)
      admitted.countDown()
      second.releasePartitionDataReservation()
    })
    worker.start()
    assert(!admitted.await(100, TimeUnit.MILLISECONDS))
    client.done.completeExceptionally(new IOException("cancelled after owners retired"))
    assert(admitted.await(5, TimeUnit.SECONDS))
    worker.join(5000)
    second.abort()
    ExecutorShufflePushAdmission.releaseClient(client)
  }
  test("mapperEnd failure preserves its cause when cleanup also fails") {
    val client = new RecordingCelebornPushClient
    val expected = new IOException("mapper commit failed")
    client.mapperEndFailure = expected
    client.cleanupFailure = new IOException("cleanup failed")
    val push = pusher(client)
    push.pushPartitionData(0, frame(), 24)
    assert(intercept[IOException](push.finish()) eq expected)
    assert(expected.getSuppressed.length == 1)
    assert(client.cleanupCalls.get() == 1)
    push.abort()
    assert(client.cleanupCalls.get() == 1)
  }
  test("native configuration negotiates one or two overlapping payload representations") {
    val directClient = new RecordingCelebornPushClient
    val direct = new CelebornShufflePartitionPusher(directClient, 19, 3, 7, 12, 9, 128, 40, true)
    assert(direct.supportsDirectPush())
    assert(direct.frameCopies() == 1)
    assert(direct.maxFrameBytes() == 24)
    direct.reservePartitionData(24)
    val bytes = ByteBuffer.allocateDirect(24).put(frame())
    bytes.flip()
    direct.pushPartitionDataDirect(0, bytes, 24)
    direct.releasePartitionDataReservation()
    assert(direct.finish()(0) == 24)
    val heap = pusher(new RecordingCelebornPushClient, 64)
    assert(!heap.supportsDirectPush())
    assert(heap.frameCopies() == 2)
    assert(heap.maxFrameBytes() == 24)
    heap.abort()
    ExecutorShufflePushAdmission.releaseClient(directClient)
  }
  test("direct JNI invocation stays borrowed until completion even after interruption") {
    val client = new BufferCompletionTestClient
    val push = pusher(client)
    val bytes = ByteBuffer.allocateDirect(24).put(frame())
    bytes.flip()
    val returned = new CountDownLatch(1)
    val worker = new Thread(() => {
      try push.pushPartitionDataDirect(0, bytes, 24)
      catch { case _: IOException => () }
      finally returned.countDown()
    })
    worker.start()
    assert(client.submitted.await(5, TimeUnit.SECONDS))
    worker.interrupt()
    assert(!returned.await(100, TimeUnit.MILLISECONDS))
    assert(client.cleanupCalls.get() == 1)
    client.done.completeExceptionally(new IOException("cancelled after owners retired"))
    assert(returned.await(5, TimeUnit.SECONDS))
    worker.join(5000)
  }
  test("finish waits for public completion before mapperEnd and propagates async failure") {
    val client = new BufferCompletionTestClient
    val push = pusher(client)
    push.pushPartitionData(0, frame(), 24)
    val finished = new CountDownLatch(1)
    val worker = new Thread(() => {
      try push.finish()
      catch { case _: IOException => () }
      finally finished.countDown()
    })
    worker.start()
    assert(!finished.await(100, TimeUnit.MILLISECONDS))
    assert(client.mapperEndCalls.get() == 0)
    client.done.completeExceptionally(new IOException("retry exhausted"))
    assert(finished.await(5, TimeUnit.SECONDS))
    worker.join(5000)
    assert(client.mapperEndCalls.get() == 0)
    assert(client.cleanupCalls.get() == 1)
  }
}

class BufferCompletionTestClient extends RecordingCelebornPushClient {
  val done = new java.util.concurrent.CompletableFuture[Integer]()
  val submitted = new CountDownLatch(1)
  override def pushDataAsync(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      data: ByteBuffer,
      numMappers: Int,
      numPartitions: Int): java.util.concurrent.CompletableFuture[Integer] = {
    submitted.countDown(); done
  }
}

/** Stand-ins for optional Celeborn types used by reflective client-acquisition tests. */
final class RecordingCelebornClientConf
final class RecordingCelebornUserIdentifier

/** Mirrors Celeborn 0.7's modern and legacy static client-acquisition overloads. */
final class RecordingCryptoAwareCelebornClientFactory

object RecordingCryptoAwareCelebornClientFactory {
  val cryptoAwareCalls: AtomicInteger = new AtomicInteger()
  val legacyCalls: AtomicInteger = new AtomicInteger()

  def reset(): Unit = {
    cryptoAwareCalls.set(0)
    legacyCalls.set(0)
  }

  def get(
      appUniqueId: String,
      lifecycleManagerHost: String,
      lifecycleManagerPort: Int,
      conf: RecordingCelebornClientConf,
      userIdentifier: RecordingCelebornUserIdentifier,
      extension: Array[Byte],
      cryptoHandler: Optional[_]): RecordingCelebornPushClient = {
    cryptoAwareCalls.incrementAndGet()
    val client = new RecordingCelebornPushClient
    if (cryptoHandler.isPresent) {
      client.cryptoHandler =
        Optional.of(cryptoHandler.get().asInstanceOf[RecordingCelebornCryptoHandler])
    }
    client
  }

  def get(
      appUniqueId: String,
      lifecycleManagerHost: String,
      lifecycleManagerPort: Int,
      conf: RecordingCelebornClientConf,
      userIdentifier: RecordingCelebornUserIdentifier,
      extension: Array[Byte]): RecordingCelebornPushClient = {
    legacyCalls.incrementAndGet()
    new RecordingCelebornPushClient
  }
}

/** Mirrors Celeborn 0.6's original static client-acquisition API. */
final class RecordingLegacyCelebornClientFactory

object RecordingLegacyCelebornClientFactory {
  val calls: AtomicInteger = new AtomicInteger()

  def get(
      appUniqueId: String,
      lifecycleManagerHost: String,
      lifecycleManagerPort: Int,
      conf: RecordingCelebornClientConf,
      userIdentifier: RecordingCelebornUserIdentifier,
      extension: Array[Byte]): RecordingCelebornPushClient = {
    calls.incrementAndGet()
    new RecordingCelebornPushClient
  }
}

final case class RecordedCelebornPush(
    shuffleId: Int,
    mapId: Int,
    attemptId: Int,
    partitionId: Int,
    bytes: Array[Byte],
    offset: Int,
    length: Int,
    numMappers: Int,
    numPartitions: Int,
    doPush: Boolean,
    skipCompress: Boolean)

/** Public so the adapter can resolve and invoke the optional client's API using reflection. */
class RecordingCelebornPushClient {
  def supportsBufferPush(): Boolean = !cryptoHandler.isPresent
  def pushDataAsync(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      data: ByteBuffer,
      numMappers: Int,
      numPartitions: Int): java.util.concurrent.CompletionStage[Integer] = {
    val bytes = new Array[Byte](data.remaining())
    data.duplicate().get(bytes)
    java.util.concurrent.CompletableFuture.completedFuture(
      Int.box(
        pushOrMergeData(
          shuffleId,
          mapId,
          attemptId,
          partitionId,
          bytes,
          0,
          bytes.length,
          numMappers,
          numPartitions,
          true,
          true)))
  }

  val mapperEndCalls: AtomicInteger = new AtomicInteger()
  val cleanupCalls: AtomicInteger = new AtomicInteger()
  @volatile var acceptedBytes: Option[Int] = None
  @volatile var cryptoHandler: Optional[RecordingCelebornCryptoHandler] = Optional.empty()
  @volatile var failure: Throwable = _
  @volatile var mapperEndFailure: Throwable = _
  @volatile var cleanupFailure: Throwable = _
  @volatile var lastMapperEnd: (Int, Int, Int, Int, Int) = _
  @volatile var lastCleanup: (Int, Int, Int) = _
  @volatile var lastPush: RecordedCelebornPush = _
  @volatile var pushCount: Int = 0

  @throws[IOException]
  def pushOrMergeData(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      bytes: Array[Byte],
      offset: Int,
      length: Int,
      numMappers: Int,
      numPartitions: Int,
      doPush: Boolean,
      skipCompress: Boolean): Int = {
    pushCount += 1
    lastPush = RecordedCelebornPush(
      shuffleId,
      mapId,
      attemptId,
      partitionId,
      bytes,
      offset,
      length,
      numMappers,
      numPartitions,
      doPush,
      skipCompress)

    if (failure != null) {
      throw failure
    }
    val transportPayloadLength =
      if (cryptoHandler.isPresent) cryptoHandler.get().encrypt(bytes, offset, length).length
      else length
    acceptedBytes.getOrElse(transportPayloadLength + 16)
  }

  @throws[IOException]
  def mapperEnd(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      numMappers: Int,
      numPartitions: Int): Unit = {
    mapperEndCalls.incrementAndGet()
    lastMapperEnd = (shuffleId, mapId, attemptId, numMappers, numPartitions)
    if (mapperEndFailure != null) {
      throw mapperEndFailure
    }
  }

  @throws[IOException]
  def cleanup(shuffleId: Int, mapId: Int, attemptId: Int): Unit = {
    cleanupCalls.incrementAndGet()
    lastCleanup = (shuffleId, mapId, attemptId)
    if (cleanupFailure != null) {
      throw cleanupFailure
    }
  }
}

final class RecordingCelebornCryptoHandler {

  @volatile var encryptionCount: Int = 0
  @volatile var plaintext: Array[Byte] = _
  @volatile var encryptedLength: Int = 0

  def encrypt(bytes: Array[Byte], offset: Int, length: Int): Array[Byte] = {
    encryptionCount += 1
    plaintext = Arrays.copyOfRange(bytes, offset, offset + length)
    encryptedLength = length + java.lang.Integer.BYTES + 16
    new Array[Byte](encryptedLength)
  }
}

/** Mirrors Celeborn 0.7 integrity accounting without depending on its optional client classes. */
