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
import java.util.{ArrayList => JArrayList, Arrays, List => JList, Optional}
import java.util.concurrent.{AbstractExecutorService, ConcurrentHashMap, CountDownLatch, ExecutorService, LinkedBlockingQueue, RejectedExecutionException, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference, LongAdder}
import java.util.zip.CRC32

import scala.collection.mutable

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf

class CelebornShufflePartitionPusherSuite extends AnyFunSuite {
  private def frame(): Array[Byte] = ByteBuffer.allocate(24).order(ByteOrder.LITTLE_ENDIAN)
    .putLong(16L).putLong(1L).putLong(2L).array()
  private def pusher(client: AnyRef, limit: Int = 512): CelebornShufflePartitionPusher =
    new CelebornShufflePartitionPusher(client, 19, 3, 7, 12, 9, 128, limit)

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
  test("missing public contract falls back without modifying private transport state") {
    assert(CelebornShufflePartitionPusher.nativePushCompletionUnavailableReason(classOf[Object]) != null)
    intercept[UnsupportedOperationException](pusher(new Object))
    assert(CelebornShufflePartitionPusher.nativePushCompletionUnavailableReason(
      classOf[RecordingCelebornPushClient]) == null)
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
    val worker = new Thread(() => { try push.finish() catch { case _: IOException => () }
      finally finished.countDown() })
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
  override def pushDataAsync(shuffleId: Int, mapId: Int, attemptId: Int, partitionId: Int,
      data: ByteBuffer, numMappers: Int, numPartitions: Int): java.util.concurrent.CompletionStage[Integer] = {
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

/** Public so the adapter can resolve and invoke the optional client's API using reflection. */
class RecordingCelebornPushClient {
  def supportsBufferPush(): Boolean = !cryptoHandler.isPresent
  def pushDataAsync(shuffleId: Int, mapId: Int, attemptId: Int, partitionId: Int,
      data: ByteBuffer, numMappers: Int, numPartitions: Int): java.util.concurrent.CompletionStage[Integer] = {
    val bytes = new Array[Byte](data.remaining())
    data.duplicate().get(bytes)
    java.util.concurrent.CompletableFuture.completedFuture(Int.box(pushOrMergeData(shuffleId,
      mapId, attemptId, partitionId, bytes, 0, bytes.length, numMappers, numPartitions, true, true)))
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

/** Mirrors stock Celeborn's private request tracker without requiring its optional dependency. */
final class RecordingCelebornInFlightTracker {
  val totalInflightReqs: LongAdder = new LongAdder()
}

/** Mirrors the public PushState failure slot and its stock private tracker member. */
final class RecordingCelebornPushState {
  val inFlightRequestTracker: RecordingCelebornInFlightTracker =
    new RecordingCelebornInFlightTracker()
  val exception: AtomicReference[IOException] = new AtomicReference[IOException]()
}

/** Exposes the same lifecycle and completion state as the public Apache Celeborn client. */
class AsyncRecordingCelebornPushClient extends RecordingCelebornPushClient {
  val pushStates: ConcurrentHashMap[String, RecordingCelebornPushState] =
    new ConcurrentHashMap[String, RecordingCelebornPushState]()

  def getPushState(mapKey: String): RecordingCelebornPushState =
    pushStates.computeIfAbsent(mapKey, _ => new RecordingCelebornPushState())

  def currentState(shuffleId: Int, mapId: Int, attemptId: Int): RecordingCelebornPushState =
    pushStates.get(s"$shuffleId-$mapId-$attemptId")

  def complete(state: RecordingCelebornPushState): Unit = state.synchronized {
    state.inFlightRequestTracker.totalInflightReqs.decrement()
    state.notifyAll()
  }

  def failWithoutRemovingRequest(state: RecordingCelebornPushState, failure: IOException): Unit =
    state.synchronized {
      state.exception.compareAndSet(null, failure)
      state.notifyAll()
    }

  @throws[IOException]
  override def pushOrMergeData(
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
    val state = getPushState(s"$shuffleId-$mapId-$attemptId")
    state.inFlightRequestTracker.totalInflightReqs.increment()
    super.pushOrMergeData(
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
  }

  @throws[IOException]
  override def mapperEnd(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      numMappers: Int,
      numPartitions: Int): Unit = {
    super.mapperEnd(shuffleId, mapId, attemptId, numMappers, numPartitions)
    val key = s"$shuffleId-$mapId-$attemptId"
    val state = pushStates.get(key)
    if (state != null) {
      state.synchronized {
        while (state.exception.get() == null &&
          state.inFlightRequestTracker.totalInflightReqs.sum() > 0) {
          state.wait(25)
        }
        val failure = state.exception.get()
        if (failure != null) {
          throw failure
        }
      }
      pushStates.remove(key, state)
    }
  }

  @throws[IOException]
  override def cleanup(shuffleId: Int, mapId: Int, attemptId: Int): Unit = {
    super.cleanup(shuffleId, mapId, attemptId)
    val removed = pushStates.remove(s"$shuffleId-$mapId-$attemptId")
    if (removed != null) {
      removed.synchronized {
        removed.exception.compareAndSet(null, new IOException("Cleaned Up"))
        removed.notifyAll()
      }
    }
  }
}

/** Models the final references in released clients without starting Celeborn network services. */
final class FinalFieldsRecordingCelebornPushClient extends RecordingCelebornPushClient {
  val factory = new FinalFieldsRecordingCelebornFactory
  val pushDataRetryPool: ExecutorService = new RecordingCelebornRetryExecutor
  val factoryCalls = new AtomicInteger()

  def getDataClientFactory: FinalFieldsRecordingCelebornFactory = {
    factoryCalls.incrementAndGet()
    factory
  }
}

final class FinalFieldsRecordingCelebornFactory {
  val clientBootstraps: JList[RecordingCelebornTransportClientBootstrap] =
    new JArrayList[RecordingCelebornTransportClientBootstrap]()
}

/** Models completion boundaries with safely published hooks, unlike stock Celeborn 0.6/0.7. */
final class TransportRecordingCelebornPushClient extends AsyncRecordingCelebornPushClient {
  val dataClientFactory: RecordingCelebornTransportClientFactory =
    new RecordingCelebornTransportClientFactory
  val retryExecutor = new RecordingCelebornRetryExecutor
  @volatile var pushDataRetryPool: ExecutorService = retryExecutor

  def getDataClientFactory: RecordingCelebornTransportClientFactory = dataClientFactory

  var openConnectionBeforePush: Boolean = false
  var openUninstrumentableConnectionBeforePush: Boolean = false
  var beforePushBegins: () => Unit = () => ()
  var beforePushReturns: RecordingCelebornPushState => Unit = (_: RecordingCelebornPushState) =>
    ()
  var retriesBeforeFailure: Int = 0
  var retryCallback: RecordingCelebornTransportCallbackApi => Unit =
    callback => callback.onFailure(new IOException("revive failed"))

  @throws[IOException]
  override def pushOrMergeData(
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
    beforePushBegins()
    val accepted = super.pushOrMergeData(
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
    if (openConnectionBeforePush) {
      openConnectionBeforePush = false
      dataClientFactory.openConnection()
    }
    if (openUninstrumentableConnectionBeforePush) {
      openUninstrumentableConnectionBeforePush = false
      dataClientFactory.openUninstrumentableConnection()
    }
    val state = currentState(shuffleId, mapId, attemptId)
    dataClientFactory.handler.add(
      state,
      new RecordingCelebornTransportCallback(
        state,
        retriesBeforeFailure,
        callback => {
          pushDataRetryPool.submit(new Runnable {
            override def run(): Unit = retryCallback(callback)
          })
          ()
        }))
    beforePushReturns(state)
    accepted
  }

  override def complete(state: RecordingCelebornPushState): Unit = {
    dataClientFactory.handler.remove(state).callback.onSuccess(ByteBuffer.allocate(0))
  }

  def failTransport(state: RecordingCelebornPushState, failure: IOException): Unit = {
    val request = dataClientFactory.handler.remove(state)
    request.callback.onFailure(failure)
  }
}

final class RecordingCelebornRetryExecutor extends AbstractExecutorService {
  private val pending = new LinkedBlockingQueue[Runnable]()
  @volatile private var stopped = false

  def pendingCount: Int = pending.size()

  def runNext(): Unit = {
    val task = pending.poll(5, TimeUnit.SECONDS)
    require(task != null, "Expected a queued Celeborn retry")
    task.run()
  }

  override def execute(command: Runnable): Unit = {
    if (stopped) {
      throw new RejectedExecutionException("Celeborn retry executor is stopped")
    }
    pending.add(command)
  }

  override def shutdown(): Unit = stopped = true

  override def shutdownNow(): JList[Runnable] = {
    stopped = true
    val tasks = new JArrayList[Runnable]()
    pending.drainTo(tasks)
    tasks
  }

  override def isShutdown: Boolean = stopped

  override def isTerminated: Boolean = stopped && pending.isEmpty

  override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = isTerminated
}

final class RecordingCelebornTransportClientFactory {
  var handler: RecordingCelebornTransportResponseHandler =
    new RecordingCelebornTransportResponseHandler
  @volatile var clientBootstraps: JList[RecordingCelebornTransportClientBootstrap] =
    new JArrayList[RecordingCelebornTransportClientBootstrap]()
  val connectionPool: ConcurrentHashMap[String, RecordingCelebornTransportClientPool] =
    new ConcurrentHashMap[String, RecordingCelebornTransportClientPool]()
  connectionPool.put("worker", new RecordingCelebornTransportClientPool(handler))

  def openConnection(): Unit = {
    openConnection(new io.netty.channel.embedded.EmbeddedChannel())
  }

  def openUninstrumentableConnection(): Unit = {
    openConnection(null)
  }

  private def openConnection(channel: io.netty.channel.Channel): Unit = {
    handler = new RecordingCelebornTransportResponseHandler
    val pool = new RecordingCelebornTransportClientPool(handler, channel)
    val bootstraps = clientBootstraps.iterator()
    while (bootstraps.hasNext) {
      bootstraps.next().doBootstrap(pool.clients(0))
    }
    connectionPool.put("worker", pool)
  }
}

trait RecordingCelebornTransportClientBootstrap {
  def doBootstrap(client: RecordingCelebornTransportClient): Unit
}

final class RecordingCelebornTransportClientPool(
    handler: RecordingCelebornTransportResponseHandler,
    channel: io.netty.channel.Channel = new io.netty.channel.embedded.EmbeddedChannel()) {
  val clients: Array[RecordingCelebornTransportClient] =
    Array(new RecordingCelebornTransportClient(handler, channel))
  val locks: Array[Object] = Array(new Object)
}

final class RecordingCelebornTransportClient(
    handler: RecordingCelebornTransportResponseHandler,
    @volatile var channel: io.netty.channel.Channel) {
  def getChannel: io.netty.channel.Channel = channel
  def getHandler: RecordingCelebornTransportResponseHandler = handler
}

final class RecordingCelebornTransportResponseHandler {
  private val nextRequestId = new AtomicLong()
  @volatile var outstandingPushes
      : ConcurrentHashMap[java.lang.Long, RecordingCelebornTransportRequest] =
    new ConcurrentHashMap[java.lang.Long, RecordingCelebornTransportRequest]()

  def add(pushState: RecordingCelebornPushState): Unit =
    add(pushState, new RecordingCelebornTransportCallback(pushState))

  def add(
      pushState: RecordingCelebornPushState,
      callback: RecordingCelebornTransportCallbackApi): Unit = {
    outstandingPushes.put(
      Long.box(nextRequestId.incrementAndGet()),
      new RecordingCelebornTransportRequest(pushState, callback))
  }

  def remove(pushState: RecordingCelebornPushState): RecordingCelebornTransportRequest = {
    val entries = outstandingPushes.entrySet().iterator()
    while (entries.hasNext) {
      val entry = entries.next()
      if (entry.getValue.pushState eq pushState) {
        val removed = outstandingPushes.remove(entry.getKey)
        if (removed != null) {
          return removed
        }
      }
    }
    throw new IllegalStateException("Celeborn transport request is no longer outstanding")
  }
}

final class RecordingCelebornTransportRequest(
    val pushState: RecordingCelebornPushState,
    var callback: RecordingCelebornTransportCallbackApi)

trait RecordingCelebornTransportCallbackApi {
  def onSuccess(response: ByteBuffer): Unit
  def onFailure(failure: Throwable): Unit
}

final class RecordingCelebornTransportCallback(
    val pushState: RecordingCelebornPushState,
    private var retriesRemaining: Int = 0,
    submitRetry: RecordingCelebornTransportCallbackApi => Unit = _ => ())
    extends RecordingCelebornTransportCallbackApi {
  override def onSuccess(response: ByteBuffer): Unit = pushState.synchronized {
    pushState.inFlightRequestTracker.totalInflightReqs.decrement()
    pushState.notifyAll()
  }

  override def onFailure(failure: Throwable): Unit = {
    if (pushState.exception.get() == null) {
      if (retriesRemaining > 0) {
        retriesRemaining -= 1
        submitRetry(this)
      } else {
        val reportedFailure = failure match {
          case io: IOException => io
          case _ => new IOException(failure)
        }
        pushState.exception.compareAndSet(null, reportedFailure)
      }
    }
  }
}

/** Implements the older public Celeborn 0.6 four-argument mapper-completion API. */
final class LegacyMapperEndCelebornPushClient {
  private val delegate = new RecordingCelebornPushClient
  val mapperEndCalls: AtomicInteger = new AtomicInteger()
  @volatile var lastMapperEnd: (Int, Int, Int, Int) = _

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
      skipCompress: Boolean): Int =
    delegate.pushOrMergeData(
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

  def mapperEnd(shuffleId: Int, mapId: Int, attemptId: Int, numMappers: Int): Unit = {
    mapperEndCalls.incrementAndGet()
    lastMapperEnd = (shuffleId, mapId, attemptId, numMappers)
  }

  def cleanup(shuffleId: Int, mapId: Int, attemptId: Int): Unit = ()
}

/** Models the Spark crypto wire format's minimum 4-byte length plus 16-byte IV overhead. */
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
final class IntegrityCheckingCelebornPushClient extends RecordingCelebornPushClient {

  @volatile var integrityFailure: Throwable = _
  val accountedFrames: mutable.ArrayBuffer[RecordedCelebornAccounting] =
    mutable.ArrayBuffer.empty
  val recordedPushes: mutable.ArrayBuffer[RecordedCelebornPush] = mutable.ArrayBuffer.empty
  val invocationOrder: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty
  private val checksums = mutable.HashMap.empty[Int, Long]
  private val byteTotals = mutable.HashMap.empty[Int, Long]

  def partitionCrc(partitionId: Int): Long = checksums(partitionId)

  def partitionBytes(partitionId: Int): Long = byteTotals(partitionId)

  @throws[IOException]
  def computeBatchCRC(
      shuffleId: Int,
      mapId: Int,
      attemptId: Int,
      partitionId: Int,
      bytes: Array[Byte],
      offset: Int,
      length: Int): Unit = {
    invocationOrder += s"crc:$partitionId"
    if (integrityFailure != null) {
      throw integrityFailure
    }

    accountedFrames +=
      RecordedCelebornAccounting(shuffleId, mapId, attemptId, partitionId, bytes, offset, length)
    val batchChecksum = new CRC32
    batchChecksum.update(bytes, offset, length)
    val previous = checksums.getOrElse(partitionId, 0L)
    val combined = (0 until java.lang.Integer.BYTES).foldLeft(0L) { (result, index) =>
      val shift = index * java.lang.Byte.SIZE
      val next = ((previous >>> shift) & 0xffL) + ((batchChecksum.getValue >>> shift) & 0xffL)
      result | ((next & 0xffL) << shift)
    }
    checksums.update(partitionId, combined)
    byteTotals.update(partitionId, byteTotals.getOrElse(partitionId, 0L) + length)
  }

  @throws[IOException]
  override def pushOrMergeData(
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
    invocationOrder += s"push:$partitionId"
    val accepted = super.pushOrMergeData(
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
    recordedPushes += lastPush
    accepted
  }
}

final case class RecordedCelebornAccounting(
    shuffleId: Int,
    mapId: Int,
    attemptId: Int,
    partitionId: Int,
    bytes: Array[Byte],
    offset: Int,
    length: Int)

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

/** Mimics an incompatible optional client whose raw-push method does not return an int. */
final class WrongReturnTypeCelebornPushClient {

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
      skipCompress: Boolean): Long = length.toLong
}
