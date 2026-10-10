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

import java.lang.reflect.{InvocationHandler, Method, Modifier, Proxy}
import java.nio.file.Files
import java.util.Collections
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.iceberg.BaseMetastoreTableOperations
import org.apache.iceberg.BaseTable
import org.apache.iceberg.DataFiles
import org.apache.iceberg.FileScanTask
import org.apache.iceberg.Metrics
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.PartitionSpecParser
import org.apache.iceberg.Schema
import org.apache.iceberg.Table
import org.apache.iceberg.TableMetadata
import org.apache.iceberg.TableScan
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.io.FileIO
import org.apache.iceberg.types.Conversions
import org.apache.iceberg.types.Types
import org.apache.spark.sql.internal.SQLConf

import org.apache.comet.CometConf
import org.apache.comet.iceberg.IcebergReflection.{RuntimeStatsMetrics, RuntimeStatsResult}

class IcebergReflectionSuite extends AnyFunSuite {

  /** Wrap real immutable Iceberg scans, counting only Comet's second planning pass. */
  class CountingRuntimeScan(source: TableScan) {
    var planningCalls = 0
    var requestedColumns: Seq[String] = Nil
    var beforePlanning: () => Unit = () => ()
    var closedPlans = 0
    var visitedFiles = 0

    def table(): Table = source.table()
    def scan(): TableScan = counted(source)

    private def counted(delegate: TableScan): TableScan =
      Proxy
        .newProxyInstance(
          classOf[TableScan].getClassLoader,
          Array[Class[_]](classOf[TableScan]),
          new InvocationHandler {
            override def invoke(proxy: AnyRef, method: Method, args: Array[AnyRef]): AnyRef = {
              if (method.getName == "planFiles") {
                planningCalls += 1
                beforePlanning()
              }
              if (method.getName == "includeColumnStats") {
                assert(args != null && args.length == 1, "must request only runtime columns")
                requestedColumns = args(0)
                  .asInstanceOf[java.util.Collection[String]]
                  .asScala
                  .toSeq
              }
              val result =
                method.invoke(delegate, Option(args).getOrElse(Array.empty[AnyRef]): _*)
              result match {
                case planned: org.apache.iceberg.io.CloseableIterable[_] =>
                  new org.apache.iceberg.io.CloseableIterable[AnyRef] {
                    override def iterator(): org.apache.iceberg.io.CloseableIterator[AnyRef] = {
                      val underlying = planned.iterator()
                      new org.apache.iceberg.io.CloseableIterator[AnyRef] {
                        override def hasNext: Boolean = underlying.hasNext
                        override def next(): AnyRef = {
                          visitedFiles += 1
                          underlying.next().asInstanceOf[AnyRef]
                        }
                        override def close(): Unit = underlying.close()
                      }
                    }
                    override def close(): Unit = {
                      closedPlans += 1
                      planned.close()
                    }
                  }
                case next: TableScan => counted(next)
                case other => other
              }
            }
          })
        .asInstanceOf[TableScan]
  }

  private def withRuntimeStatsTable(f: Table => Unit): Unit = {
    val conf = new Configuration()
    val directory = new Path(Files.createTempDirectory("comet-runtime-stats").toUri)
    val schema = new Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "extra", Types.IntegerType.get()))
    val table = new HadoopTables(conf).create(schema, directory.toString)
    val sqlConf = new SQLConf
    try {
      appendRuntimeStatsFiles(table, 0, 3)
      SQLConf.withExistingConf(sqlConf) { f(table) }
    } finally {
      directory.getFileSystem(conf).delete(directory, true)
    }
  }

  private def appendRuntimeStatsFiles(
      table: Table,
      start: Int,
      end: Int,
      pathPadding: Int = 0): Unit = {
    val append = table.newAppend()
    (start until end).foreach { index =>
      val bounds = Map(
        Integer.valueOf(1) -> Conversions.toByteBuffer(Types.IntegerType.get(), Int.box(index)),
        Integer.valueOf(2) -> Conversions.toByteBuffer(Types.IntegerType.get(), Int.box(index)))
      val metrics = new Metrics(10L, null, null, null, null, bounds.asJava, bounds.asJava)
      append.appendFile(
        DataFiles
          .builder(PartitionSpec.unpartitioned())
          .withPath(s"${table.location()}/data/${"x" * pathPadding}/$index.parquet")
          .withFileSizeInBytes(100L)
          .withMetrics(metrics)
          .build())
    }
    append.commit()
  }

  private def runtimeTasks(scan: TableScan): java.util.List[FileScanTask] = {
    val planned = scan.planFiles()
    try planned.iterator().asScala.toVector.asJava
    finally planned.close()
  }

  test("runtime statistics skip zero and one task without replanning") {
    withRuntimeStatsTable { table =>
      val source = table.newScan()
      val scan = new CountingRuntimeScan(source)
      val tasks = runtimeTasks(source)
      assert(
        IcebergReflection.runtimeFileStatistics(scan, tasks.subList(0, 0), Seq("id")).isEmpty)
      assert(
        IcebergReflection.runtimeFileStatistics(scan, tasks.subList(0, 1), Seq("id")).isEmpty)
      assert(scan.planningCalls == 0)
    }
  }

  test("runtime statistics reuse a snapshot and request only the runtime columns") {
    withRuntimeStatsTable { table =>
      val source = table.newScan().useSnapshot(table.currentSnapshot().snapshotId())
      val scan = new CountingRuntimeScan(source)
      val tasks = runtimeTasks(source)
      val first = IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id"))
      assert(first.size == 3)
      assert(scan.planningCalls == 1)
      assert(scan.requestedColumns == Seq("id"))
      first.values.foreach { file =>
        val dataFile = file.asInstanceOf[org.apache.iceberg.DataFile]
        assert(dataFile.lowerBounds().keySet().asScala.toSet == Set(Integer.valueOf(1)))
      }
      val repeated = new CountingRuntimeScan(source)
      val cached = IcebergReflection.runtimeFileStatistics(repeated, tasks, Seq("id"))
      assert(cached == first)
      assert(repeated.planningCalls == 0)
      val subset = IcebergReflection.runtimeFileStatistics(scan, tasks.subList(0, 2), Seq("id"))
      assert(subset.size == 2)
      assert(scan.planningCalls == 1)
      IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("extra"))
      assert(scan.planningCalls == 2)
    }
  }

  test("runtime statistics key the pinned snapshot rather than the table's current snapshot") {
    withRuntimeStatsTable { table =>
      val oldSnapshot = table.currentSnapshot().snapshotId()
      appendRuntimeStatsFiles(table, 3, 4)
      val oldSource = table.newScan().useSnapshot(oldSnapshot)
      val currentSource = table.newScan().useSnapshot(table.currentSnapshot().snapshotId())
      val oldScan = new CountingRuntimeScan(oldSource)
      val currentScan = new CountingRuntimeScan(currentSource)
      assert(
        IcebergReflection
          .runtimeFileStatistics(oldScan, runtimeTasks(oldSource), Seq("id"))
          .size == 3)
      // The newer snapshot can select exactly the old paths. It must still miss: coverage
      // alone must not mask a cache key that incorrectly uses the table's current snapshot.
      val overlappingSource = currentSource.filter(Expressions.lessThan("id", Int.box(3)))
      val overlappingScan = new CountingRuntimeScan(overlappingSource)
      assert(
        IcebergReflection
          .runtimeFileStatistics(overlappingScan, runtimeTasks(overlappingSource), Seq("id"))
          .size == 3)
      assert(overlappingScan.planningCalls == 1)
      assert(
        IcebergReflection
          .runtimeFileStatistics(currentScan, runtimeTasks(currentSource), Seq("id"))
          .size == 4)
      assert(oldScan.planningCalls == 1)
      assert(currentScan.planningCalls == 1)
      IcebergReflection.runtimeFileStatistics(oldScan, runtimeTasks(oldSource), Seq("id"))
      assert(oldScan.planningCalls == 1)
    }
  }

  test("runtime statistics replan when cached paths do not cover the planned files") {
    withRuntimeStatsTable { table =>
      val source = table.newScan()
      val filtered =
        new CountingRuntimeScan(source.filter(Expressions.lessThan("id", Int.box(2))))
      val filteredTasks = runtimeTasks(source.filter(Expressions.lessThan("id", Int.box(2))))
      assert(filteredTasks.size() == 2)
      assert(
        IcebergReflection.runtimeFileStatistics(filtered, filteredTasks, Seq("id")).size == 2)
      val full = new CountingRuntimeScan(source)
      val tasks = runtimeTasks(source)
      assert(IcebergReflection.runtimeFileStatistics(full, tasks, Seq("id")).size == 3)
      assert(full.planningCalls == 1)
      IcebergReflection.runtimeFileStatistics(full, tasks, Seq("id"))
      assert(full.planningCalls == 1)
    }
  }

  test("runtime statistics cache is bounded with least recently used eviction") {
    withRuntimeStatsTable { table =>
      val source = table.newScan()
      val scan = new CountingRuntimeScan(source)
      val tasks = runtimeTasks(source)
      def collect(maxFiles: Int): Unit = {
        SQLConf.get.setConfString(
          CometConf.COMET_ICEBERG_RUNTIME_STATS_MAX_FILES.key,
          maxFiles.toString)
        assert(IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id")).size == 3)
      }
      // Each small entry charges the fixed internal 1 MiB floor in the 64 MiB cache.
      (3 to 66).foreach(collect)
      assert(scan.planningCalls == 64)
      collect(3)
      collect(67)
      collect(3)
      assert(scan.planningCalls == 65)
      collect(4)
      assert(scan.planningCalls == 66)
      assert(
        IcebergReflection.runtimeStatsCacheUsage._1 <=
          IcebergReflection.RuntimeStatsCacheMaxBytes)
    }
  }

  test("disabled runtime statistics skip collection and preserve cached snapshot bounds") {
    withRuntimeStatsTable { table =>
      val source = table.newScan()
      val scan = new CountingRuntimeScan(source)
      val tasks = runtimeTasks(source)
      val cached = IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id"))
      SQLConf.get.setConfString(CometConf.COMET_ICEBERG_RUNTIME_STATS_ENABLED.key, "false")
      (1 to 2).foreach { _ =>
        assert(IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id")).isEmpty)
      }
      val uncached = new CountingRuntimeScan(source)
      assert(IcebergReflection.runtimeFileStatistics(uncached, tasks, Seq("extra")).isEmpty)
      assert(uncached.planningCalls == 0)
      assert(scan.planningCalls == 1)
      SQLConf.get.setConfString(CometConf.COMET_ICEBERG_RUNTIME_STATS_ENABLED.key, "true")
      assert(IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id")) == cached)
      assert(scan.planningCalls == 1)
    }
  }

  test("oversized selections never replan and disabled statistics skip split tasks") {
    withRuntimeStatsTable { table =>
      val source = table.newScan()
      val scan = new CountingRuntimeScan(source)
      val tasks = runtimeTasks(source)
      SQLConf.get.setConfString(CometConf.COMET_ICEBERG_RUNTIME_STATS_MAX_FILES.key, "2")
      for (enabled <- Seq(true, false); _ <- 1 to 2) {
        SQLConf.get.setConfString(
          CometConf.COMET_ICEBERG_RUNTIME_STATS_ENABLED.key,
          enabled.toString)
        assert(IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id")).isEmpty)
      }
      assert(scan.planningCalls == 0)
      // The bound counts physical files, not split tasks, without changing the one-task gate.
      SQLConf.get.setConfString(CometConf.COMET_ICEBERG_RUNTIME_STATS_ENABLED.key, "true")
      val splits = Collections.nCopies(100, tasks.get(0))
      assert(IcebergReflection.runtimeFileStatistics(scan, splits, Seq("id")).size == 1)
      assert(scan.planningCalls == 1)
    }
  }

  test(
    "oversized byte collection closes the plan, returns no partial metrics and caches rejection") {
    withRuntimeStatsTable { table =>
      // Selection fits the internal 8 MiB budget; retaining file paths exceeds it.
      appendRuntimeStatsFiles(table, 3, 1000, pathPadding = 2000)
      val source = table.newScan()
      val scan = new CountingRuntimeScan(source)
      val tasks = runtimeTasks(source)
      assert(IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id")).isEmpty)
      assert(scan.planningCalls == 1)
      assert(scan.closedPlans == 1)
      assert(scan.visitedFiles < tasks.size(), "abort before collecting the full snapshot")
      assert(IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id")).isEmpty)
      assert(scan.planningCalls == 1)
      SQLConf.get.setConfString(CometConf.COMET_ICEBERG_RUNTIME_STATS_ENABLED.key, "false")
      assert(IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id")).isEmpty)
      assert(scan.planningCalls == 1)
      assert(scan.closedPlans == 1)
      // A different maxFiles policy has its own negative entry, under the same byte budget.
      SQLConf.get.setConfString(CometConf.COMET_ICEBERG_RUNTIME_STATS_ENABLED.key, "true")
      SQLConf.get.setConfString(CometConf.COMET_ICEBERG_RUNTIME_STATS_MAX_FILES.key, "1000")
      assert(IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id")).isEmpty)
      assert(scan.planningCalls == 2)
    }
  }

  test("alternating session policies retain useful entries under the shared process bound") {
    withRuntimeStatsTable { table =>
      val source = table.newScan()
      val tasks = runtimeTasks(source)
      val sessions = Seq(8, 16).map { capacity =>
        val conf = new SQLConf
        conf.setConfString(CometConf.COMET_ICEBERG_RUNTIME_STATS_MAX_FILES.key, capacity.toString)
        (conf, new CountingRuntimeScan(source))
      }
      for (_ <- 1 to 3; (conf, scan) <- sessions) {
        SQLConf.withExistingConf(conf) {
          assert(IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id")).size == 3)
        }
      }
      assert(sessions.forall(_._2.planningCalls == 1))
      val (chargedBytes, retainedFiles) = IcebergReflection.runtimeStatsCacheUsage
      assert(chargedBytes <= IcebergReflection.RuntimeStatsCacheMaxBytes)
      assert(retainedFiles >= 6)
    }
  }

  test("many-file snapshots obey the collection gate and bound cache retention") {
    withRuntimeStatsTable { table =>
      appendRuntimeStatsFiles(table, 3, 100)
      val source = table.newScan()
      val scan = new CountingRuntimeScan(source)
      val tasks = runtimeTasks(source)
      SQLConf.get.setConfString(CometConf.COMET_ICEBERG_RUNTIME_STATS_MAX_FILES.key, "32")
      assert(IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id")).isEmpty)
      assert(scan.planningCalls == 0)
      SQLConf.get.setConfString(CometConf.COMET_ICEBERG_RUNTIME_STATS_MAX_FILES.key, "100")
      assert(IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id")).size == 100)
      assert(
        IcebergReflection.runtimeFileStatistics(scan, tasks.subList(0, 2), Seq("id")).size == 2)
      assert(scan.planningCalls == 1)
      val (chargedBytes, retainedFiles) = IcebergReflection.runtimeStatsCacheUsage
      assert(chargedBytes <= IcebergReflection.RuntimeStatsCacheMaxBytes)
      assert(retainedFiles >= 100)
    }
  }

  test("advisory collection failure releases the process collection permit") {
    withRuntimeStatsTable { table =>
      val source = table.newScan()
      val scan = new CountingRuntimeScan(source)
      val tasks = runtimeTasks(source)
      scan.beforePlanning = () => throw new IllegalStateException("injected planning failure")
      assert(IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id")).isEmpty)
      scan.beforePlanning = () => ()
      assert(IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id")).size == 3)
      assert(scan.planningCalls == 2)
      assert(scan.closedPlans == 1)
    }
  }

  test("concurrent misses for distinct keys proceed and preserve configuration isolation") {
    withRuntimeStatsTable { table =>
      val source = table.newScan()
      val tasks = runtimeTasks(source)
      val started = new CountDownLatch(1)
      val release = new CountDownLatch(1)
      val scan = new CountingRuntimeScan(source)
      scan.beforePlanning = () => {
        started.countDown()
        assert(release.await(30, TimeUnit.SECONDS))
      }
      val collectingConf = new SQLConf
      collectingConf.setConfString(CometConf.COMET_ICEBERG_RUNTIME_STATS_MAX_FILES.key, "8")
      val executor = Executors.newSingleThreadExecutor()
      try {
        val collecting = executor.submit(new java.util.concurrent.Callable[Map[String, AnyRef]] {
          override def call(): Map[String, AnyRef] = SQLConf.withExistingConf(collectingConf) {
            IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id"))
          }
        })
        assert(started.await(30, TimeUnit.SECONDS))
        // A different field and policy is a distinct key: it collects while the first is held.
        val other = new CountingRuntimeScan(source)
        val otherConf = new SQLConf
        otherConf.setConfString(CometConf.COMET_ICEBERG_RUNTIME_STATS_MAX_FILES.key, "16")
        SQLConf.withExistingConf(otherConf) {
          assert(IcebergReflection.runtimeFileStatistics(other, tasks, Seq("extra")).size == 3)
        }
        assert(other.planningCalls == 1)
        release.countDown()
        assert(collecting.get(30, TimeUnit.SECONDS).size == 3)
        SQLConf.withExistingConf(collectingConf) {
          assert(IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id")).size == 3)
        }
        assert(scan.planningCalls == 1)
        SQLConf.withExistingConf(otherConf) {
          assert(IcebergReflection.runtimeFileStatistics(other, tasks, Seq("extra")).size == 3)
        }
        assert(other.planningCalls == 1)
        SQLConf.withExistingConf(otherConf) {
          assert(IcebergReflection.runtimeFileStatistics(other, tasks, Seq("extra")).size == 3)
        }
        SQLConf.withExistingConf(collectingConf) {
          assert(IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id")).size == 3)
        }
        assert(scan.planningCalls == 1)
        assert(other.planningCalls == 1)
      } finally {
        release.countDown()
        executor.shutdownNow()
      }
    }
  }

  private def collectStatistics(
      scan: CountingRuntimeScan,
      tasks: java.util.List[FileScanTask],
      conf: SQLConf): java.util.concurrent.Callable[RuntimeStatsResult] =
    new java.util.concurrent.Callable[RuntimeStatsResult] {
      override def call(): RuntimeStatsResult = SQLConf.withExistingConf(conf) {
        IcebergReflection.collectRuntimeFileStatistics(scan, tasks, Seq("id"))
      }
    }

  test("identical concurrent misses share one in-flight collection") {
    withRuntimeStatsTable { table =>
      val source = table.newScan()
      val tasks = runtimeTasks(source)
      val started = new CountDownLatch(1)
      val release = new CountDownLatch(1)
      val leader = new CountingRuntimeScan(source)
      leader.beforePlanning = () => {
        started.countDown()
        assert(release.await(30, TimeUnit.SECONDS))
      }
      val follower = new CountingRuntimeScan(source)
      val conf = SQLConf.get
      val executor = Executors.newFixedThreadPool(2)
      try {
        val leading = executor.submit(collectStatistics(leader, tasks, conf))
        assert(started.await(30, TimeUnit.SECONDS))
        val following = executor.submit(collectStatistics(follower, tasks, conf))
        // Release the leader only once the follower waits on its flight.
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
        while (IcebergReflection.runtimeStatsWaiters.get() == 0) {
          assert(System.nanoTime() < deadline, "the identical request never waited")
          Thread.`yield`()
        }
        release.countDown()
        val led = leading.get(30, TimeUnit.SECONDS)
        val shared = following.get(30, TimeUnit.SECONDS)
        assert(led.statistics.size == 3)
        assert(led.metrics(RuntimeStatsMetrics.CacheMisses) == 1L)
        assert(led.metrics.contains(RuntimeStatsMetrics.CollectionTime))
        assert(shared.statistics == led.statistics)
        assert(
          shared.metrics == Map(
            RuntimeStatsMetrics.SharedWaits -> 1L,
            RuntimeStatsMetrics.CacheHits -> 1L))
        assert(leader.planningCalls == 1)
        assert(follower.planningCalls == 0)
      } finally {
        release.countDown()
        executor.shutdownNow()
      }
    }
  }

  test("distinct concurrent misses are bounded and excess misses fail open") {
    withRuntimeStatsTable { table =>
      val source = table.newScan()
      val tasks = runtimeTasks(source)
      val limit = IcebergReflection.RuntimeStatsMaxConcurrentCollections
      val started = new CountDownLatch(limit)
      val release = new CountDownLatch(1)
      val executor = Executors.newFixedThreadPool(limit)
      try {
        // Each selected-file limit is a distinct key for the same snapshot and field.
        val collecting = (1 to limit).map { index =>
          val scan = new CountingRuntimeScan(source)
          scan.beforePlanning = () => {
            started.countDown()
            assert(release.await(30, TimeUnit.SECONDS))
          }
          val conf = new SQLConf
          conf.setConfString(
            CometConf.COMET_ICEBERG_RUNTIME_STATS_MAX_FILES.key,
            (8 * index).toString)
          executor.submit(collectStatistics(scan, tasks, conf))
        }
        assert(started.await(30, TimeUnit.SECONDS))
        val excess = new CountingRuntimeScan(source)
        val excessConf = new SQLConf
        excessConf.setConfString(CometConf.COMET_ICEBERG_RUNTIME_STATS_MAX_FILES.key, "1000")
        val declined = collectStatistics(excess, tasks, excessConf).call()
        assert(declined.statistics.isEmpty)
        assert(declined.metrics == Map(RuntimeStatsMetrics.ContentionSkips -> 1L))
        assert(excess.planningCalls == 0)
        release.countDown()
        collecting.foreach(f => assert(f.get(30, TimeUnit.SECONDS).statistics.size == 3))
        // Contention caches no rejection: the declined key collects once permits are free.
        val retried = collectStatistics(excess, tasks, excessConf).call()
        assert(retried.statistics.size == 3)
        assert(excess.planningCalls == 1)
      } finally {
        release.countDown()
        executor.shutdownNow()
      }
    }
  }

  test("a byte-budget rejection declines equal or larger selections but not smaller ones") {
    withRuntimeStatsTable { table =>
      // Files 3 to 999 have long paths, so the full selection exceeds the 8 MiB budget, while
      // the files 0 to 2 form a small, selective query on the same snapshot, field and policy.
      appendRuntimeStatsFiles(table, 3, 1000, pathPadding = 2000)
      val conf = SQLConf.get
      val source = table.newScan()
      val broad = new CountingRuntimeScan(source)
      val tasks = runtimeTasks(source)
      val selectiveSource = source.filter(Expressions.lessThan("id", Int.box(3)))
      val selective = new CountingRuntimeScan(selectiveSource)
      val selectiveTasks = runtimeTasks(selectiveSource)
      assert(selectiveTasks.size() == 3)
      val negativeHit = Map(RuntimeStatsMetrics.NegativeCacheHits -> 1L)

      val rejected = collectStatistics(broad, tasks, conf).call()
      assert(rejected.statistics.isEmpty)
      assert(rejected.metrics(RuntimeStatsMetrics.CacheMisses) == 1L)
      assert(rejected.metrics(RuntimeStatsMetrics.OverBudget) == 1L)
      assert(broad.planningCalls == 1)
      // An equal selection is declined without replanning.
      assert(collectStatistics(broad, tasks, conf).call().metrics == negativeHit)
      assert(broad.planningCalls == 1)

      // A strictly smaller selection bypasses the rejection and collects its statistics.
      val collected = collectStatistics(selective, selectiveTasks, conf).call()
      assert(collected.statistics.size == 3)
      assert(collected.metrics(RuntimeStatsMetrics.CacheMisses) == 1L)
      assert(selective.planningCalls == 1)
      // Neither result displaces the other: the broad selection is still declined and the
      // selective one is now a cache hit, both without replanning.
      assert(collectStatistics(broad, tasks, conf).call().metrics == negativeHit)
      val hit = collectStatistics(selective, selectiveTasks, conf).call()
      assert(hit.statistics == collected.statistics)
      assert(hit.metrics == Map(RuntimeStatsMetrics.CacheHits -> 1L))
      assert(broad.planningCalls == 1)
      assert(selective.planningCalls == 1)

      // A smaller selection that is also too large lowers the threshold and keeps the cached
      // selective statistics.
      val nearlyBroad = tasks.subList(0, tasks.size() - 1)
      val lowered = collectStatistics(broad, nearlyBroad, conf).call()
      assert(lowered.statistics.isEmpty)
      assert(lowered.metrics(RuntimeStatsMetrics.OverBudget) == 1L)
      assert(broad.planningCalls == 2)
      assert(collectStatistics(broad, nearlyBroad, conf).call().metrics == negativeHit)
      assert(broad.planningCalls == 2)
      assert(
        collectStatistics(selective, selectiveTasks, conf).call().metrics ==
          Map(RuntimeStatsMetrics.CacheHits -> 1L))
      assert(selective.planningCalls == 1)
    }
  }

  test("tiny selections use direct lookups rather than traverse a large cached snapshot") {
    val file = new Object
    val underlying = (0 until 100000).map(index => s"file-$index" -> file).toMap
    var lookups = 0
    def lookup(path: String): Option[AnyRef] = {
      lookups += 1
      underlying.get(path)
    }
    assert(
      IcebergReflection
        .selectRuntimeStatistics(Set("file-1", "file-99999"), lookup)
        .contains(Map("file-1" -> file, "file-99999" -> file)))
    assert(lookups == 2)
    assert(IcebergReflection.selectRuntimeStatistics(Set("missing"), lookup).isEmpty)
    assert(lookups == 3)
  }

  /** Mimics HiveTableOperations/GlueTableOperations which inherit current(). */
  class StubTableOperations extends BaseMetastoreTableOperations {
    override protected def tableName(): String = "test"
    override def refresh(): TableMetadata = null
    override def io(): FileIO = null
  }

  test("getTableMetadata succeeds when operations class inherits current()") {
    val ops = new StubTableOperations()
    val schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()))
    val expectedMetadata = TableMetadata.newTableMetadata(
      schema,
      PartitionSpec.unpartitioned(),
      "file:///tmp/test-table",
      Collections.emptyMap[String, String]())
    val metadataField = classOf[BaseMetastoreTableOperations]
      .getDeclaredField("currentMetadata")
    metadataField.setAccessible(true)
    metadataField.set(ops, expectedMetadata)
    // current() checks shouldRefresh (default true) and calls refresh() instead of
    // returning currentMetadata. Set to false so current() returns our stubbed metadata.
    val refreshField = classOf[BaseMetastoreTableOperations]
      .getDeclaredField("shouldRefresh")
    refreshField.setAccessible(true)
    refreshField.set(ops, false)

    val table = new BaseTable(ops, "test-table")
    val metadata = IcebergReflection.getTableMetadata(table)
    assert(metadata.isDefined)
    assert(metadata.get.isInstanceOf[TableMetadata])
  }

  test("findMethod resolves a method once and returns the cached instance") {
    val first = IcebergReflection.findMethod(classOf[Schema], "columns")
    val second = IcebergReflection.findMethod(classOf[Schema], "columns")
    assert(first.isDefined)
    assert(first.get.getName == "columns")
    // Class.getMethod hands back a fresh copy per call; the cache must not.
    assert(first.get eq second.get)
  }

  test("an absent method is a cached miss, and getMethod still throws for it") {
    assert(IcebergReflection.findMethod(classOf[Schema], "noSuchAccessor").isEmpty)
    assert(IcebergReflection.findMethod(classOf[Schema], "noSuchAccessor").isEmpty)
    assertThrows[NoSuchMethodException] {
      IcebergReflection.getMethod(classOf[Schema], "noSuchAccessor")
    }
  }

  test("findMethod distinguishes overloads by parameter type") {
    val byId = IcebergReflection.findMethod(classOf[Schema], "findField", classOf[Int])
    val byName = IcebergReflection.findMethod(classOf[Schema], "findField", classOf[String])
    assert(byId.isDefined && byName.isDefined)
    assert(byId.get ne byName.get)

    val schema = new Schema(Types.NestedField.required(7, "id", Types.IntegerType.get()))
    val fieldById = byId.get.invoke(schema, Integer.valueOf(7)).asInstanceOf[Types.NestedField]
    val fieldByName = byName.get.invoke(schema, "id").asInstanceOf[Types.NestedField]
    assert(fieldById.name() == "id")
    assert(fieldByName.fieldId() == 7)
  }

  test("findMethodInHierarchy finds an inherited method and caches it") {
    val first = IcebergReflection.findMethodInHierarchy(classOf[StubTableOperations], "current")
    val second = IcebergReflection.findMethodInHierarchy(classOf[StubTableOperations], "current")
    assert(first.isDefined)
    // current() is declared on BaseMetastoreTableOperations, not on the stub itself.
    assert(first.get.getDeclaringClass == classOf[BaseMetastoreTableOperations])
    assert(first.get eq second.get)
    assert(IcebergReflection.findMethodInHierarchy(classOf[StubTableOperations], "nope").isEmpty)
  }

  test("extractFileLocation reads location() when the class has one") {
    val file = new LocationFile("s3://bucket/data/f.parquet")
    assert(
      IcebergReflection.extractFileLocation(classOf[LocationFile], file) ==
        Some("s3://bucket/data/f.parquet"))
  }

  test("extractFileLocation falls back to path() on Iceberg versions without location()") {
    val file = new PathOnlyFile("s3://bucket/data/f.parquet")
    // Called twice: the second call reads the cached "location() is absent" answer.
    assert(
      IcebergReflection.extractFileLocation(classOf[PathOnlyFile], file) ==
        Some("s3://bucket/data/f.parquet"))
    assert(
      IcebergReflection.extractFileLocation(classOf[PathOnlyFile], file) ==
        Some("s3://bucket/data/f.parquet"))
  }

  test("extractFileLocation returns None when the class exposes neither accessor") {
    assert(IcebergReflection.extractFileLocation(classOf[Object], new Object).isEmpty)
  }

  test("extractFileLocation propagates a genuine invoke failure instead of returning None") {
    val file = new ThrowingLocationFile
    val ex = intercept[java.lang.reflect.InvocationTargetException] {
      IcebergReflection.extractFileLocation(classOf[ThrowingLocationFile], file)
    }
    assert(ex.getCause.getMessage == "boom")
  }

  test("taskCommitFileLocations reads plain SparkWrite task commits") {
    val commit = new PlainTaskCommit(
      Array(
        new LocationFile("s3://bucket/data/a.parquet"),
        new LocationFile("s3://bucket/data/b.parquet")))
    assert(
      IcebergReflection.taskCommitFileLocations(commit) ==
        Seq("s3://bucket/data/a.parquet", "s3://bucket/data/b.parquet"))
  }

  test("taskCommitFileLocations reads new files from position-delta task commits") {
    val commit = new DeltaTaskCommit(
      Array(new LocationFile("s3://bucket/data/a.parquet")),
      Array(new LocationFile("s3://bucket/delete/d.parquet")),
      Array(new LocationFile("s3://bucket/delete/old.parquet")))

    assert(
      IcebergReflection.taskCommitFileLocations(commit) ==
        Seq("s3://bucket/data/a.parquet", "s3://bucket/delete/d.parquet"))
  }

  test("getFileFormat reads format() when declared") {
    val file = new FormatFile("PARQUET")
    assert(IcebergReflection.getFileFormat(classOf[FormatFile], file) == Some("PARQUET"))
  }

  test("getFileFormat returns None when format() is not declared") {
    assert(IcebergReflection.getFileFormat(classOf[Object], new Object).isEmpty)
  }

  test("getFileFormat propagates a genuine invoke failure instead of returning None") {
    val file = new ThrowingFormatFile
    val ex = intercept[java.lang.reflect.InvocationTargetException] {
      IcebergReflection.getFileFormat(classOf[ThrowingFormatFile], file)
    }
    assert(ex.getCause.getMessage == "boom")
  }

  test("getEqualityFieldIds reads declared equality field ids") {
    val ids = java.util.List.of(Integer.valueOf(3), Integer.valueOf(5))
    val file = new EqualityIdsFile(ids)
    assert(IcebergReflection.getEqualityFieldIds(classOf[EqualityIdsFile], file) == ids)
  }

  test("getEqualityFieldIds treats a null return (position delete) as empty, not a failure") {
    val file = new NullEqualityIdsFile
    assert(IcebergReflection.getEqualityFieldIds(classOf[NullEqualityIdsFile], file).isEmpty)
  }

  test("getEqualityFieldIds returns empty when equalityFieldIds() is not declared") {
    assert(IcebergReflection.getEqualityFieldIds(classOf[Object], new Object).isEmpty)
  }

  test("getEqualityFieldIds propagates a genuine invoke failure instead of returning empty") {
    val file = new ThrowingEqualityIdsFile
    val ex = intercept[java.lang.reflect.InvocationTargetException] {
      IcebergReflection.getEqualityFieldIds(classOf[ThrowingEqualityIdsFile], file)
    }
    assert(ex.getCause.getMessage == "boom")
  }

  test("a resolved method has access checks suppressed") {
    // Iceberg's concrete file impls are package-private (a built DataFile is a GenericDataFile,
    // and its accessors are declared on the equally package-private BaseFile), so an accessor
    // resolved on one is not invocable from Comet's package until setAccessible has run. The
    // modifier assertions keep the test from going vacuous if Iceberg ever makes them public.
    val file = DataFiles
      .builder(PartitionSpec.unpartitioned())
      .withPath("/tmp/data/f.parquet")
      .withFileSizeInBytes(10)
      .withRecordCount(1)
      .withFormat("PARQUET")
      .build()
    assert(!Modifier.isPublic(file.getClass.getModifiers))

    val method = IcebergReflection.findMethod(file.getClass, "path")
    assert(method.isDefined)
    assert(!Modifier.isPublic(method.get.getDeclaringClass.getModifiers))
    // Without makeAccessible this invoke throws IllegalAccessException.
    assert(method.get.invoke(file).toString == "/tmp/data/f.parquet")
  }

  /** Mimics a table whose operations installed the stock plaintext manager. */
  class PlaintextEncryptionTable {
    def encryption(): AnyRef =
      org.apache.iceberg.encryption.PlaintextEncryptionManager.instance()
  }

  /** Mimics a table whose (possibly custom) operations installed a real encryption manager. */
  class CustomEncryptionTable {
    def encryption(): AnyRef = new Object
  }

  class NoEncryptionMethodTable

  test("getEncryptionManager resolves the manager the table actually installed") {
    val plaintext = IcebergReflection.getEncryptionManager(new PlaintextEncryptionTable)
    assert(
      plaintext.exists(
        _.getClass.getName == "org.apache.iceberg.encryption.PlaintextEncryptionManager"))

    val custom = IcebergReflection.getEncryptionManager(new CustomEncryptionTable)
    assert(custom.isDefined)
    assert(
      custom.get.getClass.getName != "org.apache.iceberg.encryption.PlaintextEncryptionManager")
  }

  test("getEncryptionManager returns None when encryption() cannot be resolved") {
    // The write gate treats None as fail-closed, so a table type without the accessor (or a
    // future rename) declines the native write rather than assuming plaintext.
    assert(IcebergReflection.getEncryptionManager(new NoEncryptionMethodTable).isEmpty)
  }

  class CustomLocationProviderTable {
    def locationProvider(): AnyRef = new Object
  }

  class NoLocationProviderTable

  test("getLocationProvider resolves the provider the table actually installed") {
    val custom = IcebergReflection.getLocationProvider(new CustomLocationProviderTable)
    assert(custom.isDefined)
    assert(custom.get.getClass.getName != IcebergReflection.ClassNames.DEFAULT_LOCATION_PROVIDER)
  }

  test("getLocationProvider returns None when locationProvider() cannot be resolved") {
    // The write gate treats None as fail-closed, so a table type without the accessor (or a
    // future rename) declines the native write rather than assuming DefaultLocationProvider.
    assert(IcebergReflection.getLocationProvider(new NoLocationProviderTable).isEmpty)
  }

  test("executor-side reflection surface resolves against the linked Iceberg") {
    // The eligibility gate declines a native write when any class, method, or constructor used
    // by the executor-side commit-message assembly fails to resolve (it would otherwise be a
    // task failure after data files were already written). Asserting the probe is green here
    // means an Iceberg version bump that moves part of that surface fails this test loudly
    // instead of silently falling every native write back to the JVM writer.
    assert(
      IcebergReflection.executorReflectionUnresolved.isEmpty,
      IcebergReflection.executorReflectionUnresolved)
  }

  /** Schema the transform tests below partition on, one column per transform source type. */
  private val transformSchema = new Schema(
    Types.NestedField.optional(1, "id", Types.LongType.get()),
    Types.NestedField.optional(2, "s", Types.StringType.get()),
    Types.NestedField.optional(3, "ts", Types.TimestampType.withZone()))

  /**
   * The single-field spec Iceberg parses out of `transform`, applied to source column `sourceId`.
   */
  private def singleFieldSpec(transform: String, sourceId: Int): PartitionSpec =
    PartitionSpecParser.fromJson(
      transformSchema,
      s"""{"spec-id":0,"fields":[{"name":"p","transform":"$transform",""" +
        s""""source-id":$sourceId,"field-id":1000}]}""")

  test("forNative keeps every transform iceberg-rust can deserialize") {
    // Spelled as Iceberg serializes them. The round-trip assertion matters as much as forNative's
    // own answer: forNative matches on Transform.toString, so a version that renders a transform
    // differently from its JSON spelling would silently start rewriting it to "unknown".
    Seq(
      ("identity", 1),
      ("void", 1),
      ("bucket[8]", 1),
      ("truncate[4]", 2),
      ("year", 3),
      ("month", 3),
      ("day", 3),
      ("hour", 3)).foreach { case (transform, sourceId) =>
      val rendered = singleFieldSpec(transform, sourceId).fields().get(0).transform().toString
      assert(rendered == transform, s"Iceberg renders $transform as $rendered")
      assert(IcebergReflection.Transforms.forNative(rendered) == transform)
    }
  }

  test("forNative rewrites a transform Iceberg Java could not resolve") {
    // TestForwardCompatibility's UNKNOWN_SPEC. Iceberg parses "zero" into an UnknownTransform whose
    // toString is the original name; serialized verbatim it fails PartitionSpec deserialization in
    // iceberg-rust, leaving the scan task holding partition values with no spec, which
    // FileScanTask validation rejects ("Non-empty FileScanTask partition requires a partition
    // spec") and the whole scan dies.
    val spec = singleFieldSpec("zero", 1)
    val rendered = spec.fields().get(0).transform().toString
    assert(rendered == "zero")
    assert(IcebergReflection.Transforms.forNative(rendered) == "unknown")

    // The partition type Comet serializes alongside the rewritten spec has to agree with what
    // iceberg-rust derives for Transform::Unknown, which is string.
    assert(spec.partitionType().fields().get(0).`type`().toString == "string")
  }

  class PlainTaskCommit(taskFiles: Array[LocationFile]) {
    def files(): Array[LocationFile] = taskFiles
  }

  class DeltaTaskCommit(
      data: Array[LocationFile],
      deletes: Array[LocationFile],
      rewrittenDeletes: Array[LocationFile]) {
    def dataFiles(): Array[LocationFile] = data
    def deleteFiles(): Array[LocationFile] = deletes
    def rewrittenDeleteFiles(): Array[LocationFile] = rewrittenDeletes
  }

  /** Mimics a newer Iceberg ContentFile, which exposes location(). */
  class LocationFile(loc: String) {
    def location(): String = loc
  }

  /** Mimics Iceberg before 1.7, where ContentFile only exposed path(): CharSequence. */
  class PathOnlyFile(p: String) {
    def path(): CharSequence = p
  }

  /** location() is declared (not a version difference) but the call itself fails. */
  class ThrowingLocationFile {
    def location(): String = throw new RuntimeException("boom")
  }

  class FormatFile(fmt: String) {
    def format(): String = fmt
  }

  /** format() is declared but the call itself fails. */
  class ThrowingFormatFile {
    def format(): String = throw new RuntimeException("boom")
  }

  class EqualityIdsFile(ids: java.util.List[Integer]) {
    def equalityFieldIds(): java.util.List[Integer] = ids
  }

  /** Mimics a position-delete file: the accessor is declared and returns null, not a failure. */
  class NullEqualityIdsFile {
    def equalityFieldIds(): java.util.List[Integer] = null
  }

  /** equalityFieldIds() is declared but the call itself fails. */
  class ThrowingEqualityIdsFile {
    def equalityFieldIds(): java.util.List[Integer] = throw new RuntimeException("boom")
  }
}
