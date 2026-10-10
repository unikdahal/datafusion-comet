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

class IcebergReflectionSuite extends AnyFunSuite {

  /** Wrap real immutable Iceberg scans, counting only Comet's second planning pass. */
  class CountingRuntimeScan(source: TableScan) {
    var planningCalls = 0
    var requestedColumns = Seq.empty[String]

    def table(): Table = source.table()
    def scan(): TableScan = counted(source)

    private def counted(delegate: TableScan): TableScan =
      Proxy
        .newProxyInstance(
          classOf[TableScan].getClassLoader,
          Array[Class[_]](classOf[TableScan]),
          new InvocationHandler {
            override def invoke(proxy: AnyRef, method: Method, args: Array[AnyRef]): AnyRef = {
              if (method.getName == "planFiles") planningCalls += 1
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

  private def appendRuntimeStatsFiles(table: Table, start: Int, end: Int): Unit = {
    val append = table.newAppend()
    (start until end).foreach { index =>
      val bounds = Map(
        Integer.valueOf(1) -> Conversions.toByteBuffer(Types.IntegerType.get(), Int.box(index)),
        Integer.valueOf(2) -> Conversions.toByteBuffer(Types.IntegerType.get(), Int.box(index)))
      val metrics = new Metrics(10L, null, null, null, null, bounds.asJava, bounds.asJava)
      append.appendFile(
        DataFiles
          .builder(PartitionSpec.unpartitioned())
          .withPath(s"${table.location()}/data/$index.parquet")
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
      val conf = SQLConf.get
      conf.setConfString(CometConf.COMET_ICEBERG_RUNTIME_STATS_CACHE_MAX_ENTRIES.key, "2")
      val source = table.newScan()
      val scan = new CountingRuntimeScan(source)
      val tasks = runtimeTasks(source)
      IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id"))
      IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("extra"))
      IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id"))
      IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id", "extra"))
      assert(scan.planningCalls == 3)
      IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("extra", "id"))
      IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id"))
      assert(scan.planningCalls == 3)
      IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("extra"))
      assert(scan.planningCalls == 4)
      conf.setConfString(CometConf.COMET_ICEBERG_RUNTIME_STATS_CACHE_MAX_ENTRIES.key, "1")
      IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id"))
      assert(scan.planningCalls == 5)
    }
  }

  test("disabled runtime statistics cache replans and preserves the same bounds") {
    withRuntimeStatsTable { table =>
      val source = table.newScan()
      val scan = new CountingRuntimeScan(source)
      val tasks = runtimeTasks(source)
      val cached = IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id"))
      SQLConf.get.setConfString(CometConf.COMET_ICEBERG_RUNTIME_STATS_CACHE_ENABLED.key, "false")
      (1 to 2).foreach { _ =>
        val uncached = IcebergReflection.runtimeFileStatistics(scan, tasks, Seq("id"))
        assert(uncached.keySet == cached.keySet)
        uncached.foreach { case (path, file) =>
          val expected = cached(path).asInstanceOf[org.apache.iceberg.DataFile]
          val actual = file.asInstanceOf[org.apache.iceberg.DataFile]
          assert(actual.lowerBounds() == expected.lowerBounds())
          assert(actual.upperBounds() == expected.upperBounds())
        }
      }
      assert(scan.planningCalls == 3)
    }
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
