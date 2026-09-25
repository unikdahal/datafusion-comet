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

import java.lang.ref.WeakReference
import java.lang.reflect.{Constructor, Method}
import java.nio.ByteBuffer
import java.util.WeakHashMap

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.connector.write.WriterCommitMessage

import org.apache.comet.iceberg.IcebergReflection.{findMethodInHierarchy, loadClass}

sealed trait TransportManifestContent extends Product with Serializable
case object DataManifestContent extends TransportManifestContent
case object DeleteManifestContent extends TransportManifestContent

final case class TransportManifest(
    content: TransportManifestContent,
    specId: Int,
    avroBytes: Array[Byte])

/**
 * The stable task-result model for native Iceberg row-level writes. Manifest content is explicit
 * so a delete transport manifest can never be passed to Iceberg's data-manifest reader.
 */
final case class IcebergDeltaTaskPayload(
    schemaRevision: Int,
    manifests: Seq[TransportManifest],
    referencedDataFiles: Seq[String],
    rewrittenDeleteFileLocations: Seq[String])

final case class PreviousPositionDeleteFile(
    location: String,
    fileSizeInBytes: Long,
    format: String,
    partitionSpecId: Int,
    keyMetadata: Option[Array[Byte]],
    referencedDataFile: Option[String],
    contentOffset: Option[Long],
    contentSizeInBytes: Option[Long],
    recordCount: Long,
    originalFile: AnyRef)

final case class PreviousPositionDeletesForDataFile(
    dataFile: String,
    deleteFiles: Seq[PreviousPositionDeleteFile])

/** Reflection seam for Iceberg's package-private DeltaTaskCommit constructor and manifest APIs. */
object IcebergDeltaReflection {

  /**
   * Reads the command scan's own rewritableDeletes(false) map. This is Iceberg's source of truth
   * for FILE-granularity position-delete replacement; a new table scan here could observe a
   * different snapshot than the one used by the row-level command.
   */
  def rewritablePositionDeletes(
      positionDeltaWrite: AnyRef): Either[String, Seq[PreviousPositionDeletesForDataFile]] =
    try {
      val scan = IcebergReflection
        .getPositionDeltaWriteValue(positionDeltaWrite, "scan")
        .getOrElse(return Left("SparkPositionDeltaWrite.scan reflection failed"))
      val method = IcebergReflection
        .findMethodInHierarchy(scan.getClass, "rewritableDeletes", java.lang.Boolean.TYPE)
        .getOrElse(return Left("SparkBatchQueryScan.rewritableDeletes(boolean) is unavailable"))
      val rewritable = method.invoke(scan, java.lang.Boolean.FALSE)
        .asInstanceOf[java.util.Map[String, AnyRef]]
      if (rewritable == null || rewritable.isEmpty) return Right(Seq.empty)

      val contentFileClass = loadClass("org.apache.iceberg.ContentFile")
      val deleteFileClass = loadClass("org.apache.iceberg.DeleteFile")
      val contentFileUtil = loadClass("org.apache.iceberg.util.ContentFileUtil")
      val isFileScoped = contentFileUtil.getMethods
        .find(m =>
          m.getName == "isFileScoped" && m.getParameterCount == 1 &&
            m.getParameterTypes.head.isAssignableFrom(deleteFileClass))
        .getOrElse(throw new NoSuchMethodException("ContentFileUtil.isFileScoped(DeleteFile)"))
      isFileScoped.setAccessible(true)
      val content = contentFileClass.getMethod("content")
      val specId = contentFileClass.getMethod("specId")
      val fileSize = contentFileClass.getMethod("fileSizeInBytes")
      val keyMetadata = contentFileClass.getMethod("keyMetadata")
      val recordCount = contentFileClass.getMethod("recordCount")
      val referencedDataFile = deleteFileClass.getMethod("referencedDataFile")
      val contentOffset = deleteFileClass.getMethod("contentOffset")
      val contentSize = deleteFileClass.getMethod("contentSizeInBytes")

      val groups = rewritable.asScala.toSeq.sortBy(_._1).map { case (dataFile, fileSet) =>
        val files = fileSet match {
          case iterable: java.lang.Iterable[_] => iterable.asScala.toSeq
          case other =>
            throw new IllegalArgumentException(
              s"SparkBatchQueryScan.rewritableDeletes returned ${other.getClass.getName}, " +
                "expected an iterable DeleteFileSet")
        }
        val descriptors = files.map { value =>
          val file = value.asInstanceOf[AnyRef]
          val location = IcebergReflection.extractFileLocation(file).getOrElse {
            throw new NoSuchMethodException("DeleteFile.location()/path()")
          }
          val fileContent = content.invoke(file).toString
          val format = IcebergReflection.getFileFormat(contentFileClass, file).getOrElse {
            throw new NoSuchMethodException("ContentFile.format()")
          }
          val isScoped = isFileScoped.invoke(null, file).asInstanceOf[Boolean]
          val referenced = Option(referencedDataFile.invoke(file)).map(_.toString)
          val offset = Option(contentOffset.invoke(file))
            .map(_.asInstanceOf[java.lang.Number].longValue())
          val size = Option(contentSize.invoke(file))
            .map(_.asInstanceOf[java.lang.Number].longValue())
          val fileSizeInBytes = fileSize.invoke(file).asInstanceOf[java.lang.Number].longValue()
          val deleteRecordCount =
            recordCount.invoke(file).asInstanceOf[java.lang.Number].longValue()
          if (fileContent != "POSITION_DELETES" || format != "PARQUET" || !isScoped) {
            throw new IllegalArgumentException(
              s"Previous delete $location is not a file-scoped Parquet position delete " +
                s"(content=$fileContent, format=$format, fileScoped=$isScoped)")
          }
          if (referenced.exists(_ != dataFile) || offset.nonEmpty || size.nonEmpty) {
            throw new IllegalArgumentException(
              s"Previous delete $location has unsupported file reference or split metadata")
          }
          val metadata = Option(keyMetadata.invoke(file)).map { value =>
            val source = value.asInstanceOf[ByteBuffer].duplicate()
            val bytes = new Array[Byte](source.remaining())
            source.get(bytes)
            bytes
          }
          if (metadata.isDefined) {
            throw new IllegalArgumentException(
              s"Previous delete $location uses encryption metadata unsupported by native reads")
          }
          if (fileSizeInBytes <= 0 || deleteRecordCount < 0) {
            throw new IllegalArgumentException(
              s"Previous delete $location has invalid file size or record count " +
                s"($fileSizeInBytes bytes, $deleteRecordCount records)")
          }
          PreviousPositionDeleteFile(
            location = location,
            fileSizeInBytes = fileSizeInBytes,
            format = format,
            partitionSpecId = specId.invoke(file).asInstanceOf[java.lang.Number].intValue(),
            keyMetadata = metadata,
            referencedDataFile = referenced,
            contentOffset = offset,
            contentSizeInBytes = size,
            recordCount = deleteRecordCount,
            originalFile = file)
        }
        PreviousPositionDeletesForDataFile(dataFile, descriptors)
      }
      val locations = groups.flatMap(_.deleteFiles.map(_.location))
      if (locations.distinct.size != locations.size) {
        throw new IllegalArgumentException(
          "Spark rewritable delete map contains the same delete file under multiple data files")
      }
      Right(groups)
    } catch {
      case NonFatal(e) =>
        Left(s"Could not resolve compatible rewritable position deletes: ${e.getMessage}")
    }

  private final case class Handles(
      writeResultBuilder: Method,
      addDataFiles: Method,
      addDeleteFiles: Method,
      addReferencedDataFiles: Method,
      addRewrittenDeleteFiles: Method,
      buildWriteResult: Method,
      deltaTaskCommitConstructor: Constructor[_],
      dataFiles: Method,
      deleteFiles: Method,
      rewrittenDeleteFiles: Method,
      referencedDataFiles: Method,
      inMemoryFileIoConstructor: Constructor[_],
      addInMemoryFile: Method,
      inMemoryInputFileConstructor: Constructor[_],
      genericManifestFileConstructor: Constructor[_],
      manifestContentField: java.lang.reflect.Field,
      manifestSnapshotIdField: Option[java.lang.reflect.Field],
      manifestReadData: Method,
      manifestReadDeletes: Method,
      contentFileCopy: Method,
      manifestReaderIterator: Method,
      manifestReaderClose: Method)

  // Values are weak too: Methods and Constructors reference their declaring Class and would
  // otherwise keep a discarded Iceberg classloader alive through the WeakHashMap value.
  private val handlesByLoader = new WeakHashMap[ClassLoader, WeakReference[Handles]]()

  private def handles(): Handles = synchronized {
    val contentFileClass = loadClass("org.apache.iceberg.ContentFile")
    val loader = contentFileClass.getClassLoader
    Option(handlesByLoader.get(loader)).flatMap(ref => Option(ref.get())) match {
      case Some(cached) => cached
      case None =>
        val resolved = resolveHandles()
        handlesByLoader.put(loader, new WeakReference(resolved))
        resolved
    }
  }

  private def resolveHandles(): Handles = {
    def accessible(method: Method): Method = {
      method.setAccessible(true)
      method
    }
    def method(clazz: Class[_], name: String, count: Int): Method =
      clazz.getMethods.find(m => m.getName == name && m.getParameterCount == count)
        .map(accessible)
        .getOrElse(throw new NoSuchMethodException(s"${clazz.getName}.$name/$count"))
    def iterableMethod(clazz: Class[_], name: String): Method =
      clazz.getMethods
        .find(m =>
          m.getName == name && m.getParameterCount == 1 &&
            m.getParameterTypes()(0).isAssignableFrom(classOf[java.lang.Iterable[_]]))
        .map(accessible)
        .getOrElse(throw new NoSuchMethodException(s"${clazz.getName}.$name(Iterable)"))
    def declaredCtor(clazz: Class[_], params: Class[_]*): Constructor[_] = {
      val ctor = clazz.getDeclaredConstructor(params: _*)
      ctor.setAccessible(true)
      ctor
    }

    val writeResultClass = loadClass("org.apache.iceberg.io.WriteResult")
    val inputFileClass = loadClass("org.apache.iceberg.io.InputFile")
    val genericManifestClass = loadClass("org.apache.iceberg.GenericManifestFile")
    val manifestFileClass = loadClass("org.apache.iceberg.ManifestFile")
    val fileIoClass = loadClass("org.apache.iceberg.io.FileIO")
    val readerClass = loadClass("org.apache.iceberg.ManifestReader")
    val contentFileClass = loadClass("org.apache.iceberg.ContentFile")
    val manifestFilesClass = loadClass("org.apache.iceberg.ManifestFiles")
    val readDeletes = manifestFilesClass.getMethods
      .find(m => m.getName == "readDeleteManifest" && m.getParameterCount == 3)
      .map(accessible)
      .getOrElse(throw new NoSuchMethodException("ManifestFiles.readDeleteManifest/3"))
    val genericManifestCtor =
      try declaredCtor(genericManifestClass, inputFileClass, classOf[Int], classOf[Long])
      catch {
        case _: NoSuchMethodException =>
          declaredCtor(genericManifestClass, inputFileClass, classOf[Int])
      }
    val contentField = genericManifestClass.getDeclaredField("content")
    contentField.setAccessible(true)
    val snapshotField =
      try {
        val field = genericManifestClass.getDeclaredField("snapshotId")
        field.setAccessible(true)
        Some(field)
      } catch {
        case _: NoSuchFieldException => None
      }
    val inputFile = loadClass("org.apache.iceberg.inmemory.InMemoryInputFile")
    val inMemoryFileIo = loadClass("org.apache.iceberg.inmemory.InMemoryFileIO")
    val readData = manifestFilesClass.getMethod("read", manifestFileClass, fileIoClass)
    val taskCommitClass =
      loadClass("org.apache.iceberg.spark.source.SparkPositionDeltaWrite$DeltaTaskCommit")
    val writeResultBuilder = method(writeResultClass, "builder", 0)
    val writeResultBuilderValue = writeResultBuilder.invoke(null)
    val actualBuilderClass = writeResultBuilderValue.getClass
    val writeResultBuilderMethods = Seq(
      "addDataFiles",
      "addDeleteFiles",
      "addReferencedDataFiles",
      "addRewrittenDeleteFiles").map(iterableMethod(actualBuilderClass, _))
    val iteratorMethod = readerClass.getMethod("iterator")
    val closeMethod = readerClass.getMethod("close")
    Handles(
      writeResultBuilder = writeResultBuilder,
      addDataFiles = writeResultBuilderMethods(0),
      addDeleteFiles = writeResultBuilderMethods(1),
      addReferencedDataFiles = writeResultBuilderMethods(2),
      addRewrittenDeleteFiles = writeResultBuilderMethods(3),
      buildWriteResult = actualBuilderClass.getMethod("build"),
      deltaTaskCommitConstructor = declaredCtor(taskCommitClass, writeResultClass),
      dataFiles = findMethodInHierarchy(taskCommitClass, "dataFiles").getOrElse(
        throw new NoSuchMethodException("DeltaTaskCommit.dataFiles")),
      deleteFiles = findMethodInHierarchy(taskCommitClass, "deleteFiles").getOrElse(
        throw new NoSuchMethodException("DeltaTaskCommit.deleteFiles")),
      rewrittenDeleteFiles = findMethodInHierarchy(taskCommitClass, "rewrittenDeleteFiles")
        .getOrElse(throw new NoSuchMethodException("DeltaTaskCommit.rewrittenDeleteFiles")),
      referencedDataFiles = findMethodInHierarchy(taskCommitClass, "referencedDataFiles")
        .getOrElse(throw new NoSuchMethodException("DeltaTaskCommit.referencedDataFiles")),
      inMemoryFileIoConstructor = declaredCtor(inMemoryFileIo),
      addInMemoryFile = inMemoryFileIo.getMethod(
        "addFile",
        classOf[String],
        classOf[Array[Byte]]),
      inMemoryInputFileConstructor = inputFile.getConstructor(classOf[String], classOf[Array[Byte]]),
      genericManifestFileConstructor = genericManifestCtor,
      manifestContentField = contentField,
      manifestSnapshotIdField = snapshotField,
      manifestReadData = accessible(readData),
      manifestReadDeletes = readDeletes,
      contentFileCopy = contentFileClass.getMethod("copy"),
      manifestReaderIterator = accessible(iteratorMethod),
      manifestReaderClose = accessible(closeMethod))
  }

  /** Returns the first missing driver-side reflective contract, if any. */
  def driverReflectionUnresolved(): Option[String] = probeReflection()

  /** Returns the first missing executor-side reflective contract, if any. */
  def executorReflectionUnresolved(): Option[String] = probeReflection()

  private def probeReflection(): Option[String] =
    try {
      handles()
      None
    } catch {
      case NonFatal(e) =>
        Some(
          s"Iceberg delta reflection did not resolve: ${e.getClass.getSimpleName}: ${e.getMessage}")
    }

  def buildDeltaWriteResult(
      dataFiles: java.lang.Iterable[AnyRef],
      deleteFiles: java.lang.Iterable[AnyRef],
      referencedDataFiles: java.lang.Iterable[CharSequence],
      rewrittenDeleteFiles: java.lang.Iterable[AnyRef]): AnyRef = {
    val h = handles()
    val builder = h.writeResultBuilder.invoke(null)
    h.addDataFiles.invoke(builder, dataFiles)
    h.addDeleteFiles.invoke(builder, deleteFiles)
    h.addReferencedDataFiles.invoke(builder, referencedDataFiles)
    h.addRewrittenDeleteFiles.invoke(builder, rewrittenDeleteFiles)
    h.buildWriteResult.invoke(builder).asInstanceOf[AnyRef]
  }

  def buildDeltaTaskCommit(writeResult: AnyRef): WriterCommitMessage =
    handles().deltaTaskCommitConstructor.newInstance(writeResult).asInstanceOf[WriterCommitMessage]

  def deltaTaskCommitCreatedFileLocations(message: WriterCommitMessage): Seq[String] = {
    if (message == null ||
      !message.getClass.getName.contains("SparkPositionDeltaWrite$DeltaTaskCommit")) {
      return Nil
    }
    val h = handles()
    Seq(h.dataFiles, h.deleteFiles).flatMap { accessor =>
      accessor.invoke(message) match {
        case files: Array[_] =>
          files.toSeq.flatMap(file => IcebergReflection.extractFileLocation(file))
        case files: java.lang.Iterable[_] =>
          import scala.jdk.CollectionConverters._
          files.asScala.toSeq.flatMap(file => IcebergReflection.extractFileLocation(file))
        case _ => Nil
      }
    }
  }

  def decodeTransportManifest(
      manifest: TransportManifest,
      specsById: java.util.Map[Integer, AnyRef]): java.util.List[AnyRef] = {
    if (!specsById.containsKey(Integer.valueOf(manifest.specId))) {
      throw new IllegalArgumentException(
        s"Iceberg delta transport references unknown partition spec id ${manifest.specId}")
    }
    if (manifest.content == DataManifestContent) {
      return readManifest(manifest, specsById, deleteContent = false)
    }
    if (manifest.content == DeleteManifestContent) {
      return readManifest(manifest, specsById, deleteContent = true)
    }
    throw new IllegalArgumentException(
      s"Unknown Iceberg transport manifest content ${manifest.content}")
  }

  private def readManifest(
      transport: TransportManifest,
      specsById: java.util.Map[Integer, AnyRef],
      deleteContent: Boolean): java.util.List[AnyRef] = {
    val h = handles()
    val contentName = if (deleteContent) "delete" else "data"
    val location = s"memory:comet-$contentName-manifest-${java.util.UUID.randomUUID()}.avro"
    val inMemoryFileIo = h.inMemoryFileIoConstructor.newInstance().asInstanceOf[AnyRef]
    h.addInMemoryFile.invoke(inMemoryFileIo, location, transport.avroBytes)
    val inputFile = h.inMemoryInputFileConstructor
      .newInstance(location, transport.avroBytes)
      .asInstanceOf[AnyRef]
    val manifestFile =
      if (h.genericManifestFileConstructor.getParameterCount == 3) {
        h.genericManifestFileConstructor
          .newInstance(inputFile, Integer.valueOf(transport.specId), java.lang.Long.valueOf(0L))
          .asInstanceOf[AnyRef]
      } else {
        h.genericManifestFileConstructor
          .newInstance(inputFile, Integer.valueOf(transport.specId))
          .asInstanceOf[AnyRef]
      }
    h.manifestSnapshotIdField.foreach(_.set(manifestFile, java.lang.Long.valueOf(0L)))
    if (deleteContent) {
      val deleteValue = h.manifestContentField.getType.getEnumConstants
        .find(_.asInstanceOf[Enum[_]].name() == "DELETES")
        .getOrElse(throw new IllegalStateException("ManifestContent.DELETES is unavailable"))
      h.manifestContentField.set(manifestFile, deleteValue)
    }

    val reader =
      if (deleteContent) {
        h.manifestReadDeletes.invoke(null, manifestFile, inMemoryFileIo, specsById)
      } else {
        h.manifestReadData.invoke(null, manifestFile, inMemoryFileIo)
      }
    val result = new java.util.ArrayList[AnyRef]()
    try {
      val iterator = h.manifestReaderIterator
        .invoke(reader)
        .asInstanceOf[java.util.Iterator[AnyRef]]
      while (iterator.hasNext) result.add(h.contentFileCopy.invoke(iterator.next()))
      result
    } finally {
      h.manifestReaderClose.invoke(reader)
    }
  }
}
