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
import java.util.WeakHashMap

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

/** Reflection seam for Iceberg's package-private DeltaTaskCommit constructor and manifest APIs. */
object IcebergDeltaReflection {

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
      addInMemoryFile = inMemoryFileIo.getMethod("addFile", classOf[String], classOf[Array[Byte]]),
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
        Some(s"Iceberg delta reflection did not resolve: ${e.getClass.getSimpleName}: ${e.getMessage}")
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
        case files: Array[_] => files.toSeq.flatMap(IcebergReflection.extractFileLocation)
        case files: java.lang.Iterable[_] =>
          import scala.jdk.CollectionConverters._
          files.asScala.toSeq.flatMap(IcebergReflection.extractFileLocation)
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
    throw new IllegalArgumentException(s"Unknown Iceberg transport manifest content ${manifest.content}")
  }

  private def readManifest(
      transport: TransportManifest,
      specsById: java.util.Map[Integer, AnyRef],
      deleteContent: Boolean): java.util.List[AnyRef] = {
    val h = handles()
    val location =
      s"memory:comet-${if (deleteContent) "delete" else "data"}-manifest-${java.util.UUID.randomUUID()}.avro"
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
