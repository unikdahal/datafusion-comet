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

package org.apache.comet

import java.lang.reflect.{InvocationHandler, Method, Proxy}

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.comet.{IcebergCommitExec, IcebergTransactionalCommitExec, OrdinaryIcebergCreatedFiles}
import org.apache.spark.sql.connector.catalog.transactions.Transaction
import org.apache.spark.sql.connector.metric.{CustomMetric, CustomTaskMetric}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage, Write}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.v2.TransactionalExec

class IcebergTransactionSuite extends CometTestBase {
  override protected def sparkConf: SparkConf = super.sparkConf

  test("Spark 4.2 transaction wrapper commits after batch write and driver metrics") {
    val events = ArrayBuffer.empty[String]
    val batchWrite = recordingBatchWrite(events)
    val write = recordingWrite(batchWrite, events)
    val base = commitExec(batchWrite, write, events)
    val transactional = IcebergTransactionalCommitExec(base).asInstanceOf[TransactionalExec]
    val attached = transactional.withTransaction(Some(recordingTransaction(events)))
    assert(attached.isInstanceOf[TransactionalExec])
    assert(attached.asInstanceOf[TransactionalExec].transaction.isDefined)

    attached.executeCollect()

    assert(events.toSeq == Seq("batchCommit", "driverMetrics", "transactionCommit", "refresh"))
  }

  test("batch commit failure posts driver metrics and skips transaction commit") {
    val events = ArrayBuffer.empty[String]
    val failure = new RuntimeException("batch commit failed")
    val batchWrite = recordingBatchWrite(events, Some(failure))
    val write = recordingWrite(batchWrite, events)
    val transactional = IcebergTransactionalCommitExec(commitExec(batchWrite, write, events))
      .asInstanceOf[TransactionalExec]
      .withTransaction(Some(recordingTransaction(events)))

    intercept[RuntimeException](transactional.executeCollect())
    assert(events.toSeq == Seq("batchCommit", "abort", "driverMetrics"))
  }

  test("transaction failure does not abort an already committed batch or refresh the cache") {
    val events = ArrayBuffer.empty[String]
    val batchWrite = recordingBatchWrite(events)
    val write = recordingWrite(batchWrite, events)
    val transactional = IcebergTransactionalCommitExec(commitExec(batchWrite, write, events))
      .asInstanceOf[TransactionalExec]
      .withTransaction(Some(recordingTransaction(
        events,
        Some(new RuntimeException("transaction commit failed")))))

    intercept[RuntimeException](transactional.executeCollect())
    assert(events.toSeq == Seq("batchCommit", "driverMetrics", "transactionCommit"))
  }

  private def commitExec(
      batchWrite: BatchWrite,
      write: Write,
      events: ArrayBuffer[String]): IcebergCommitExec =
    IcebergCommitExec(
      batchWrite,
      write,
      () => events += "refresh",
      EmptyWriteLeaf(spark.sparkContext.emptyRDD[InternalRow]),
      OrdinaryIcebergCreatedFiles,
      tableName = Some("tx_table"))

  private def recordingBatchWrite(
      events: ArrayBuffer[String],
      commitFailure: Option[Throwable] = None): BatchWrite = new BatchWrite {
    override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
      throw new UnsupportedOperationException("no task writer is expected")

    override def commit(messages: Array[WriterCommitMessage]): Unit = {
      events += "batchCommit"
      commitFailure.foreach(error => throw error)
    }

    override def abort(messages: Array[WriterCommitMessage]): Unit = {
      events += "abort"
    }
  }

  private def recordingWrite(batchWrite: BatchWrite, events: ArrayBuffer[String]): Write = new Write {
    override def toBatch: BatchWrite = batchWrite
    override def supportedCustomMetrics(): Array[CustomMetric] = Array.empty
    override def reportDriverMetrics(): Array[CustomTaskMetric] = {
      events += "driverMetrics"
      Array.empty
    }
  }

  private def recordingTransaction(
      events: ArrayBuffer[String],
      commitFailure: Option[Throwable] = None): Transaction = {
    val handler = new InvocationHandler {
      override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
        method.getName match {
          case "commit" =>
            events += "transactionCommit"
            commitFailure.foreach(error => throw error)
            null
          case "abort" =>
            events += "transactionAbort"
            null
          case "close" => null
          case "catalog" => null
          case "registerScans" => java.lang.Boolean.FALSE
          case "toString" => "RecordingTransaction"
          case "hashCode" => Int.box(System.identityHashCode(proxy))
          case "equals" => java.lang.Boolean.valueOf(proxy eq args(0))
          case other => throw new UnsupportedOperationException(s"Unexpected Transaction.$other")
        }
    }
    Proxy
      .newProxyInstance(classOf[Transaction].getClassLoader, Array(classOf[Transaction]), handler)
      .asInstanceOf[Transaction]
  }
}

private case class EmptyWriteLeaf(rdd: org.apache.spark.rdd.RDD[InternalRow]) extends LeafExecNode {
  override def output: Seq[org.apache.spark.sql.catalyst.expressions.Attribute] = Nil
  override protected def doExecute(): org.apache.spark.rdd.RDD[InternalRow] = rdd
}
