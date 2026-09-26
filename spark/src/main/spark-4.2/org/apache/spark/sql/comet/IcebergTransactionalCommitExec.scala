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

package org.apache.spark.sql.comet

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.transactions.TransactionUtils
import org.apache.spark.sql.connector.catalog.transactions.Transaction
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.TransactionalExec

/** Spark 4.2 transaction-aware view of Comet's existing driver-side commit node. */
private[org] final class IcebergTransactionalCommitExec private (
    delegate: IcebergCommitExec,
    override val transaction: Option[Transaction])
    extends IcebergCommitExec(
      delegate.batchWrite,
      delegate.write,
      delegate.refreshCache,
      delegate.child,
      delegate.createdFilesExtractor,
      delegate.command,
      delegate.tableName)
    with TransactionalExec {

  override def withTransaction(txn: Option[Transaction]): SparkPlan = copyWith(transaction = txn)

  override protected def run(): Seq[InternalRow] =
    runWithHooks(() => transaction.foreach(TransactionUtils.commit))

  override protected def withNewChildInternal(
      newChild: SparkPlan): IcebergTransactionalCommitExec =
    copyWith(child = newChild)

  private def copyWith(
      child: SparkPlan = this.child,
      transaction: Option[Transaction] = this.transaction): IcebergTransactionalCommitExec =
    new IcebergTransactionalCommitExec(
      IcebergCommitExec(
        batchWrite,
        write,
        refreshCache,
        child,
        createdFilesExtractor,
        command,
        tableName),
      transaction)
}

private[org] object IcebergTransactionalCommitExec {
  def apply(commit: IcebergCommitExec): SparkPlan =
    new IcebergTransactionalCommitExec(commit, None)
}
