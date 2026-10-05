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

import java.lang.reflect.{Field, Modifier}

import org.scalatest.funsuite.AnyFunSuite

/** Verifies the actual Celeborn JAR on the test classpath satisfies native push ownership. */
class PatchedCelebornCompatibilityProof extends AnyFunSuite {

  private case class RequiredField(owner: String, name: String)

  private val required = Seq(
    RequiredField(
      "org.apache.celeborn.common.network.client.TransportClientFactory",
      "clientBootstraps"),
    RequiredField("org.apache.celeborn.common.network.client.TransportClient", "channel"),
    RequiredField(
      "org.apache.celeborn.common.network.client.TransportResponseHandler",
      "outstandingPushes"),
    RequiredField("org.apache.celeborn.client.ShuffleClientImpl", "pushDataRetryPool"))

  private def field(owner: Class[_], name: String): Field = {
    var current = owner
    while (current != null) {
      try {
        val result = current.getDeclaredField(name)
        result.setAccessible(true)
        return result
      } catch {
        case _: NoSuchFieldException => current = current.getSuperclass
      }
    }
    throw new NoSuchFieldException(s"${owner.getName}.$name")
  }

  test("required completion fields are safely published by the loaded Celeborn binary") {
    required.foreach { requiredField =>
      val owner = Class.forName(requiredField.owner, false, getClass.getClassLoader)
      val actual = field(owner, requiredField.name)
      val modifiers = actual.getModifiers
      assert(!Modifier.isStatic(modifiers), s"$actual must be an instance field")
      assert(!Modifier.isFinal(modifiers), s"$actual must not be final")
      assert(Modifier.isVolatile(modifiers), s"$actual must be volatile")
    }
  }


  test("loaded Celeborn binary exposes the direct native-frame push contract") {
    val client =
      Class.forName(
        "org.apache.celeborn.client.ShuffleClientImpl",
        false,
        getClass.getClassLoader)

    val push =
      client.getMethod(
        "pushDataDirect",
        Integer.TYPE,
        Integer.TYPE,
        Integer.TYPE,
        Integer.TYPE,
        classOf[java.nio.ByteBuffer],
        Integer.TYPE,
        Integer.TYPE,
        Integer.TYPE)
    assert(push.getReturnType == Integer.TYPE)
    assert(!Modifier.isStatic(push.getModifiers))

    val crc =
      client.getMethod(
        "computeBatchCRCDirect",
        Integer.TYPE,
        Integer.TYPE,
        Integer.TYPE,
        Integer.TYPE,
        classOf[java.nio.ByteBuffer],
        Integer.TYPE)
    assert(crc.getReturnType == java.lang.Void.TYPE)
    assert(!Modifier.isStatic(crc.getModifiers))
  }

  test("Comet accepts the loaded Celeborn binary for native push completion") {
    val client =
      Class.forName(
        "org.apache.celeborn.client.ShuffleClientImpl",
        false,
        getClass.getClassLoader)
    val source = client.getProtectionDomain.getCodeSource.getLocation.toString
    assert(
      source.contains("celeborn-client-spark-3-shaded_2.12-0.7.0"),
      s"unexpected Celeborn binary: $source")

    val reason = CelebornShufflePartitionPusher.nativePushCompletionUnavailableReason(client)
    assert(reason == null, Option(reason).getOrElse("native completion unexpectedly rejected"))
  }
}
