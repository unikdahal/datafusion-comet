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

package org.apache.comet.shuffle;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Optional;

/** Checks that a shared Celeborn client cannot allocate unadmitted encrypted frame copies. */
final class CelebornClientEncryption {
  private final Object client;
  private final Field cryptoHandler;

  CelebornClientEncryption(Object client) {
    this.client = client;
    this.cryptoHandler = findCryptoHandler(client.getClass());
    requireUnencrypted();
  }

  private static Field findCryptoHandler(Class<?> type) {
    for (Class<?> current = type; current != null; current = current.getSuperclass()) {
      try {
        Field field = current.getDeclaredField("cryptoHandler");
        if (Modifier.isStatic(field.getModifiers()) || field.getType() != Optional.class) {
          throw new IllegalArgumentException("Cannot inspect the Celeborn client's encryption state");
        }
        field.setAccessible(true);
        return field;
      } catch (NoSuchFieldException absent) {
        // Application wrappers can inherit the handler.
      }
    }
    // Legacy Celeborn clients have no handler; the factory also checks Spark IO encryption.
    return null;
  }

  void requireUnencrypted() {
    if (cryptoHandler == null) {
      return;
    }
    try {
      Object handler = cryptoHandler.get(client);
      if (!(handler instanceof Optional) || ((Optional<?>) handler).isPresent()) {
        // SparkCryptoHandler keeps per-thread workspace and extra frame copies without a bound.
        throw new UnsupportedOperationException(
            "Encrypted native Celeborn shuffle is not supported by bounded push admission; "
                + "use ordinary Spark shuffle with encryption enabled");
      }
    } catch (IllegalAccessException failure) {
      throw new IllegalStateException(
          "Cannot inspect the Celeborn client's encryption state", failure);
    }
  }
}
