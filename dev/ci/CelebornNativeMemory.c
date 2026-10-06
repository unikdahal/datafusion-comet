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

#include <jni.h>
#include <malloc.h>

JNIEXPORT jlongArray JNICALL
Java_org_apache_comet_benchmark_CelebornJvmAllocation_nativeAllocatorSnapshot(
    JNIEnv* env, jclass clazz) {
  (void)clazz;
  struct mallinfo2 memory = mallinfo2();
  jlong values[] = {(jlong)memory.arena, (jlong)memory.uordblks,
                    (jlong)memory.fordblks, (jlong)memory.hblkhd,
                    (jlong)memory.keepcost};
  jlongArray result = (*env)->NewLongArray(env, 5);
  if (result == NULL) {
    return NULL;
  }
  (*env)->SetLongArrayRegion(env, result, 0, 5, values);
  return result;
}

JNIEXPORT jboolean JNICALL
Java_org_apache_comet_benchmark_CelebornJvmAllocation_trimNativeAllocator(
    JNIEnv* env, jclass clazz) {
  (void)env;
  (void)clazz;
  return malloc_trim(0) ? JNI_TRUE : JNI_FALSE;
}
