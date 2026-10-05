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

import java.util.Arrays;
import org.roaringbitmap.longlong.Roaring64Bitmap;

/**
 * Plain Spark (Comet disabled): the same id streams as the native benchmark, run through what
 * Spark's MergeRowsExec.BitmapCardinalityValidator does per matched row, contains then add on a
 * Roaring64Bitmap. Benchmark branch only.
 */
public class SparkCardinalityBench {
  static final int IDS = 5_000_000;
  static final int ROUNDS = 5;

  static long[] stride(long stride) {
    long[] ids = new long[IDS];
    for (int i = 0; i < IDS; i++) ids[i] = i * stride;
    return ids;
  }

  static long[] spark(long partitions) {
    long[] ids = new long[IDS];
    for (int i = 0; i < IDS; i++) ids[i] = ((i % partitions) << 33) + i / partitions;
    return ids;
  }

  // The same Fisher-Yates and linear congruential generator as the native benchmark.
  static long[] shuffled(long[] ids) {
    long state = 0x9e3779b97f4a7c15L;
    for (int i = ids.length - 1; i >= 1; i--) {
      state = state * 6364136223846793005L + 1442695040888963407L;
      int j = (int) Long.remainderUnsigned(state >>> 33, i + 1);
      long t = ids[i];
      ids[i] = ids[j];
      ids[j] = t;
    }
    return ids;
  }

  static long usedHeap() {
    Runtime rt = Runtime.getRuntime();
    for (int i = 0; i < 3; i++) System.gc();
    return rt.totalMemory() - rt.freeMemory();
  }

  static long[] run(long[] ids) {
    Roaring64Bitmap matched = new Roaring64Bitmap();
    long start = System.nanoTime();
    for (long id : ids) {
      if (matched.contains(id)) throw new IllegalStateException("duplicate " + id);
      matched.add(id);
    }
    long elapsed = System.nanoTime() - start;
    return new long[] {elapsed, matched.getLongSizeInBytes(), matched.getLongCardinality()};
  }

  static double mib(long bytes) {
    return bytes / (1024.0 * 1024.0);
  }

  public static void main(String[] args) {
    String version = Roaring64Bitmap.class.getPackage().getImplementationVersion();
    System.out.println("## Plain Spark: Roaring64Bitmap contains + add per row (RoaringBitmap "
        + (version == null ? args.length > 0 ? args[0] : "unknown" : version) + ")\n");
    System.out.println("| layout | median ms | size estimate MiB | heap MiB |");
    System.out.println("|---|---:|---:|---:|");
    Object[][] layouts = {
      {"dense", stride(1)},
      {"one-in-8", stride(8)},
      {"one-in-200", stride(200)},
      {"spark-8-partitions", spark(8)},
      {"spark-200-partitions", spark(200)},
      {"spark-2k-partitions", spark(2_048)},
      {"spark-16k-partitions", spark(16_384)},
      {"shuffled-dense", shuffled(stride(1))},
      {"shuffled-spark-200", shuffled(spark(200))},
    };
    for (Object[] layout : layouts) {
      long[] ids = (long[]) layout[1];
      run(ids); // warm up the JIT
      long[] times = new long[ROUNDS];
      long size = 0;
      for (int r = 0; r < ROUNDS; r++) {
        long[] result = run(ids);
        if (result[2] != IDS) throw new IllegalStateException("lost ids");
        times[r] = result[0];
        size = result[1];
      }
      Arrays.sort(times);
      long before = usedHeap();
      Roaring64Bitmap held = new Roaring64Bitmap();
      for (long id : ids) held.add(id);
      long heap = usedHeap() - before;
      if (held.getLongCardinality() != IDS) throw new IllegalStateException("lost ids");
      System.out.printf(
          "| %s | %.1f | %.2f | %.2f |%n",
          layout[0], times[ROUNDS / 2] / 1e6, mib(size), mib(heap));
    }
  }
}
