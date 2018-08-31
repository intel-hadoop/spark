/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.unsafe;

import com.google.common.base.Preconditions;
import org.scijava.nativelib.NativeLoader;
import sun.misc.Cleaner;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

/**
 * A platform used to allocate/free volatile memory from Intel Optane DC persistent memory.
 */
public class PMPlatform {
  private static volatile boolean initialized = false;

  static {
    try {
      NativeLoader.loadLibrary("pmplatform");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Initialize the persistent memory.
   * @param path The initial path which should be a directory.
   * @param size The initial size
   */
  public static void initialize(String path, long size) {
    synchronized (PMPlatform.class) {
      if (!initialized) {
        Preconditions.checkNotNull(path, "Persistent memory initial path can't be null");
        File dir = new File(path);
        Preconditions.checkArgument(dir.exists() && dir.isDirectory(), "Persistent memory " +
                "initial path should be a directory");
        Preconditions.checkArgument(size > 0,
                "Persistent memory initial size must be a positive number");
        try {
          initializeNative(path, size);
        } catch (Exception e) {
          throw new RuntimeException("Persistent memory initialize (path: " +
                  path + ", size: " + size + ") failed. Please check the path permission.");
        }
        initialized = true;
      }
    }
  }

  private static native void initializeNative(String path, long size);

  /**
   * Allocate memory from persistent memory.
   * @param size the requested size
   * @return the address which same as Platform.allocateMemory, it can be operated by
   * Platform which same as OFF_HEAP memory.
   */
  public static native long allocateMemory(long size);

  /**
   * Allocate given size memory from persistent memory and wrap with a DirectBuffer.
   * Same behaviour as OFF_HEAP DRAME memory.
   */
  @SuppressWarnings("unchecked")
  public static ByteBuffer allocateDirectBuffer(int size) {
    try {
      Class<?> cls = Class.forName("java.nio.DirectByteBuffer");
      Constructor<?> constructor = cls.getDeclaredConstructor(Long.TYPE, Integer.TYPE);
      constructor.setAccessible(true);
      Field cleanerField = cls.getDeclaredField("cleaner");
      cleanerField.setAccessible(true);
      long memory = allocateMemory(size);
      ByteBuffer buffer = (ByteBuffer) constructor.newInstance(memory, size);
      Cleaner cleaner = Cleaner.create(buffer, () -> freeMemory(memory));
      cleanerField.set(buffer, cleaner);
      return buffer;
    } catch (Exception e) {
      Platform.throwException(e);
    }
    throw new IllegalStateException("unreachable");
  }

  /**
   * Free the memory by address.
   */
  public static native void freeMemory(long address);
}
