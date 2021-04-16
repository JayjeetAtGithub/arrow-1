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

package org.apache.arrow.dataset.rados;

import org.apache.arrow.dataset.jni.NativeDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.dataset.file.FileFormat;

/** Java binding of the C++ RadosDatasetFactory. */
public class RadosDatasetFactory extends NativeDatasetFactory {

  public RadosDatasetFactory(BufferAllocator allocator, NativeMemoryPool memoryPool, FileFormat format, String path_to_config, String data_pool, String user_name, String cluster_name, String path) {
    super(allocator, memoryPool, createNative(format, path_to_config, data_pool, user_name, cluster_name, path, -1L, -1L));
  }

  public RadosDatasetFactory(BufferAllocator allocator, NativeMemoryPool memoryPool, FileFormat format, String path_to_config, String data_pool, String user_name, String cluster_name, String path, long startOffset, long length) {
    super(allocator, memoryPool, createNative(format, path_to_config, data_pool, user_name, cluster_name, path, startOffset, length));
  }

  private static long createNative(FileFormat format, String path_to_config, String data_pool, String user_name, String cluster_name, String path, long startOffset, long length) {
    return JniWrapper.get().makeRadosDatasetFactory(path_to_config, data_pool, user_name, cluster_name, "file://" + path, format.id(), startOffset, length);
  }

}
