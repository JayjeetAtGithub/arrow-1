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

import java.util.Objects;

/**
 * Keeper implementation for `RadosConnection` instances.
 * @see ConnectionKeeper
 */
public class RadosConnectionKeeper extends ConnectionKeeper<RadosConnectionKeeper.ConnectionCtx, RadosConnection> {

    /** Key class for `RadosConnectionKeeper`. */
    public static class ConnectionCtx {
        public String path_to_config, data_pool, user_name, cluster_name, cls_name;

        public ConnectionCtx() {}

        public ConnectionCtx(String path_to_config, String data_pool, String user_name, String cluster_name, String cls_name) {
            this.path_to_config = path_to_config;
            this.data_pool = data_pool;
            this.user_name = user_name;
            this.cluster_name = cluster_name;
            this.cls_name = cls_name;
        }

        @Override
        public int hashCode() {
            return Objects.hash(path_to_config, data_pool, user_name, cluster_name, cls_name);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConnectionCtx that = (ConnectionCtx) o;
            return this.path_to_config.equals(that.path_to_config) && this.data_pool.equals(that.data_pool) && this.user_name.equals(that.user_name) && this.cluster_name.equals(that.cluster_name) && this.cls_name.equals(that.cls_name);
        }
    }

    private static final RadosConnectionKeeper INSTANCE = new RadosConnectionKeeper();
    public static RadosConnectionKeeper get() {
        return INSTANCE;
    }
}
