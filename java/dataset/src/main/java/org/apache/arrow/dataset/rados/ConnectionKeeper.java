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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Abstract connection keeper implementation. Stores connections.
 * Optimized for consistent reads across threads.
 * Implementations are recommended to build singletons for thread-level consistency.
 * @param <T> Keytype to use for keeping connection.
 * @param <U> Connection-like valuetype to use.
 */
public abstract class ConnectionKeeper<T, U extends Connection> implements Closeable {

    @FunctionalInterface
    public interface Construct<T, U> {
        U construct(T t);
    }

    protected ConcurrentHashMap<T, U> map = new ConcurrentHashMap<>();
    protected final Object putLock = new Object();

    protected ConnectionKeeper() {}

    public boolean containsKey(T key) {
        return map.containsKey(key);
    }

    public boolean containsValue(@Nonnull U connection) {
        return map.containsValue(connection);
    }

    /**
     * Registers new connection only if there is no registered connection for given key.
     * @param key Key to store connection under.
     * @param connection Connection to store.
     * @return `true` when we registered the connection, `false` otherwise.
     */
    public boolean putSoft(T key, @Nonnull U connection) {
        synchronized (putLock) {
            if (this.containsKey(key))
                return false;
            map.put(key, connection);
            return true;
        }
    }

    /**
     * Registers a new connection. If a connection is already stored under given key, removes and closes old connection.
     * @param key Key to store new connection with.
     * @param connection Connection to store.
     */
    public void put(T key, @Nonnull U connection) throws IOException {
        @Nullable U old;
        synchronized (putLock) {
            old = map.put(key, connection);
        }
        if (old != null)
            old.close();
    }

    /**
     * Get a connection.
     * @implNote weak retrieval operations such as this only provide eventual consistency, but are faster than their regular counterparts.
     * @see #get(Object)
     * @param key Key we used to store connection with.
     * @return Connection if found. `null` otherwise.
     */
    public @Nullable U getWeak(T key) {
        return map.get(key);
    }

    /**
     * Get a connection.
     * @implNote strong retrieval operations such as this provide strong consistency, but are slower than their 'weak' counterparts.
     * @see #getWeak(Object)
     * @param key Key we used to store connection with.
     * @return Connection if found. `null` otherwise.
     */
    public @Nullable U get(T key) {
        synchronized (putLock) {
            return map.get(key);
        }
    }

    /**
     * Get a connection, or create.
     * @param key Key for connection to fetch or insert.
     * @param construct Construction method to create a new Connection, in case we have to create.
     * @return the retrieved or created connection.
     */
    public U getOrCreate(T key, Construct<T, U> construct) {
        @Nullable U conn;
        synchronized (putLock) {
            conn = map.get(key);
            if (conn == null) {
                conn = construct.construct(key);
                map.put(key, conn);
            }
        }
        return conn;
    }

    /**
     * Remove a connection and close it.
     * @param key Key for connection to remove.
     */
    public void remove(T key) throws IOException {
        synchronized (putLock) {
            @Nullable U old = map.remove(key);
            if (old != null)
                old.close();
        }
    }

    /**
     * Returns approximate size of connection container.
     * @implNote Only approximate size supported, as multiple threads could put new items in the container at the same time as size is requested.
     * @return Current known size of container.
     */
    public int size() {
        return map.size();
    }

    /**
     * Finds a registered connection by Id. This is a weak operation (eventually consistent).
     * @param id Id to search for.
     * @return connection with given id if found, `null` otherwise.
     * @implNote If multiple connections exist with given id (should NEVER occur), it is undefined which one is returned.
     */
    public @Nullable U getById(long id) {
         Optional<U> conn = map.values().stream().parallel().filter(connection -> connection.id() == id).findAny();
        return conn.orElse(null);
    }

    public void clear() throws IOException {
        this.close();
        map.clear();
    }

    @Override
    public void close() throws IOException {
        for (U c : map.values())
            c.close();
    }
}
