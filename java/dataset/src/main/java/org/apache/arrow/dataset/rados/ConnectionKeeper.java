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
abstract class ConnectionKeeper<T, U extends Connection> implements Closeable {

    protected ConcurrentHashMap<T, U> map;
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
     * Gets a connection.
     * @implNote weak retrieval operations such as this only provide eventual consistency, but are faster than their regular counterparts.
     * @see #get(Object)
     * @param key Key we used to store connection with.
     * @return Connection if found. `null` otherwise.
     */
    public @Nullable Connection getWeak(T key) {
        return map.get(key);
    }

    /**
     * Gets a connection.
     * @implNote strong retrieval operations such as this provide strong consistency, but are slower than their 'weak' counterparts.
     * @see #getWeak(Object)
     * @param key Key we used to store connection with.
     * @return Connection if found. `null` otherwise.
     */
    public @Nullable Connection get(T key) {
        synchronized (putLock) {
            return map.get(key);
        }
    }

    /**
     * Removes a connection. This also closes it.
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
    public @Nullable Connection getById(long id) {
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
