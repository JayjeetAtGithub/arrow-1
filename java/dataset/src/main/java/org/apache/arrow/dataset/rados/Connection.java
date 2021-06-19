package org.apache.arrow.dataset.rados;

import java.io.Closeable;
import java.io.IOException;

public abstract class Connection implements Closeable {
    protected long id;

    public Connection(long id) {
        this.id = id;
    }

    public long id() {
        return id;
    }

    @Override
    public void close() throws IOException {
        org.apache.arrow.dataset.jni.JniWrapper.get().releaseBuffer(this.id);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Connection that = (Connection) o;
        return id == that.id;
    }
}