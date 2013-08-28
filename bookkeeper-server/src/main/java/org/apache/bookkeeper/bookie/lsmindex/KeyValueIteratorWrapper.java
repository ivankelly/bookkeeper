package org.apache.bookkeeper.bookie.lsmindex;

import java.util.Iterator;
import org.apache.bookkeeper.proto.DataFormats.KeyValue;
import java.io.IOException;

public class KeyValueIteratorWrapper implements KeyValueIterator {
    final Iterator<KeyValue> toWrap;
    KeyValue nextKV = null;

    public KeyValueIteratorWrapper(Iterator<KeyValue> toWrap) {
        this.toWrap = toWrap;
    }

    @Override
    public synchronized boolean hasNext() throws IOException {
        if (nextKV == null) {
            if (!toWrap.hasNext()) {
                return false;
            }
            nextKV = toWrap.next();
        }
        return true;
    }

    @Override
    public synchronized KeyValue peek() throws IOException {
        if (!hasNext()) {
            throw new IOException("No more elements");
        }
        return nextKV;
    }

    @Override
    public synchronized KeyValue next() throws IOException {
        if (!hasNext()) {
            throw new IOException("No more elements");
        }
        KeyValue kv = nextKV;
        nextKV = null;
        return kv;
    }

    @Override
    public void close() throws IOException {
        // noop
    }
}
