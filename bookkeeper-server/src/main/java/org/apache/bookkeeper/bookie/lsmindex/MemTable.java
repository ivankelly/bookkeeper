package org.apache.bookkeeper.bookie.lsmindex;

import org.apache.bookkeeper.proto.DataFormats.KeyValue;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentSkipListMap;
import com.google.protobuf.ByteString;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.io.IOException;

public class MemTable {
    final ConcurrentSkipListMap<ByteString,ByteString> map;
    final AtomicLong sizeAccum = new AtomicLong(0);
    final Comparator<ByteString> keyComparator;

    MemTable(Comparator<ByteString> keyComparator) {
        this.keyComparator = keyComparator;
        map = new ConcurrentSkipListMap<ByteString,ByteString>(keyComparator);
    }

    public void lock() {
    }

    public void unlock() {
    }

    public void put(ByteString key, ByteString value) {
        ByteString old = map.put(key, value);
        long size = key.size();
        if (old != null) {
            size -= old.size();
        }
        size += value.size();
        sizeAccum.addAndGet(size);
    }

    public ByteString get(ByteString key) {
        return map.get(key);
    }

    private static class MapEntryKeyValueIterator implements KeyValueIterator {
        KeyValue nextKV = null;
        final Iterator<Map.Entry<ByteString,ByteString>> iter;

        MapEntryKeyValueIterator(Iterator<Map.Entry<ByteString,ByteString>> iter) {
            this.iter = iter;
        }

        public synchronized boolean hasNext() throws IOException {
            if (nextKV == null) {
                if (!iter.hasNext()) {
                    return false;
                }
                Map.Entry<ByteString,ByteString> e = iter.next();
                nextKV = KeyValue.newBuilder().setKey(e.getKey()).setValue(e.getValue()).build();
            }
            return true;
        }

        public synchronized KeyValue peek() throws IOException {
            if (!hasNext()) {
                throw new IOException("No more elements");
            }
            return nextKV;
        }

        public synchronized KeyValue next() throws IOException {
            if (!hasNext()) {
                throw new IOException("No more elements");
            }
            KeyValue kv = nextKV;
            nextKV = null;
            return kv;
        }
        
        public void close() throws IOException {
            // noop
        }
    }

    public KeyValueIterator scan() {
        return new MapEntryKeyValueIterator(map.entrySet().iterator());
    }

    public KeyValueIterator scan(ByteString from, ByteString to) {
        return new MapEntryKeyValueIterator(
                map.subMap(from, true, to, true).entrySet().iterator());
    }

    public long size() {
        return sizeAccum.get();
    }
}
