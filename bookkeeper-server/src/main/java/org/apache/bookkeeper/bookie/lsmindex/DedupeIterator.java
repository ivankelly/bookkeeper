package org.apache.bookkeeper.bookie.lsmindex;

import org.apache.bookkeeper.proto.DataFormats.KeyValue;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DedupeIterator implements KeyValueIterator {
    private final static Logger LOG
        = LoggerFactory.getLogger(DedupeIterator.class);

    final KeyValueIterator iter;
    final Comparator<ByteString> keyComparator;
    ByteString lastKey = ByteString.EMPTY;
    
    DedupeIterator(Comparator<ByteString> keyComparator, KeyValueIterator iter) {
        this.keyComparator = keyComparator;
        this.iter = iter;
    }

    @Override
    public synchronized boolean hasNext() throws IOException {
        while (iter.hasNext() && keyComparator.compare(iter.peek().getKey(), lastKey) == 0) {
            iter.next();
        }
        return iter.hasNext();
    }

    @Override
    public synchronized KeyValue peek() throws IOException {
        if (!hasNext()) {
            throw new IOException("No next entry");
        }
        return iter.peek();
    }

    @Override
    public synchronized KeyValue next() throws IOException {
        if (!hasNext()) {
            throw new IOException("No next entry");
        }
        KeyValue kv = iter.next();
        lastKey = kv.getKey();
        return kv;
    }

    @Override
    public void close() throws IOException {
        // noop
    }
}
