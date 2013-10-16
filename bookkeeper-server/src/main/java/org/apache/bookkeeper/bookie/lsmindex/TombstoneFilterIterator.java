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

class TombstoneFilterIterator implements KeyValueIterator {
    private final static Logger LOG
        = LoggerFactory.getLogger(TombstoneFilterIterator.class);

    final static ByteString TOMBSTONE = ByteString.copyFromUtf8("70MB570N3");
    final KeyValueIterator iter;
    final Comparator<ByteString> comparator;
    ByteString lastKey = ByteString.EMPTY;

    TombstoneFilterIterator(KeyValueIterator iter) {
        comparator = KeyComparators.unsignedLexicographical();
        this.iter = iter;
    }

    @Override
    public synchronized boolean hasNext() throws IOException {
        while (iter.hasNext()
               && comparator.compare(iter.peek().getValue(), TOMBSTONE) == 0) {
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
        return iter.next();
    }

    @Override
    public void close() throws IOException {
        iter.close();
    }
}
