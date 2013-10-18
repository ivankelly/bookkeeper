package org.apache.bookkeeper.bookie.lsmindex;

import org.apache.bookkeeper.proto.DataFormats.KeyValue;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Queue;
import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;
import com.google.protobuf.ByteString;

class ConcatIterator implements KeyValueIterator {
    final Queue<KeyValueIterator> iterators;

    ConcatIterator(List<KeyValueIterator> iterators)
            throws IOException {
        if (iterators.size() < 1) {
            throw new IllegalArgumentException("Must pass at least one iterator");
        }

        this.iterators = new LinkedList<KeyValueIterator>();
        for (KeyValueIterator i : iterators) {
            if (i.hasNext()) {
                this.iterators.add(i);
            }
        }
    }

    @Override
    public synchronized boolean hasNext() throws IOException {
        while (!this.iterators.isEmpty() && !this.iterators.peek().hasNext()) {
            this.iterators.remove().close();
        }
        return !this.iterators.isEmpty() && this.iterators.peek().hasNext();
    }

    @Override
    public synchronized KeyValue peek() throws IOException {
        if (hasNext()) {
            return iterators.peek().peek();
        } else {
            throw new IOException("No more entries");
        }
    }

    @Override
    public synchronized KeyValue next() throws IOException {
        if (hasNext()) {
            return iterators.peek().next();
        } else {
            throw new IOException("No more entries");
        }
    }

    @Override
    public void close() throws IOException {
        for (KeyValueIterator i : iterators) {
            i.close();
        }
    }
}
