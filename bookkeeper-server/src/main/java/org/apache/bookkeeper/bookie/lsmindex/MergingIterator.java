package org.apache.bookkeeper.bookie.lsmindex;

import org.apache.bookkeeper.proto.DataFormats.KeyValue;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;
import com.google.protobuf.ByteString;

class MergingIterator implements KeyValueIterator {
    final List<KeyValueIterator> origOrder;
    final PriorityQueue<KeyValueIterator> iterators;
    final Comparator<ByteString> keyComparator;
    
    IOException sortError = null;
   
    final Comparator<KeyValueIterator> iteratorComp = new Comparator<KeyValueIterator>() {
        @Override
        public int compare(KeyValueIterator o1, KeyValueIterator o2) {
            try {
                int diff = keyComparator.compare(o1.peek().getKey(), o2.peek().getKey());
                if (diff == 0) {
                    return origOrder.indexOf(o1) - origOrder.indexOf(o2);
                } else {
                    return diff;
                }
            } catch (IOException ioe) {
                if (sortError == null) {
                    sortError = ioe;
                }
                return 0;
            }
        }
    };

    /**
     * Create an iterator that iterates over the entries of all iterators in the key
     * order determined by keyComparator.
     * If two keys match, then the keys will be merged in the order that the iterator
     * that contains them was passed to the MergingIterator constructor
     * (Based on guava's merging iterator)
     */
    MergingIterator(Comparator<ByteString> keyComparator, List<KeyValueIterator> iterators)
            throws IOException {
        if (iterators.size() < 1) {
            throw new IllegalArgumentException("Must pass at least one iterator");
        }
        this.origOrder = new ArrayList<KeyValueIterator>();

        this.keyComparator = keyComparator;
        this.iterators = new PriorityQueue<KeyValueIterator>(iterators.size(), iteratorComp);
        for (KeyValueIterator i : iterators) {
            if (i.hasNext()) {
                this.origOrder.add(i);
                this.iterators.add(i);
            }
        }
    }

    @Override
    public synchronized boolean hasNext() throws IOException {
        if (sortError != null) {
            throw sortError;
        }

        return !this.iterators.isEmpty();
    }

    @Override
    public synchronized KeyValue peek() throws IOException {
        return iterators.peek().peek();
    }

    @Override
    public synchronized KeyValue next() throws IOException {
        KeyValueIterator iter = iterators.remove();
        KeyValue next = iter.next();
        if (iter.hasNext()) {
            iterators.add(iter);

            if (sortError != null) {
                throw sortError;
            }
        }
        return next;
    }

    @Override
    public void close() throws IOException {
        for (KeyValueIterator i : iterators) {
            i.close();
        }
    }
}
