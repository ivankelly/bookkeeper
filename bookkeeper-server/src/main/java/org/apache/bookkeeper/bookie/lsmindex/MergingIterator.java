package org.apache.bookkeeper.bookie.lsmindex;

import org.apache.bookkeeper.proto.DataFormats.KeyValue;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import com.google.protobuf.ByteString;

class MergingIterator implements KeyValueIterator {
    final List<KeyValueIterator> origOrder;
    final List<KeyValueIterator> iterators;
    final Comparator<ByteString> keyComparator;
    
    boolean sorted = false;
    IOException sortError = null;
   
    final Comparator<KeyValueIterator> iteratorComp = new Comparator<KeyValueIterator>() {
        @Override
        public int compare(KeyValueIterator o1, KeyValueIterator o2) {
            try {
                if (!o1.hasNext()) {
                    return 1;
                } else if (!o2.hasNext()) {
                    return -1;
                }
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
     */
    MergingIterator(Comparator<ByteString> keyComparator, List<KeyValueIterator> iterators) {
        if (iterators.size() < 1) {
            throw new IllegalArgumentException("Must pass at least one iterator");
        }
        this.origOrder = new ArrayList<KeyValueIterator>(iterators);
        this.iterators = iterators;
        this.keyComparator = keyComparator;
    }

    @Override
    public synchronized boolean hasNext() throws IOException {
        if (!sorted) {
            Collections.sort(iterators, iteratorComp);
            if (sortError != null) {
                throw sortError;
            }
            sorted = true;
        }

        return iterators.get(0).hasNext();
    }

    @Override
    public synchronized KeyValue peek() throws IOException {
        if (!hasNext()) {
            throw new IOException("No next entry");
        }
        return iterators.get(0).peek();
    }

    @Override
    public synchronized KeyValue next() throws IOException {
        if (!hasNext()) {
            throw new IOException("No next entry");
        }
        sorted = false;
        return iterators.get(0).next();
    }

    @Override
    public void close() throws IOException {
        for (KeyValueIterator i : iterators) {
            i.close();
        }
    }

}
