package org.apache.bookkeeper.bookie.lsmindex;

import org.apache.bookkeeper.proto.DataFormats.KeyValue;
import java.io.IOException;
import java.util.Iterator;
import java.util.Comparator;
import com.google.protobuf.ByteString;

class CompactingIterator implements KeyValueIterator {

    final Comparator<ByteString> keyComparator;
    final MergingIterator iter;
    final Iterator<Manifest.Entry> level2iter;

    Manifest.Entry level2cur = null;
    int level2overlapped = 0;
    long bytesIterated = 0;
    
    KeyValue curKV;

    CompactingIterator(Comparator<ByteString> keyComparator,
                       MergingIterator iter, Iterator<Manifest.Entry> level2iter) {
        this.iter = iter;
        this.level2iter = level2iter;
        this.keyComparator = keyComparator;
        if (level2iter != null && level2iter.hasNext()) {
            level2cur = level2iter.next();
        }
    }

    public boolean reallyHasNext() throws IOException {
        return iter.hasNext();
    }

    public void resetNewFileCondition() throws IOException {
        level2overlapped = 0;
        bytesIterated = 0;
    }

    boolean atNewFileCondition() throws IOException {
        if (bytesIterated > Compactor.SSTABLE_MAX_SIZE) {
            return true;
        }
        return false;
    }

    public synchronized boolean hasNext() throws IOException {
        if (curKV == null) {
            if (atNewFileCondition()) {
                return false;
            }

            if (!iter.hasNext()) {
                return false;
            } else {
                curKV = iter.next();
            }
        }
        return true;
    }

    public synchronized KeyValue peek() throws IOException {
        if (!hasNext()) {
            throw new IOException("No more elements");
        }
        return curKV;
    }

    public synchronized KeyValue next() throws IOException {
        if (!hasNext()) {
            throw new IOException("No more elements");
        }
        KeyValue kv = curKV;
        curKV = null;

        bytesIterated += kv.getSerializedSize();

        if (level2cur != null) {
            if (keyComparator.compare(kv.getKey(), level2cur.getLastKey()) == 1) {
                level2overlapped++;
                if (level2iter.hasNext()) {
                    level2cur = level2iter.next();
                } else {
                    level2cur = null;
                }
            }
        }
        return kv;
    }
        
    public void close() throws IOException {
        iter.close();
    }
}
