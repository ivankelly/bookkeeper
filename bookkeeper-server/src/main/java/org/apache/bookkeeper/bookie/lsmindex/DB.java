package org.apache.bookkeeper.bookie.lsmindex;

import com.google.protobuf.ByteString;
import java.util.Comparator;

public class DB {
    MemTable currentMem;
    MemTable flushingMem;
    final Comparator<ByteString> keyComparator;

    public DB(Comparator<ByteString> keyComparator) {
        this.keyComparator = keyComparator;

        currentMem = new MemTable(keyComparator);
        flushingMem = new MemTable(keyComparator);
    }
    
    public void put(ByteString key, ByteString value) {
        /*        swapLock.readLock().lock();
        try {

            currentMem.lock();
            currentMem.put(key, value);
            currentMem.unlock();

        } finally {
            swapLock.readLock().unlock();
            }*/
        // get current memtable
        // put in memtable
        // if (memtable > size)
        //    write out memtable if needed
        // this is the throttle point
    }

    public ByteString get(ByteString key) {
        // choose files based on keys
        // add files to Iterator
        // .next()
        // return value
        return null;
    }

    public KeyValueIterator scan(ByteString from, ByteString to) {
        // choose files based on keys
        // add files to Iterator
        return null;
    }

    // gaurantee that everything added has hit disk
    // i.e. flush all memstores
    public void sync() {
    }

    public void startCompacting() {};
    public void pauseCompacting() {};
}
