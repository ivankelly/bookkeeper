package org.apache.bookkeeper.bookie.lsmindex;

import java.io.File;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.google.protobuf.ByteString;
import java.util.Comparator;

public class DB {
    MemTable currentMem;
    MemTable flushingMem;
    ReadWriteLock flushLock;
    final Manifest manifest;
    final Compactor compactor;
    final Comparator<ByteString> keyComparator;

    public DB(Comparator<ByteString> keyComparator, File tablesDir) {
        this.keyComparator = keyComparator;

        currentMem = new MemTable(keyComparator);
        flushingMem = new MemTable(keyComparator);
        flushLock = new ReentrantReadWriteLock();

        manifest = new Manifest(keyComparator);
        compactor = new Compactor(manifest, keyComparator, tablesDir);
    }

    public void start() {
        compactor.start();
    }
    
    public void put(ByteString key, ByteString value)
            throws InterruptedException {
        flushLock.readLock().lock();
        try {
            currentMem.put(key, value);
        } finally {
            flushLock.readLock().unlock();
        }

        if (currentMem.size() > MemTable.MAX_MEMTABLE_SIZE) {
            flushLock.writeLock().lock();
            try {
                if (currentMem.size() > MemTable.MAX_MEMTABLE_SIZE) {
                    compactor.flushMemtable(currentMem.scan());
                }
                flushingMem = currentMem;
                currentMem = new MemTable(keyComparator);
            } finally {
                flushLock.writeLock().unlock();
            }
        }
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
