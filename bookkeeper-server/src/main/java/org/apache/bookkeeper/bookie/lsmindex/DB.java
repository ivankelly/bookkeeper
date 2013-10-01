package org.apache.bookkeeper.bookie.lsmindex;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.SortedSet;

import java.io.File;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.google.protobuf.ByteString;
import java.util.Comparator;
import com.google.common.io.Closeables;


public class DB {
    MemTable currentMem;
    MemTable flushingMem;
    ReadWriteLock flushLock;
    final Manifest manifest;
    final Compactor compactor;
    final Comparator<ByteString> keyComparator;

    public DB(Comparator<ByteString> keyComparator, File tablesDir)
            throws IOException {
        this.keyComparator = keyComparator;

        currentMem = new MemTable(keyComparator);
        flushingMem = new MemTable(keyComparator);
        flushLock = new ReentrantReadWriteLock();

        manifest = new Manifest(keyComparator, tablesDir);
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

    public ByteString get(ByteString key) throws IOException {
        KeyValueIterator iter = scan(key, key);
        if (iter.hasNext()) {
            return iter.next().getValue();
        }
        return null;
    }

    public KeyValueIterator scan(ByteString from, ByteString to) throws IOException {
        final List<KeyValueIterator> iterators = new ArrayList<KeyValueIterator>();
        flushLock.readLock().lock();
        try {
            iterators.add(currentMem.scan(from, to));
            iterators.add(flushingMem.scan(from, to));
        } finally {
            flushLock.readLock().unlock();
        }

        compactor.blockDeletion();
        SortedSet<Manifest.Entry> entries = manifest.getEntriesForRange(from, to);

        try {
            for (Manifest.Entry e : entries) {
                SSTableImpl t = SSTableImpl.open(e.getFile(), keyComparator);
                iterators.add(t.iterator(from, to))
            }
        } catch (IOException ioe) {
            for (KeyValueIterator i : iterators) {
                Closeables.closeQuietly(i);
            }
            throw ioe;
        } finally {
            compactor.unblockDeletion();
        }
        return new DedupeIterator(keyComparator,
                                  new MergingIterator(keyComparator, iterators));
    }

    // gaurantee that everything added has hit disk
    // i.e. flush all memstores
    public void sync() {

    }

    public void startCompacting() {};
    public void pauseCompacting() {};
}
