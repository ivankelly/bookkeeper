package org.apache.bookkeeper.bookie.lsmindex;

import java.util.Comparator;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.SortedSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;
import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Compactor implements Runnable {
    private final static Logger LOG = LoggerFactory.getLogger(Compactor.class);

    static int LEVEL_STEP_BASE = 10;
    static int SSTABLE_MAX_SIZE = 2 * 1024 * 1024; // 2Meg
    static int LEVEL0_THRESHOLD = 4;

    final Comparator<ByteString> keyComparator;
    final ReadWriteLock deletionLock = new ReentrantReadWriteLock();

    final File tablesDir;
    final Manifest manifest;

    Compactor(Manifest manifest, Comparator<ByteString> keyComparator, File tablesDir) {
        this.manifest = manifest;
        this.keyComparator = keyComparator;
        this.tablesDir = tablesDir;
    }

    private long calcLevelSize(SortedSet<Manifest.Entry> level) {
        long size = 0;
        for (Manifest.Entry e : level) {
            size += e.getFile().length();
        }
        return size;
    }

    private ConcurrentHashMap<Integer,ByteString> lastCompacted = new ConcurrentHashMap<Integer,ByteString>();
    private Manifest.Entry selectEntryToCompact(Integer levelnum, SortedSet<Manifest.Entry> level) {
        assert(level.size() > 0); // Empty level cant be added
        if (!lastCompacted.contains(levelnum)) {
            Manifest.Entry e = level.first();
            lastCompacted.put(levelnum, e.getFirstKey());
            return e;
        }
        ByteString lastCompactedKey = lastCompacted.get(levelnum);
        Iterator<Manifest.Entry> iter = level.iterator();
        while (iter.hasNext()) {
            Manifest.Entry e = iter.next();
            if (keyComparator.compare(lastCompactedKey, e.getFirstKey()) > 0) {
                lastCompacted.put(levelnum, e.getFirstKey());
                return e;
            }
        }
        Manifest.Entry e = level.first();
        lastCompacted.put(levelnum, e.getFirstKey());
        return e;
    }

    File generateNewFile(long creationOrder) {
        return new File(tablesDir, creationOrder + ".sst");
    }

    void start() {
        Thread t = new Thread(this, "LSMCompact");
        t.start();
    }

    void blockDeletion() {
        deletionLock.readLock().lock();
    }

    void unblockDeletion() {
        deletionLock.readLock().unlock();
    }

    public void mergeEntries(int l, SortedSet<Manifest.Entry> toMerge,
                             SortedSet<Manifest.Entry> overlaps)
            throws IOException {
        LOG.debug("Merging {} on level {} with {} on level {}",
                  new Object[] { toMerge, l, overlaps, l + 1 });
        ByteString firstKey = toMerge.first().getFirstKey();
        ByteString lastKey = toMerge.last().getLastKey();
        if (overlaps.size() > 0) {
            firstKey = overlaps.first().getFirstKey();
            lastKey = overlaps.last().getLastKey();
        }
        SortedSet<Manifest.Entry> overlapsL2
            = manifest.getEntriesForRange(l + 2, firstKey, lastKey);

        List<SSTableImpl> tables = new ArrayList<SSTableImpl>();
        List<KeyValueIterator> iterators = new ArrayList<KeyValueIterator>();
        try {
            for (Manifest.Entry e : toMerge) {
                SSTableImpl t = SSTableImpl.open(e.getFile(), keyComparator);
                tables.add(t);
                iterators.add(t.iterator());
            }

            for (Manifest.Entry overlap : overlaps) {
                SSTableImpl t2 = SSTableImpl.open(overlap.getFile(),
                                                  keyComparator);
                tables.add(t2);
                iterators.add(t2.iterator());
            }
            KeyValueIterator baseiter = new MergingIterator(keyComparator,
                                                         iterators);
            baseiter = new DedupeIterator(keyComparator,
                                          baseiter);
            /* If we're merging into the highest level or a
               new level, trim out the tombstones */
            if (l + 1 == manifest.getNumLevels()
                || l + 1 == manifest.getNumLevels() - 1) {
                LOG.debug("Using tombstone filter");
                baseiter = new TombstoneFilterIterator(baseiter);
            }
            CompactingIterator iter = new CompactingIterator(keyComparator,
                    baseiter, overlapsL2.iterator());
            SortedSet<Manifest.Entry> newEntries
                = new TreeSet<Manifest.Entry>(manifest.entryComparator());
            while (iter.reallyHasNext()) {
                iter.resetNewFileCondition();
                long creationOrder = manifest.creationOrder();
                File f = generateNewFile(creationOrder);

                SSTableImpl newt = SSTableImpl.store(f, keyComparator, iter);
                tables.add(newt);

                newEntries.add(new Manifest.Entry(l+1, creationOrder, f,
                                                  newt.getFirstKey(),
                                                  newt.getLastKey()));
            }
            manifest.replaceInLevel(l+1, overlaps, newEntries);
            manifest.removeFromLevel(l, toMerge);

            deletionLock.writeLock().lock();
            try {
                for (Manifest.Entry e : overlaps) {
                    if (!e.getFile().delete()) {
                        LOG.error("Couldn't delete {}", e.getFile());
                    }
                }
                for (Manifest.Entry e : toMerge) {
                    if (!e.getFile().delete()) {
                        LOG.error("Couldn't delete {}", e.getFile());
                    }
                }
            } finally {
                deletionLock.writeLock().unlock();
            }
        } finally {
            for (KeyValueIterator i : iterators) {
                try {
                    i.close();
                } catch (IOException ioe) {
                    LOG.error("Failed to close " + i
                              + ". File descriptor may have leaked", ioe);
                }
            }
            for (SSTableImpl t : tables) {
                // FIXME should I close?
            }
        }
    }

    void compactLevel0() throws IOException {
        // Level0 compaction is special
        SortedSet<Manifest.Entry> level0 = manifest.getLevel(0);
        if (level0.size() >= LEVEL0_THRESHOLD) {
            SortedSet<Manifest.Entry> toMerge
                = new TreeSet<Manifest.Entry>(manifest.entryComparator());
            Iterator<Manifest.Entry> iter = level0.iterator();
            for (int i = 0; i < level0.size() - LEVEL0_THRESHOLD; i++) {
                iter.next(); // skip newest
            }
            ByteString firstKey = null;
            ByteString lastKey = null;
            while (iter.hasNext()) {
                Manifest.Entry e = iter.next();
                if (firstKey == null
                    || keyComparator.compare(firstKey, e.getFirstKey()) > 0) {
                    firstKey = e.getFirstKey();
                }
                if (lastKey == null
                    || keyComparator.compare(lastKey, e.getLastKey()) < 0) {
                    lastKey = e.getLastKey();
                }
                toMerge.add(e);
            }
            SortedSet<Manifest.Entry> overlaps = manifest.getEntriesForRange(1,
                                                                             firstKey, lastKey);
            mergeEntries(0, toMerge, overlaps);
        }
    }

    void compactLevelX(int l) throws IOException {
        SortedSet<Manifest.Entry> level = manifest.getLevel(l);
        int numLevels = manifest.getNumLevels();
        long maxSizeForLevel = (long)(SSTABLE_MAX_SIZE * Math.pow(LEVEL_STEP_BASE, l));
                
        if (calcLevelSize(level) > maxSizeForLevel) {
            Manifest.Entry e = selectEntryToCompact(l, level);
                    
            if (l == numLevels-1) {
                // just promote file up
                // creating new level
                LOG.debug("Promoting {} to level {}", e, l+1);
                Manifest.Entry newe = new Manifest.Entry(l+1, e.getCreationOrder(),
                                                         e.getFile(), e.getFirstKey(), e.getLastKey());
                manifest.addToLevel(l+1, newe);

                SortedSet<Manifest.Entry> entries
                    = new TreeSet<Manifest.Entry>(manifest.entryComparator());
                entries.add(e);
                manifest.removeFromLevel(l, entries);
            } else {
                SortedSet<Manifest.Entry> overlaps = manifest.getEntriesForRange(l + 1,
                        e.getFirstKey(), e.getLastKey());
                if (overlaps.size() == 0) {
                    // just promote file up
                    Manifest.Entry newe = new Manifest.Entry(l+1, e.getCreationOrder(),
                                                             e.getFile(), e.getFirstKey(), e.getLastKey());
                    manifest.addToLevel(l+1, newe);

                    SortedSet<Manifest.Entry> entries
                        = new TreeSet<Manifest.Entry>(manifest.entryComparator());
                    entries.add(e);
                    manifest.removeFromLevel(l, entries);
                } else {
                    SortedSet<Manifest.Entry> entries
                        = new TreeSet<Manifest.Entry>(manifest.entryComparator());
                    entries.add(e);
                    mergeEntries(l, entries, overlaps);
                }
            }
        }
    }

    void compact() throws IOException {
        int numLevels = manifest.getNumLevels();

        flushMemtableIfNecessary();

        compactLevel0();
        // LevelX compaction
        for (int l = 1; l < numLevels; l++) {
            flushMemtableIfNecessary();

            compactLevelX(l);
        }
    }

    public void run() {
        try {
            while (true) {
                compact();
            }
        } catch (IOException ioe) {
            LOG.error("IOException during compaction, exiting compaction thread", ioe);
        }
    }

    class FlushMemtableOp implements Future<Void> {
        final KeyValueIterator memtable;
        final CountDownLatch latch = new CountDownLatch(1);

        FlushMemtableOp(KeyValueIterator iter) {
            memtable = iter;
        }

        KeyValueIterator getMemTable() {
            return memtable;
        }

        void complete() {
            latch.countDown();
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) { return false; }

        @Override
        public boolean isCancelled() { return false; }

        @Override
        public boolean isDone() {
            return latch.getCount() == 0;
        }

        @Override
        public Void get()
            throws InterruptedException, ExecutionException {
            latch.await();
            return null;
        }

        @Override
        public Void get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
            if (!latch.await(timeout, unit)) {
                throw new TimeoutException();
            }
            return null;
        }
    }

    final ArrayBlockingQueue<FlushMemtableOp> memtableQueue = new ArrayBlockingQueue<FlushMemtableOp>(1);

    Future<Void> flushMemtable(KeyValueIterator memTable) throws InterruptedException {
        FlushMemtableOp op = new FlushMemtableOp(memTable);
        memtableQueue.put(op);
        return op;
    }

    private void flushMemtableIfNecessary() throws IOException {
        FlushMemtableOp op = memtableQueue.poll();
        if (op != null) {
            long creationOrder = manifest.creationOrder();
            File f = generateNewFile(creationOrder);
            // should probable store creation order in sstable also
            SSTableImpl newt = SSTableImpl.store(f, keyComparator, op.getMemTable());

            Manifest.Entry newe = new Manifest.Entry(0, creationOrder, f,
                                                     newt.getFirstKey(), newt.getLastKey());
            manifest.addToLevel(0, newe);
            op.complete();
        }
    }
}
