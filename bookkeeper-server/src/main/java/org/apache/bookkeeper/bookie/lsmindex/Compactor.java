package org.apache.bookkeeper.bookie.lsmindex;

import java.util.Comparator;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ArrayBlockingQueue;
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

    final File tablesDir;
    final Manifest manifest;

    final ArrayBlockingQueue<KeyValueIterator> memtableQueue = new ArrayBlockingQueue<KeyValueIterator>(1);

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
        Thread t = new Thread(this);
        t.start();
    }

    public void mergeEntries(int l, SortedSet<Manifest.Entry> toMerge,
                             SortedSet<Manifest.Entry> overlaps)
            throws IOException {
        SortedSet<Manifest.Entry> overlapsL2
            = new TreeSet<Manifest.Entry>(manifest.entryComparator());
        if (manifest.hasLevel(l + 2)) {
            ByteString firstKey = toMerge.first().getFirstKey();
            ByteString lastKey = toMerge.last().getLastKey();
            if (overlaps.size() > 0) {
                firstKey = overlaps.first().getFirstKey();
                lastKey = overlaps.last().getLastKey();
            }
            overlapsL2 = manifest.getEntriesForRange(l + 2, firstKey, lastKey);
        }

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
            MergingIterator merger = new MergingIterator(keyComparator,
                                                         iterators);
            DedupeIterator dedupe = new DedupeIterator(keyComparator,
                                                       merger);
            CompactingIterator iter = new CompactingIterator(keyComparator,
                    dedupe, overlapsL2.iterator());
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
            // FIXME delete the old file
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

    public void run() {
        try {
            while (true) {
                int numLevels = manifest.getNumLevels();
            
                flushMemtableIfNecessary();

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

                // LevelX compaction
                for (int l = 1; l < numLevels; l++) {
                    flushMemtableIfNecessary();

                    SortedSet<Manifest.Entry> level = manifest.getLevel(l);
                    long maxSizeForLevel = (long)(SSTABLE_MAX_SIZE * Math.pow(LEVEL_STEP_BASE, l));
                
                    if (calcLevelSize(level) > maxSizeForLevel) {
                        Manifest.Entry e = selectEntryToCompact(l, level);
                    
                        if (l == numLevels-1) {
                            // just promote file up
                            // creating new level
                            Manifest.Entry newe = new Manifest.Entry(l+1, e.getCreationOrder(),
                                    e.getFile(), e.getFirstKey(), e.getLastKey());
                            manifest.addToLevel(l+1, newe);

                            SortedSet<Manifest.Entry> entries
                                = new TreeSet<Manifest.Entry>(manifest.entryComparator());
                            entries.add(e);
                            manifest.removeFromLevel(l, entries);
                            // FIXME delete the old file
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
                                // FIXME delete the old file
                            } else {
                                SortedSet<Manifest.Entry> entries
                                    = new TreeSet<Manifest.Entry>(manifest.entryComparator());
                                entries.add(e);
                                mergeEntries(l, entries, overlaps);
                            }
                        }
                    }
                }
            }
        } catch (IOException ioe) {
            LOG.error("IOException during compaction, exiting compaction thread", ioe);
        }
    }

    void flushMemtable(KeyValueIterator memTable) throws InterruptedException {
        memtableQueue.put(memTable);
    }

    private void flushMemtableIfNecessary() throws IOException {
        KeyValueIterator iter = memtableQueue.poll();
        if (iter != null) {
            long creationOrder = manifest.creationOrder();
            File f = generateNewFile(creationOrder);
            // should probable store creation order in sstable also
            SSTableImpl newt = SSTableImpl.store(f, keyComparator, iter);

            try {
                Manifest.Entry newe = new Manifest.Entry(0, creationOrder, f,
                        newt.getFirstKey(), newt.getLastKey());

                manifest.addToLevel(0, newe);
            } finally {
                // FIXME newt.close()
            }
        }
    }
}
