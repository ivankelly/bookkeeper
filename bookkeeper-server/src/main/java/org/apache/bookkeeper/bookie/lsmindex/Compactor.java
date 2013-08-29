package org.apache.bookkeeper.bookie.lsmindex;

import java.util.Comparator;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.SortedSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;
import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Compactor {
    private final static Logger LOG = LoggerFactory.getLogger(Compactor.class);

    static int LEVEL_STEP_BASE = 10;
    static int SSTABLE_MAX_SIZE = 2 * 1024 * 1024; // 2Meg

    final Comparator<ByteString> keyComparator;
    final File tablesDir;
    final Manifest manifest;

    Compactor(Manifest manifest, Comparator<ByteString> keyComparator, File tablesDir) {
        this.manifest = manifest;
        this.keyComparator = keyComparator;
        this.tablesDir = tablesDir;
    }

    long calcLevelSize(SortedSet<Manifest.Entry> level) {
        long size = 0;
        for (Manifest.Entry e : level) {
            size += e.getFile().length();
        }
        return size;
    }

    ConcurrentHashMap<Integer,ByteString> lastCompacted = new ConcurrentHashMap<Integer,ByteString>();
    Manifest.Entry selectEntryToCompact(Integer levelnum, SortedSet<Manifest.Entry> level) {
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

    File generateNewFile() {
        while (true) {
            File f = new File(tablesDir, System.currentTimeMillis() + ".sst");
            if (!f.exists()) {
                return f;
            }
        }
    }

    public void run() {
        try {
            while (true) {
                int numLevels = manifest.getNumLevels();
            
                for (int l = 1; l < numLevels; l++) {
                    // flush memtable if necessary
                    SortedSet<Manifest.Entry> level = manifest.getLevel(l);
                    long maxSizeForLevel = (long)(SSTABLE_MAX_SIZE * Math.pow(LEVEL_STEP_BASE, l));
                
                    if (calcLevelSize(level) > maxSizeForLevel) {
                        Manifest.Entry e = selectEntryToCompact(l, level);
                    
                        if (l == numLevels-1) {
                            // just promote file up
                            // creating new level
                            Manifest.Entry newe = new Manifest.Entry(l+1, e.getFile(),
                                                                 e.getFirstKey(), e.getLastKey());
                            manifest.newLevel(l+1, newe);
                            manifest.removeFromLevel(l, e);
                            // FIXME delete the old file
                        } else {
                            SortedSet<Manifest.Entry> overlaps = manifest.getEntriesForRange(l + 1,
                                    e.getFirstKey(), e.getLastKey());
                            if (overlaps.size() == 0) {
                                // just promote file up
                                Manifest.Entry newe = new Manifest.Entry(l+1, e.getFile(),
                                        e.getFirstKey(), e.getLastKey());
                                manifest.addToLevel(l+1, newe);
                                manifest.removeFromLevel(l, e);
                                // FIXME delete the old file
                            } else {
                                SortedSet<Manifest.Entry> overlapsL2 = null;
                                if (l + 2 <= numLevels - 1) {
                                    overlapsL2 = manifest.getEntriesForRange(l + 2,
                                            overlaps.first().getFirstKey(),
                                            overlaps.last().getLastKey());
                                }

                                List<SSTableImpl> tables = new ArrayList<SSTableImpl>();
                                List<KeyValueIterator> iterators = new ArrayList<KeyValueIterator>();
                                try {
                                    SSTableImpl t = SSTableImpl.open(e.getFile(), keyComparator);
                                    tables.add(t);
                                    iterators.add(t.iterator());

                                    for (Manifest.Entry overlap : overlaps) {
                                        SSTableImpl t2 = SSTableImpl.open(overlap.getFile(),
                                                                          keyComparator);
                                        tables.add(t2);
                                        iterators.add(t2.iterator());
                                    }
                                    MergingIterator merger = new MergingIterator(keyComparator,
                                                                                 iterators);
                                    CompactingIterator iter = new CompactingIterator(keyComparator,
                                            merger, overlapsL2.iterator());
                                    SortedSet<Manifest.Entry> newEntries
                                        = new TreeSet<Manifest.Entry>();
                                    while (iter.reallyHasNext()) {
                                        iter.resetNewFileCondition();
                                        File f = generateNewFile();
                                        SSTableImpl newt = SSTableImpl.store(f, keyComparator, iter);
                                        tables.add(newt);

                                        newEntries.add(new Manifest.Entry(l+1, f,
                                                                          newt.getFirstKey(),
                                                                          newt.getLastKey()));
                                    }
                                    manifest.replaceInLevel(l+1, overlaps, newEntries);
                                    manifest.removeFromLevel(l, e);
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
                        }
                    }
                }
            }
        } catch (IOException ioe) {
            LOG.error("IOException during compaction, exiting compaction thread");
        }
    }
}
