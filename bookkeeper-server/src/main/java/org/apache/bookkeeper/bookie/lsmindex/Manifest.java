package org.apache.bookkeeper.bookie.lsmindex;

import java.io.File;
import java.io.IOException;

import java.util.Iterator;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.ConcurrentModificationException;

import javax.xml.bind.DatatypeConverter;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Manifest {
    private final static Logger LOG
        = LoggerFactory.getLogger(Manifest.class);

    static class Entry {
        final int level;

        final File file;
        final ByteString firstKey;
        final ByteString lastKey;
        final long creationOrder;

        Entry(int level, long creationOrder, File file, ByteString firstKey, ByteString lastKey) {
            this.level = level;
            this.file = file;
            this.firstKey = firstKey;
            this.lastKey = lastKey;
            this.creationOrder = creationOrder;
        }

        File getFile() {
            return file;
        }

        int getLevel() {
            return level;
        }

        long getCreationOrder() {
            return creationOrder;
        }

        ByteString getFirstKey() {
            return firstKey;
        }

        ByteString getLastKey() {
            return lastKey;
        }

        @Override
        public String toString() {
            return String.format("Entry[%d,%s,%s,%s]",
                    level, file.toString(),
                    dumpKey(firstKey),
                    dumpKey(lastKey));
        }
    }

    final Comparator<ByteString> keyComparator;
    final private Comparator<Entry> entryComp = new Comparator<Entry>() {
            @Override
            public int compare(Entry e1, Entry e2) {
                if (e1.getLevel() == 0 && e2.getLevel() == 0) {
                    // args inverted because we want newer first
                    return Long.valueOf(e2.getCreationOrder())
                        .compareTo(Long.valueOf(e1.getCreationOrder()));
                }
                if (e1.getLevel() != e2.getLevel()) {
                    return e1.getLevel() - e2.getLevel();
                }
                int ret = keyComparator.compare(e1.getFirstKey(), e2.getFirstKey());
                if (ret != 0) {
                    return ret;
                }
                return keyComparator.compare(e1.getLastKey(), e2.getLastKey());
            }
        };

    final ConcurrentHashMap<Integer, SortedSet<Entry>> levels;
    final AtomicLong fileIds = new AtomicLong(0);

    Manifest(Comparator<ByteString> keyComparator) {
        this.keyComparator = keyComparator;
        levels = new ConcurrentHashMap<Integer, SortedSet<Entry>>();
    }

    Comparator<Entry> entryComparator() {
        return entryComp;
    }

    long creationOrder() {
        return fileIds.getAndIncrement();
    }

    int getNumLevels() {
        return levels.size();
    }

    boolean hasLevel(Integer level) {
        return levels.containsKey(level);
    }

    SortedSet<Entry> getLevel(Integer level) {
        SortedSet<Entry> levelSet = new TreeSet<Entry>(entryComp);
        SortedSet<Entry> curLevel = levels.get(level);
        if (curLevel != null) {
            levelSet.addAll(curLevel);
        }
        return levelSet;
    }

    SortedSet<Entry> getEntriesForRange(Integer level, ByteString firstKey, ByteString lastKey) {
        SortedSet<Entry> entries = new TreeSet<Entry>(entryComp);
        if (!hasLevel(level)) {
            return entries;
        }
        SortedSet<Entry> levelSet = levels.get(level);
        for (Entry e : levelSet) {
            // TODO: can do less comparisons here
            if (keyComparator.compare(e.getFirstKey(), firstKey) <= 0
                && keyComparator.compare(e.getLastKey(), firstKey) >= 0) {
                // entry straddles firstKey
                entries.add(e);
            } else if (keyComparator.compare(e.getFirstKey(), lastKey) <= 0
                       && keyComparator.compare(e.getLastKey(), lastKey) >= 0) {
                // entry straddles lastKey
                entries.add(e);
            } else if (keyComparator.compare(e.getFirstKey(), firstKey) >= 0
                       && keyComparator.compare(e.getLastKey(), lastKey) <= 0) {
                // entry all internal
                entries.add(e);
            }
        }
        //        dumpLevel(levelSet);
        //LOG.debug("Entries in level {} overlapping {}-{}",
        //          new Object[] { level, dumpKey(firstKey), dumpKey(lastKey) });
        //dumpLevel(entries);
        return entries;
    }

    SortedSet<Entry> getEntriesForRange(ByteString firstKey, ByteString lastKey) {
        Set<Integer> keys = new TreeSet<Integer>(levels.keySet());
        
        SortedSet<Entry> entries = new TreeSet<Entry>(entryComp);
        for (Integer k : keys) {
            entries.addAll(getEntriesForRange(k, firstKey, lastKey));
        }

        if (levels.size() > keys.size()) {
            // new level has been added, start again
            return getEntriesForRange(firstKey, lastKey);
        }
        return entries;
    }
    
    void newLevel(Integer level) throws IOException {
        // log changes
        if (level != 0
            && !hasLevel(level - 1)) {
            throw new IOException("Skipped level, can't add level"+level
                                  +" when level"+(level-1)+" doesn't exist");
        }
        SortedSet<Entry> levelSet = new TreeSet<Entry>(entryComp);
        SortedSet<Entry> prev = levels.putIfAbsent(level, levelSet);
        if (prev != null) {
            throw new ConcurrentModificationException(
                    "Level already exists. Only one thread should be modifying manifest");
        }
    }

    void addToLevel(Integer level, Entry newEntry) throws IOException {
        if (!hasLevel(level)) {
            newLevel(level);
        }
        // log changes
        SortedSet<Entry> emptySet = new TreeSet<Entry>(entryComp);
        SortedSet<Entry> newSet = new TreeSet<Entry>(entryComp);
        newSet.add(newEntry);

        replaceInLevel(level, emptySet, newSet);
    }

    void replaceInLevel(Integer level, SortedSet<Entry> oldSet, SortedSet<Entry> newSet)
            throws IOException {
        // log changes
        if (!hasLevel(level)) {
            newLevel(level);
        }
        //LOG.debug("OldSet ");
        //dumpLevel(oldSet);

        //LOG.debug("NewSet ");
        //dumpLevel(newSet);

        SortedSet<Entry> oldLevel = levels.get(level);
        SortedSet<Entry> newLevel = new TreeSet<Entry>(entryComp);
        newLevel.addAll(oldLevel);
        if (!oldLevel.containsAll(oldSet)) {
            throw new ConcurrentModificationException("Should contain all previous entries");
        }
        newLevel.removeAll(oldSet);

        if (level != 0) {
            for (Entry newEntry : newSet) {
                if (newLevel.contains(newEntry)) {
                    throw new IOException("Entry already exists in level, cant add " + newEntry);
                }
            }
        }
        newLevel.addAll(newSet);
        validateLevel(level, newLevel);
        if (!levels.replace(level, oldLevel, newLevel)) {
            throw new ConcurrentModificationException(
                    "Only one thread should be modifying the manifest at a time");
        }
    }

    void removeFromLevel(int level, SortedSet<Entry> oldSet)
            throws IOException {
        // log changes

        SortedSet<Entry> oldLevel = levels.get(level);
        SortedSet<Entry> newLevel = new TreeSet<Entry>(entryComp);
        newLevel.addAll(oldLevel);

        if (!oldLevel.containsAll(oldSet)) {
            throw new ConcurrentModificationException("Should contain all previous entries");
        }

        newLevel.removeAll(oldSet);
        validateLevel(level, newLevel);
        if (!levels.replace(level, oldLevel, newLevel)) {
            throw new ConcurrentModificationException(
                    "Only one thread should be modifying the manifest at a time");
        }
    }

    void validateLevel(int level, SortedSet<Entry> levelSet) throws IOException {
        Iterator<Entry> iter = levelSet.iterator();
        Entry prevEntry = null;
        while (iter.hasNext()) {
            Entry e = iter.next();
            if (prevEntry != null
                && level != 0
                && keyComparator.compare(prevEntry.getLastKey(), e.getFirstKey()) > -1) {
                dumpManifest();
                LOG.debug("New level for {}", level);
                dumpLevel(levelSet);

                throw new IOException("Overlapping entries in level " + prevEntry.toString()
                                      + " & " + e.toString());
            } else if (e.getLevel() != level) {
                throw new IOException("Attempting to add entry to wrong level; " + e.toString()
                                      + " to level" + level);
            }
            prevEntry = e;
        }
    }

    void dumpManifest() {
        SortedSet<Integer> keys = new TreeSet<Integer>(levels.keySet());
        
        for (Integer k : keys) {
            LOG.debug("DUMP: level {}", k);
            dumpLevel(getLevel(k));
        }
    }

    void dumpLevel(SortedSet<Entry> level) {
        int i = 0;
        for (Entry e : level) {
            i++;
            LOG.debug("DUMP:    - {}", e);
        }
        if (i == 0) {
            LOG.debug("DUMP: empty");
        }
    }

    static String dumpKey(ByteString key) {
        return DatatypeConverter.printHexBinary(key.toByteArray());
    }
}
