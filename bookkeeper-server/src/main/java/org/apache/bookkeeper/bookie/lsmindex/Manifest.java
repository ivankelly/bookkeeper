package org.apache.bookkeeper.bookie.lsmindex;

import java.io.File;
import java.io.IOException;

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ConcurrentModificationException;

import com.google.protobuf.ByteString;


public class Manifest {
    static class Entry {
        final int level;

        final File file;
        final ByteString firstKey;
        final ByteString lastKey;

        Entry(int level, File file, ByteString firstKey, ByteString lastKey) {
            this.level = level;
            this.file = file;
            this.firstKey = firstKey;
            this.lastKey = lastKey;
        }

        File getFile() {
            return file;
        }

        int getLevel() {
            return level;
        }

        ByteString getFirstKey() {
            return firstKey;
        }

        ByteString getLastKey() {
            return lastKey;
        }
    }

    final Comparator<ByteString> keyComparator;
    final Comparator<Entry> entryComp = new Comparator<Entry>() {
            @Override
            public int compare(Entry e1, Entry e2) {
                if (e1.getLevel() == 0 && e2.getLevel() == 0) {
                    throw new RuntimeException("FIXME");
                }
                if (e1.getLevel() != e2.getLevel()) {
                    return e1.getLevel() - e2.getLevel();
                }
                return keyComparator.compare(e1.getFirstKey(), e2.getFirstKey());
            }
        };

    final ConcurrentHashMap<Integer, SortedSet<Entry>> levels;

    Manifest(Comparator<ByteString> keyComparator) {
        this.keyComparator = keyComparator;
        levels = new ConcurrentHashMap<Integer, SortedSet<Entry>>();
    }

    int getNumLevels() {
        return levels.size();
    }

    boolean hasLevel(Integer level) {
        return levels.containsKey(level);
    }

    SortedSet<Entry> getLevel(Integer level) {
        SortedSet<Entry> levelSet =  new TreeSet<Entry>(entryComp);
        levelSet.addAll(levels.get(level));
        return levelSet;
    }

    SortedSet<Entry> getEntriesForRange(Integer level, ByteString firstKey, ByteString lastKey) {
        SortedSet<Entry> levelSet = levels.get(level);
        SortedSet<Entry> entries = new TreeSet<Entry>();
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
    
    void newLevel(Integer level, Entry newEntry) throws IOException {
        // log changes
        SortedSet<Entry> levelSet = new TreeSet<Entry>(entryComp);
        levelSet.add(newEntry);

        SortedSet<Entry> prev = levels.putIfAbsent(level, levelSet);
        if (prev != null) {
            throw new ConcurrentModificationException(
                    "Level already exists. Only one thread should be modifying manifest");
        }
    }

    void addToLevel(Integer level, Entry newEntry) throws IOException {
        // log changes
        SortedSet<Entry> emptySet = new TreeSet<Entry>();
        SortedSet<Entry> newSet = new TreeSet<Entry>();
        newSet.add(newEntry);

        replaceInLevel(level, emptySet, newSet);
    }

    void replaceInLevel(Integer level, SortedSet<Entry> oldSet, SortedSet<Entry> newSet)
            throws IOException {
        // log changes

        SortedSet<Entry> oldLevel = levels.get(level);
        SortedSet<Entry> newLevel = new TreeSet<Entry>(entryComp);
        newLevel.addAll(oldLevel);

        newLevel.removeAll(oldSet);
        newLevel.addAll(newSet);
        if (!levels.replace(level, oldLevel, newLevel)) {
            throw new ConcurrentModificationException(
                    "Only one thread should be modifying the manifest at a time");
        }
    }

    void removeFromLevel(int level, Entry e)
            throws IOException {
        // log changes

        SortedSet<Entry> oldLevel = levels.get(level);
        SortedSet<Entry> newLevel = new TreeSet<Entry>(entryComp);
        newLevel.addAll(oldLevel);
        newLevel.remove(e);

        if (!levels.replace(level, oldLevel, newLevel)) {
            throw new ConcurrentModificationException(
                    "Only one thread should be modifying the manifest at a time");
        }
    }
}
