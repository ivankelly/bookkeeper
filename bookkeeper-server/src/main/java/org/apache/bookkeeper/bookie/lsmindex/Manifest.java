package org.apache.bookkeeper.bookie.lsmindex;

import java.io.File;
import java.io.IOException;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.ConcurrentModificationException;

import org.apache.bookkeeper.proto.DataFormats.ManifestCurrent;

import javax.xml.bind.DatatypeConverter;

import com.google.common.io.Closeables;
import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Manifest implements Closeable {
    private final static Logger LOG
        = LoggerFactory.getLogger(Manifest.class);

    final int MAX_TRANSFORMS_PER_LOG = 100000;
    final String CURRENT_FILE = "MANIFEST.ptr";
    final String MANIFEST_NAME = "MANIFEST";

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
    final Comparator<Entry> entryComp;

    final ConcurrentHashMap<Integer, SortedSet<Entry>> levels;
    final AtomicLong creationOrder = new AtomicLong(0);
    final File manifestDir;
    long appliedTransforms = 0;
    ManifestLog manifestLog;

    Manifest(Comparator<ByteString> keyComparator, File manifestDir)
            throws IOException {
        this.keyComparator = keyComparator;
        this.entryComp = new ManifestEntryComparator(keyComparator);
        levels = new ConcurrentHashMap<Integer, SortedSet<Entry>>();

        this.manifestDir = manifestDir;
        replayManifestLog(manifestDir);
        manifestLog = newManifestLog(manifestDir);
    }

    @Override
    public void close() throws IOException {
        manifestLog.close();
    }

    Comparator<Entry> entryComparator() {
        return entryComp;
    }

    long creationOrder() {
        return creationOrder.getAndIncrement();
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
        SortedSet<Entry> emptySet = new TreeSet<Entry>(entryComp);
        SortedSet<Entry> newSet = new TreeSet<Entry>(entryComp);
        newSet.add(newEntry);

        replaceInLevel(level, emptySet, newSet);
    }

    synchronized void replaceInLevel(Integer level, SortedSet<Entry> oldSet, SortedSet<Entry> newSet)
            throws IOException {
        manifestLog.log(level, oldSet, newSet);
        applyTransform(level, oldSet, newSet);

        appliedTransforms++;
        if (appliedTransforms > MAX_TRANSFORMS_PER_LOG) {
            manifestLog = newManifestLog(manifestDir);
        }
    }

    private void applyTransform(Integer level, SortedSet<Entry> oldSet, SortedSet<Entry> newSet)
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
            throw new InvalidMutationException("Should contain all previous entries");
        }
        newLevel.removeAll(oldSet);

        if (level != 0) {
            for (Entry newEntry : newSet) {
                if (newLevel.contains(newEntry)) {
                    throw new InvalidMutationException("Entry already exists in level, cant add " + newEntry);
                }
            }
        }
        newLevel.addAll(newSet);
        validateLevel(level, newLevel);

        if (!levels.replace(level, oldLevel, newLevel)) {
            throw new InvalidMutationException(
                    "Only one thread should be modifying the manifest at a time");
        }
    }

    void removeFromLevel(int level, SortedSet<Entry> oldSet)
            throws IOException {
        SortedSet<Entry> emptySet = new TreeSet<Entry>(entryComp);

        replaceInLevel(level, oldSet, emptySet);
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

                throw new InvalidMutationException("Overlapping entries in level " + prevEntry.toString()
                                      + " & " + e.toString());
            } else if (e.getLevel() != level) {
                throw new InvalidMutationException("Attempting to add entry to wrong level; " + e.toString()
                                      + " to level" + level);
            }
            prevEntry = e;
        }
    }

    void replayManifestLog(File manifestDir) throws IOException {
        File curFile = new File(manifestDir, CURRENT_FILE);
        if (!curFile.exists()) { // no current file, no database here
            return;
        }
        FileInputStream is = new FileInputStream(curFile);
        ManifestCurrent.Builder builder = ManifestCurrent.newBuilder();
        builder.mergeDelimitedFrom(is);
        ManifestCurrent header = builder.build();

        final AtomicLong maxCreationOrder = new AtomicLong(0);
        ManifestLog.replayLog(entryComp, new File(header.getCurrentManifest()),
                new ManifestLog.LogScanner() {
                    @Override
                    public void apply(Integer level,
                            SortedSet<Entry> oldSet, SortedSet<Entry> newSet)
                            throws IOException {
                        for (Entry e : oldSet) {
                            if (e.getCreationOrder() > maxCreationOrder.get()) {
                                maxCreationOrder.set(e.getCreationOrder());
                            }
                        }
                        for (Entry e : newSet) {
                            if (e.getCreationOrder() > maxCreationOrder.get()) {
                                maxCreationOrder.set(e.getCreationOrder());
                            }
                        }
                        try {
                            LOG.info("IKDEBUG Applying transform to {}", level);
                            applyTransform(level, oldSet, newSet);
                        } catch (InvalidMutationException ime) {
                            // no problem, we log before we validate
                        }
                    }
                });
        creationOrder.set(maxCreationOrder.get());
        creationOrder.incrementAndGet();
    }

    ManifestLog newManifestLog(File manifestDir) throws IOException {
        File curFile = new File(manifestDir, CURRENT_FILE);
        File newManifestLog;
        while (true) {
            newManifestLog = new File(manifestDir,
                    MANIFEST_NAME + "." + creationOrder.incrementAndGet());
            LOG.info("Creating new manifest logfile: {}", newManifestLog);
            if (newManifestLog.createNewFile()) {
                break;
            }
        }
        SortedSet<Entry> emptySet = new TreeSet<Entry>(entryComp);

        // should check
        List<Closeable> closeOnException = new ArrayList<Closeable>();
        ManifestLog log = new ManifestLog(newManifestLog);
        closeOnException.add(log);
        try {
            for (Integer k : levels.keySet()) {
                log.log(k, emptySet, getLevel(k));
            }

            ManifestCurrent.Builder builder = ManifestCurrent.newBuilder();
            builder.setCurrentManifest(newManifestLog.getCanonicalPath()); // TODO probably shouldn't use canonical
            File tmpCur = File.createTempFile("cur", "tmp", manifestDir);
            FileOutputStream os = new FileOutputStream(tmpCur);
            closeOnException.add(os);
            builder.build().writeDelimitedTo(os);
            os.close();

            if (!tmpCur.renameTo(curFile)) {
                throw new IOException("Couldn't update current file");
            }
        } catch (IOException ioe) {
            for (Closeable c : closeOnException) {
                Closeables.closeQuietly(c);
            }
            throw ioe;
        }
        appliedTransforms = 0;

        return log;
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

    static class ManifestEntryComparator implements Comparator<Entry> {
        final Comparator<ByteString> keyComparator;
        ManifestEntryComparator(Comparator<ByteString> keyComparator) {
            this.keyComparator = keyComparator;
        }

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
    }

    static class InvalidMutationException extends IOException {
        InvalidMutationException(String reason) {
            super(reason);
        }
    }
}
