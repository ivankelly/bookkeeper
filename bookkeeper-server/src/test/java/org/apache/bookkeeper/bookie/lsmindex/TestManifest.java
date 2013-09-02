package org.apache.bookkeeper.bookie.lsmindex;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;
import com.google.common.primitives.Ints;
import org.apache.bookkeeper.proto.DataFormats.KeyValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.common.primitives.UnsignedBytes;

import org.junit.Test;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestManifest {
    private final static Logger LOG
        = LoggerFactory.getLogger(TestManifest.class);

    Comparator<ByteString> keyComp = KeyComparators.unsignedLexicographical();
    AtomicLong creationOrder = new AtomicLong(0);
    
    private Manifest.Entry newEntry(int level, int firstKey, int lastKey) {
        return new Manifest.Entry(level, creationOrder.getAndIncrement(),
                                  new File("foobar" + System.currentTimeMillis()),
                                  ByteString.copyFrom(Ints.toByteArray(firstKey)),
                                  ByteString.copyFrom(Ints.toByteArray(lastKey)));
    }

    @Test(timeout=60000)
    public void testAddToLevels() throws Exception {
        File manifestDir = File.createTempFile("manifest", "dir");
        manifestDir.delete();
        manifestDir.mkdirs();
        Manifest m = new Manifest(keyComp, manifestDir);

        m.addToLevel(0, newEntry(0, 10, 20));
        m.addToLevel(1, newEntry(1, 5, 200));

        try {
            m.addToLevel(4, newEntry(4, 0, 12021));
            Assert.fail("Shouldn't be able to skip level");
        } catch (IOException ioe) {
            Assert.assertFalse("Level 4 shouldn't exist", m.hasLevel(4));
        }

        // add overlapping to 0
        m.addToLevel(0, newEntry(0, 15, 30));
        m.addToLevel(0, newEntry(0, 5, 10));
        m.addToLevel(0, newEntry(0, 10, 20));
        m.addToLevel(0, newEntry(0, 20, 30));

        try {
            m.addToLevel(1, newEntry(1, 0, 205));
            Assert.fail("Shouldn't be able to add overlapping level above level0");
        } catch (IOException ioe) {
            // correct
        }

        try {
            m.addToLevel(1, newEntry(1, 3, 5));
            Assert.fail("Shouldn't be able to add overlapping level above level0");
        } catch (IOException ioe) {
            // correct
        }

        try {
            m.addToLevel(1, newEntry(1, 5, 200));
            Assert.fail("Shouldn't be able to add overlapping level above level0");
        } catch (IOException ioe) {
            // correct
        }

        try {
            m.addToLevel(1, newEntry(1, 200, 205));
            Assert.fail("Shouldn't be able to add overlapping level above level0");
        } catch (IOException ioe) {
            // correct
        }

        // test persistence
        m.close();

        m = new Manifest(keyComp, manifestDir);
        Assert.assertTrue("Should have level0", m.hasLevel(0));
        Assert.assertTrue("Should have level1", m.hasLevel(1));
        Assert.assertFalse("Shouldn't have level2", m.hasLevel(2));

        Assert.assertEquals("Should have 5 in level0", 5, m.getLevel(0).size());
        Assert.assertEquals("Should have 1 in level1", 1, m.getLevel(1).size());
        m.close();
    }

    private void writeToLog(ManifestLog l, Manifest.ManifestEntryComparator entryComp,
                            int numEntries) throws Exception {
        for (int i = 0; i < numEntries; i++) {
            SortedSet<Manifest.Entry> oldSet = new TreeSet<Manifest.Entry>(entryComp);
            oldSet.add(newEntry(i, 10, 20));
            SortedSet<Manifest.Entry> newSet = new TreeSet<Manifest.Entry>(entryComp);
            newSet.add(newEntry(i, 11, 27));
            l.log(i, oldSet, newSet);
        }
    }

    @Test(timeout=60000)
    public void testTruncatedLog() throws Exception {
        Manifest.ManifestEntryComparator entryComp
            = new Manifest.ManifestEntryComparator(keyComp);
        File manifestFile = File.createTempFile("manifest", "dir");
        final int NUM_RECORDS = 1000;
        ManifestLog log = new ManifestLog(manifestFile);
        writeToLog(log, entryComp, NUM_RECORDS);
        log.close();

        RandomAccessFile raf = new RandomAccessFile(manifestFile, "rw");
        raf.setLength(raf.length() - 100);
        raf.close();

        final AtomicInteger numRead = new AtomicInteger(0);
        ManifestLog.replayLog(entryComp, manifestFile,
                new ManifestLog.LogScanner() {
                    @Override
                    public void apply(Integer level, SortedSet<Manifest.Entry> oldSet,
                               SortedSet<Manifest.Entry> newSet)
                            throws IOException {
                        numRead.incrementAndGet();
                    }
                });
        LOG.info("Replayed {} records", numRead.get());
        Assert.assertTrue("Should be some records", numRead.get() > 0);
        Assert.assertTrue("Shouldn't be all records", numRead.get() < NUM_RECORDS);
    }

    @Test(timeout=60000)
    public void testTruncatedAtBoundary() throws Exception {
        Manifest.ManifestEntryComparator entryComp
            = new Manifest.ManifestEntryComparator(keyComp);
        File manifestFile = File.createTempFile("manifest", "dir");
        final int NUM_RECORDS = 1000;
        ManifestLog log = new ManifestLog(manifestFile);
        writeToLog(log, entryComp, NUM_RECORDS);
        byte[] bytes = new byte[100];
        int dummySize = 1000;
        CodedOutputStream cos = CodedOutputStream.newInstance(bytes);
        Assert.assertTrue("Not big enough", cos.computeRawVarint32Size(dummySize) > 1);
        cos.writeRawVarint32(dummySize);
        log.os.write(bytes[0]);
        log.close();

        final AtomicInteger numRead = new AtomicInteger(0);
        ManifestLog.replayLog(entryComp, manifestFile,
                new ManifestLog.LogScanner() {
                    @Override
                    public void apply(Integer level, SortedSet<Manifest.Entry> oldSet,
                               SortedSet<Manifest.Entry> newSet)
                            throws IOException {
                        numRead.incrementAndGet();
                    }
                });
        LOG.info("Replayed {} records", numRead.get());
        Assert.assertEquals("Should be all records", NUM_RECORDS, numRead.get());
    }

    @Test(timeout=60000)
    public void testCorruptLog() throws Exception {
        Manifest.ManifestEntryComparator entryComp
            = new Manifest.ManifestEntryComparator(keyComp);
        File manifestFile = File.createTempFile("manifest", "dir");
        final int NUM_RECORDS = 1000;
        ManifestLog log = new ManifestLog(manifestFile);
        writeToLog(log, entryComp, NUM_RECORDS);
        log.close();

        RandomAccessFile raf = new RandomAccessFile(manifestFile, "rw");
        raf.seek(raf.length() - 100);
        for (int i = 0; i < 100; i += 4) {
            raf.writeInt(0xdeadbeef);
        }
        raf.close();

        final AtomicInteger numRead = new AtomicInteger(0);
        try {
            ManifestLog.replayLog(entryComp, manifestFile,
                                  new ManifestLog.LogScanner() {
                                      @Override
                                      public void apply(Integer level,
                                                        SortedSet<Manifest.Entry> oldSet,
                                                        SortedSet<Manifest.Entry> newSet)
                                              throws IOException {
                                          numRead.incrementAndGet();
                                      }
                                  });
            Assert.fail("Shouldn't succeed");
        } catch (IOException ioe) {
            // correct
        }
        LOG.info("Replayed {} records", numRead.get());
        Assert.assertTrue("Should be some records", numRead.get() > 0);
        Assert.assertTrue("Shouldn't be all records", numRead.get() < NUM_RECORDS);
    }
}
