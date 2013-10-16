package org.apache.bookkeeper.bookie.lsmindex;

import java.util.Comparator;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.google.protobuf.ByteString;
import com.google.common.primitives.UnsignedBytes;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCompaction {
    private final static Logger LOG
        = LoggerFactory.getLogger(TestCompaction.class);

    Comparator<ByteString> keyComp = KeyComparators.unsignedLexicographical();
    File dbDir = null;

    @Before
    public void setupDir() throws Exception {
        dbDir = File.createTempFile("lsm", "db");
        dbDir.delete();
        dbDir.mkdirs();
        LOG.info("Using db {}", dbDir);
    }

    @After
    public void cleanDir() throws Exception {
        if (dbDir != null) {
            FileUtils.deleteDirectory(dbDir);
        }
    }

    @Test
    public void testFlushingAndLevel0() throws Exception {
        Manifest manifest = new Manifest(keyComp, dbDir);
        Compactor comp = new Compactor(manifest, keyComp, dbDir);

        Assert.assertEquals("Should be no sst tables files now",
                            0, dbDir.list(Util.sstFilter).length);

        // test that flushing a mem table flushes the correct data
        KeyValueIterator everyForth = Util.getIterator(0, 100, 4);
        Future<Void> flush = comp.flushMemtable(everyForth);
        comp.compact();
        flush.get(10, TimeUnit.SECONDS);

        String[] sstables = dbDir.list(Util.sstFilter);
        Assert.assertEquals("Should be 1 sst table file now",
                            1, sstables.length);
        SSTableImpl sst = SSTableImpl.open(new File(dbDir, sstables[0]), keyComp);
        KeyValueIterator sstiter = sst.iterator();
        everyForth = Util.getIterator(0, 100, 4);

        try {
            Util.assertMatch(keyComp, sstiter, everyForth);
        } finally {
            sstiter.close();
        }

        // now generate enough files to trigger 0 -> 1
        for (int i = 1; i < Compactor.LEVEL0_THRESHOLD; i++) {
            everyForth = Util.getIterator(0, 100, 4);
            flush = comp.flushMemtable(everyForth);
            comp.compact();
            flush.get(10, TimeUnit.SECONDS);
        }

        sstables = dbDir.list(Util.sstFilter);
        Assert.assertEquals("Should be 1 sst table file now",
                            1, sstables.length);
        Assert.assertEquals("Level0 should be empty",
                            0, manifest.getLevel(0).size());
        Assert.assertEquals("Level1 should have one file",
                            1, manifest.getLevel(1).size());

        sst = SSTableImpl.open(new File(dbDir, sstables[0]), keyComp);
        sstiter = sst.iterator();
        everyForth = Util.getIterator(0, 100, 4);

        try {
            Util.assertMatch(keyComp, sstiter, everyForth);
        } finally {
            sstiter.close();
        }

        // cause another compaction
        for (int i = 0; i < Compactor.LEVEL0_THRESHOLD; i++) {
            KeyValueIterator allKV = Util.getIterator(0, 100, 1);
            flush = comp.flushMemtable(allKV);
            comp.compact();
            flush.get(10, TimeUnit.SECONDS);
        }
        String[] newSStables = dbDir.list(Util.sstFilter);
        // should have merged over the old sstable
        Assert.assertEquals("Should be 1 sst table file now",
                            1, newSStables.length);
        Assert.assertEquals("Level0 should be empty",
                            0, manifest.getLevel(0).size());
        Assert.assertEquals("Level1 should have one file",
                            1, manifest.getLevel(1).size());
        Assert.assertFalse("SSTable file should have diff name",
                           newSStables[0].equals(sstables[0]));

        sst = SSTableImpl.open(new File(dbDir, newSStables[0]), keyComp);
        sstiter = sst.iterator();
        KeyValueIterator allKV = Util.getIterator(0, 100, 1);

        try {
            Util.assertMatch(keyComp, sstiter, allKV);
        } finally {
            sstiter.close();
        }
    }

    @Test
    public void testCompaction() throws Exception {
        Manifest manifest = new Manifest(keyComp, dbDir);
        Compactor comp = new Compactor(manifest, keyComp, dbDir);

        Assert.assertEquals("Should be no sst tables files now",
                            0, dbDir.list(Util.sstFilter).length);

        int bytesWritten = 0;
        // test that flushing a mem table flushes the correct data
        int threshold = Compactor.LEVEL0_THRESHOLD * Compactor.SSTABLE_MAX_SIZE // level0
            + Compactor.LEVEL_STEP_BASE*Compactor.SSTABLE_MAX_SIZE // level1
            + Compactor.LEVEL_STEP_BASE * 2 *Compactor.SSTABLE_MAX_SIZE; // level2

        int seediter = 0;
        while (bytesWritten < threshold) {
            KeyValueIterator iter = Util.getMaxSizeIterator(keyComp,
                    Compactor.SSTABLE_MAX_SIZE, 0xdeadbeef + seediter++);
            Future<Void> flush = comp.flushMemtable(iter);
            comp.compact();
            flush.get(10, TimeUnit.SECONDS);
            bytesWritten += Compactor.SSTABLE_MAX_SIZE;

            if (seediter % 10 == 0) {
                manifest.dumpManifest();
            }
        }
        String[] sstables = dbDir.list(Util.sstFilter);

        Assert.assertTrue("Level1 should be full, or just 1 off",
                          Compactor.LEVEL_STEP_BASE ==  manifest.getLevel(1).size()
                          || Compactor.LEVEL_STEP_BASE - 1 == manifest.getLevel(1).size());
        Assert.assertTrue("Level2 should have entries",
                          manifest.getLevel(2).size() > 0);

    }
}
