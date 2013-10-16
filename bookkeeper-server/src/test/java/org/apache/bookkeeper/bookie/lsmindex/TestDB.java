package org.apache.bookkeeper.bookie.lsmindex;

import java.util.Map;
import java.util.HashMap;
import java.util.Random;
import java.util.Comparator;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.google.protobuf.ByteString;
import com.google.common.primitives.UnsignedBytes;

import org.apache.bookkeeper.proto.DataFormats.KeyValue;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDB {
    private final static Logger LOG
        = LoggerFactory.getLogger(TestDB.class);

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
    public void testPutGetDelete() throws Exception {
        DB db = new DB(keyComp, dbDir);
        db.start();
        Map<ByteString, ByteString> toCheck = new HashMap<ByteString, ByteString>();
        Random r = new Random(0xcafebabe);
        for (int i = 0; i < 1000000; i++) {
            ByteString key = Util.intToBs(r.nextInt());
            while (toCheck.containsKey(key)) {
                key = Util.intToBs(r.nextInt());
            }
            ByteString value = Util.intToBs(r.nextInt());
            if (i % 10000 == 0) {
                toCheck.put(key, value);
            }
            db.put(key, value);
        }

        for (Map.Entry<ByteString, ByteString> e : toCheck.entrySet()) {
            ByteString value = db.get(e.getKey());
            Assert.assertNotNull("Value should exist", value);
            Assert.assertEquals("Values should match", 0,
                                keyComp.compare(value, e.getValue()));
            db.delete(e.getKey());
        }

        for (Map.Entry<ByteString, ByteString> e : toCheck.entrySet()) {
            Assert.assertNull("Value shouldn't exist", db.get(e.getKey()));
        }
    }

    private boolean findKeyInFiles(ByteString keyToFind) throws Exception {
        String[] files = dbDir.list(Util.sstFilter);

        for (String s : files) {
            SSTableImpl sst = SSTableImpl.open(new File(dbDir, s), keyComp);
            KeyValueIterator sstiter = sst.iterator();
            try {
                while (sstiter.hasNext()) {
                    KeyValue kv = sstiter.next();
                    if (keyComp.compare(kv.getKey(), keyToFind) == 0) {
                        LOG.info("Found key in {} with value {}",
                                 s, Manifest.dumpKey(kv.getValue()));
                        return true;
                    }
                }
            } finally {
                sstiter.close();
            }
        }
        return false;
    }
    /**
     * Ensure that when a key is deleted, it will disappear from
     * the database eventually.
     */
    @Test
    public void testKeyDisappearsWhenDeleted() throws Exception {
        DB db = new DB(keyComp, dbDir);
        db.start();
        ByteString toDelete = ByteString.EMPTY;
        ByteString toDeleteValue = ByteString.EMPTY;
        Random r = new Random(0xcafebabe);

        for (int i = 0; i < 1000000; i++) {
            ByteString key = Util.intToBs(r.nextInt());
            ByteString value = Util.intToBs(r.nextInt());
            while (keyComp.compare(toDelete, key) == 0) {
                key = Util.intToBs(r.nextInt());
            }
            if (i == 1000) {
                toDelete = key;
                toDeleteValue = value;
            }

            db.put(key, value);
        }
        Assert.assertTrue("Should find key in files", findKeyInFiles(toDelete));
        db.manifest.dumpManifest();
        LOG.info("Deleting {} - {}", Manifest.dumpKey(toDelete),
                 Manifest.dumpKey(toDeleteValue));
        db.delete(toDelete);
        Assert.assertTrue("Should find key in files", findKeyInFiles(toDelete));

        for (int i = 0; i < 1000000; i++) {
            ByteString key = Util.intToBs(r.nextInt());
            while (keyComp.compare(toDelete, key) == 0) {
                key = Util.intToBs(r.nextInt());
            }

            ByteString value = Util.intToBs(r.nextInt());
            db.put(key, value);
        }
        db.manifest.dumpManifest();
        Assert.assertFalse("Shouldn't find key in files", findKeyInFiles(toDelete));
    }
}
