package org.apache.bookkeeper.bookie.lsmindex;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import java.io.File;
import java.io.IOException;
import com.google.common.primitives.Ints;
import org.apache.bookkeeper.proto.DataFormats.KeyValue;
import com.google.protobuf.ByteString;
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
        Manifest m = new Manifest(keyComp);

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
    }

    @Test(timeout=60000)
    public void testConcurrentMod() throws Exception {
    }
}
