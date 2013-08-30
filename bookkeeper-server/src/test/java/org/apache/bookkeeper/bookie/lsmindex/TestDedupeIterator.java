package org.apache.bookkeeper.bookie.lsmindex;

import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.io.File;
import com.google.common.primitives.Ints;
import org.apache.bookkeeper.proto.DataFormats.KeyValue;
import com.google.protobuf.ByteString;
import com.google.common.primitives.UnsignedBytes;

import org.junit.Test;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDedupeIterator {
    private final static Logger LOG = LoggerFactory.getLogger(TestDedupeIterator.class);

    Comparator<ByteString> keyComp = KeyComparators.unsignedLexicographical();

    int verifyUnique(KeyValueIterator iter) throws Exception {
        ByteString lastKey = ByteString.EMPTY;
        int i = 0;
        while (iter.hasNext()) {
            ByteString thisKey = iter.next().getKey();
            Assert.assertTrue("keys should differ", keyComp.compare(lastKey, thisKey) < 0);
            lastKey = thisKey;
            i++;
        }
        return i;
    }

    @Test(timeout=60000)
    public void testIterator() throws Exception {
        KeyValueIterator i = Util.getIterator(keyComp, 1, 1, 2, 3, 4);
        Assert.assertEquals("Should only have 4 left",
                            4, verifyUnique(new DedupeIterator(keyComp, i)));

        i = Util.getIterator(keyComp, 1, 1, 2, 4, 4);
        Assert.assertEquals("Should only have 3 left",
                            3, verifyUnique(new DedupeIterator(keyComp, i)));

        i = Util.getIterator(keyComp, 1, 1, 2, 2, 2, 4, 4);
        Assert.assertEquals("Should only have 3 left",
                            3, verifyUnique(new DedupeIterator(keyComp, i)));

        i = Util.getIterator(keyComp, 1, 1, 1, 1, 1, 1, 1, 1, 1);
        Assert.assertEquals("Should only have 1 left",
                            1, verifyUnique(new DedupeIterator(keyComp, i)));
    }
}
