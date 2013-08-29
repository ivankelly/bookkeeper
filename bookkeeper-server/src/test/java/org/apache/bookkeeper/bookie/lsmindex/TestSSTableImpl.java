package org.apache.bookkeeper.bookie.lsmindex;

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


public class TestSSTableImpl {
    private final static Logger LOG
        = LoggerFactory.getLogger(TestSSTableImpl.class);

    Comparator<ByteString> keyComp = new Comparator<ByteString>() {
        Comparator<byte[]> byteComp = UnsignedBytes.lexicographicalComparator();

        @Override
        public int compare(ByteString k1, ByteString k2) {
            return byteComp.compare(k1.toByteArray(), k2.toByteArray());
        }
    };

    Comparator<KeyValue> kvCompByKey = new Comparator<KeyValue>() {
        @Override
        public int compare(KeyValue k1, KeyValue k2) {
            return keyComp.compare(k1.getKey(), k2.getKey());
        }
    };

    private void testIterator(SSTableImpl table, int firstKey, int lastKey,
                              int tableFirstKey, int tableLastKey)
            throws Exception {
        int firstExpected = tableFirstKey > firstKey ? tableFirstKey : firstKey;
        int lastExpected = tableLastKey > lastKey ? lastKey : tableLastKey;

        ByteString firstKeyBS = ByteString.copyFrom(Ints.toByteArray(firstExpected));
        ByteString lastKeyBS = ByteString.copyFrom(Ints.toByteArray(lastExpected));
        int expectedNumKeys = (lastExpected - firstExpected) + 1;
        int numKeys = 0;
        long start = System.nanoTime();
        KeyValueIterator iterSub = table.iterator(firstKeyBS, lastKeyBS);
        iterSub.hasNext();
        LOG.info("Took {}ns to load iterator ({}-{})",
                 new Object[] { System.nanoTime() - start, firstKey, lastKey });

        ByteString lastKeyRead = ByteString.EMPTY;
        try {
            while (iterSub.hasNext()) {
                KeyValue kv = iterSub.next();
                if (numKeys == 0) {
                    Assert.assertEquals("First key is incorrect",
                                        firstKeyBS, kv.getKey());
                }
                numKeys++;
                lastKeyRead = kv.getKey();
            }
        } finally {
            iterSub.close();
        }
        LOG.info("last key {} {}",
                 Ints.fromByteArray(lastKeyBS.toByteArray()),
                 Ints.fromByteArray(lastKeyRead.toByteArray()));
        Assert.assertEquals("Last key is incorrect", lastKeyBS, lastKeyRead);
        Assert.assertEquals("Not enough values read", expectedNumKeys, numKeys);
    }

    @Test
    public void testWritingAndReading() throws Exception {
        List<KeyValue> values = new ArrayList<KeyValue>();
        int tableFirstKey = 100;
        int tableLastKey = 200000;
        for (int i = tableFirstKey; i <= tableLastKey; i++) {
            values.add(KeyValue.newBuilder()
                       .setKey(ByteString.copyFrom(Ints.toByteArray(i)))
                       .setValue(ByteString.copyFrom(Ints.toByteArray(i))).build());
        }
        Collections.sort(values, kvCompByKey);

        File f = new File("/tmp/testlsm.sst");
        SSTableImpl.store(f, keyComp, new KeyValueIteratorWrapper(values.iterator()));

        SSTableImpl sst = SSTableImpl.open(f, keyComp);
        KeyValueIterator iterRead = sst.iterator();
        Iterator<KeyValue> iterOrig = values.iterator();
        LOG.debug("List all");
        while (iterOrig.hasNext()) {
            Assert.assertTrue("Has not read all entries",
                              iterRead.hasNext());
            Assert.assertEquals("Entries do not match",
                                iterRead.next(), iterOrig.next());
        }
        iterRead.close();
        LOG.debug("End list all");

        testIterator(sst, 101, 199999, tableFirstKey, tableLastKey);
        testIterator(sst, 90, 150000, tableFirstKey, tableLastKey);
        testIterator(sst, 150000, 250000, tableFirstKey, tableLastKey);
        testIterator(sst, 90, 250000, tableFirstKey, tableLastKey);

        testIterator(sst, 99, 100, tableFirstKey, tableLastKey);
        testIterator(sst, 100, 101, tableFirstKey, tableLastKey);
        testIterator(sst, 199999, 200000, tableFirstKey, tableLastKey);
        testIterator(sst, 200000, 200001, tableFirstKey, tableLastKey);

        testIterator(sst, 99, 200001, tableFirstKey, tableLastKey);
    }
}
