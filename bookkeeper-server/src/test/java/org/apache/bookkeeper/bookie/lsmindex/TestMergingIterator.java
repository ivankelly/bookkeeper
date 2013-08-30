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

public class TestMergingIterator {
    private final static Logger LOG = LoggerFactory.getLogger(TestMergingIterator.class);

    Comparator<ByteString> keyComp = KeyComparators.unsignedLexicographical();

    @Test(timeout=60000)
    public void testMerge() throws Exception {
        List<List<KeyValue>> values = new ArrayList<List<KeyValue>>();
        values.add(new ArrayList<KeyValue>());
        values.add(new ArrayList<KeyValue>());
        values.add(new ArrayList<KeyValue>());

        int NUM_ENTRIES = 1000;
        Random r = new Random(0xdeadbeef);
        for (int i = 0; i < NUM_ENTRIES; i++) {
            values.get(r.nextInt(values.size()))
                .add(KeyValue.newBuilder()
                     .setKey(ByteString.copyFrom(Ints.toByteArray(i)))
                     .setValue(ByteString.copyFrom(Ints.toByteArray(i))).build());
        }

        List<KeyValueIterator> iterators = new ArrayList<KeyValueIterator>();
        iterators.add(new KeyValueIteratorWrapper(values.get(0).iterator()));
        iterators.add(new KeyValueIteratorWrapper(values.get(1).iterator()));
        iterators.add(new KeyValueIteratorWrapper(values.get(2).iterator()));
        MergingIterator iter = new MergingIterator(keyComp, iterators);
        int i = 0;
        ByteString lastKey = null;
        while (iter.hasNext()) {
            i++;
            KeyValue kv = iter.next();

            if (lastKey != null) {
                Assert.assertTrue("Should be in order", keyComp.compare(lastKey, kv.getKey()) < 0);
            }
            lastKey = kv.getKey();
        }
        Assert.assertEquals("All entries should be accounted for", NUM_ENTRIES, i);
    }

    @Test(timeout=60000)
    public void testDupes() throws Exception {
        List<List<KeyValue>> values = new ArrayList<List<KeyValue>>();
        values.add(new ArrayList<KeyValue>());
        values.add(new ArrayList<KeyValue>());
        values.add(new ArrayList<KeyValue>());

        int NUM_ENTRIES = 1000;
        Random r = new Random(0xdeadbeef);
        int entriesAdded = 0;
        for (int i = 0; i < NUM_ENTRIES; i++) {
            if ((i % 13) == 0) { // put in 2
                int which = r.nextInt(values.size());
                values.get(which)
                    .add(KeyValue.newBuilder()
                         .setKey(ByteString.copyFrom(Ints.toByteArray(i)))
                         .setValue(ByteString.copyFrom(Ints.toByteArray(which))).build());
                which = (which + 1) % values.size();
                values.get(which)
                    .add(KeyValue.newBuilder()
                         .setKey(ByteString.copyFrom(Ints.toByteArray(i)))
                         .setValue(ByteString.copyFrom(Ints.toByteArray(which))).build());
                entriesAdded += 2;
            } else if ((i % 11) == 0) { // put in all 3
                int which = r.nextInt(values.size());
                for (int j = 0; j < values.size(); j++) {
                    which = (which + 1) % values.size();
                    values.get(which)
                        .add(KeyValue.newBuilder()
                             .setKey(ByteString.copyFrom(Ints.toByteArray(i)))
                             .setValue(ByteString.copyFrom(Ints.toByteArray(which))).build());
                    entriesAdded++;
                }                
            } else {
                values.get(r.nextInt(values.size()))
                    .add(KeyValue.newBuilder()
                         .setKey(ByteString.copyFrom(Ints.toByteArray(i)))
                         .setValue(ByteString.copyFrom(Ints.toByteArray(i))).build());
                entriesAdded++;
            }
        }
        List<KeyValueIterator> iterators = new ArrayList<KeyValueIterator>();
        iterators.add(new KeyValueIteratorWrapper(values.get(0).iterator()));
        iterators.add(new KeyValueIteratorWrapper(values.get(1).iterator()));
        iterators.add(new KeyValueIteratorWrapper(values.get(2).iterator()));

        MergingIterator iter = new MergingIterator(keyComp, iterators);
        int i = 0;
        KeyValue lastKV = null;
        while (iter.hasNext()) {
            i++;
            KeyValue kv = iter.next();

            if (lastKV != null) {
                if (keyComp.compare(lastKV.getKey(), kv.getKey()) == 0) {
                    int lastVal = Ints.fromByteArray(lastKV.getValue().toByteArray());
                    int curVal = Ints.fromByteArray(kv.getValue().toByteArray());
                    Assert.assertTrue("Should be in the order of the iterators", lastVal < curVal);
                } else {
                    Assert.assertTrue("Should be in order",
                                      keyComp.compare(lastKV.getKey(), kv.getKey()) < 0);
                }
            }
            lastKV = kv;
        }
        Assert.assertEquals("All entries should be accounted for", entriesAdded, i);
    }
}
