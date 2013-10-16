package org.apache.bookkeeper.bookie.lsmindex;

import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import com.google.common.primitives.Ints;
import org.apache.bookkeeper.proto.DataFormats.KeyValue;
import com.google.protobuf.ByteString;
import com.google.common.primitives.UnsignedBytes;

import javax.xml.bind.DatatypeConverter;

import org.junit.Test;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {
    static Comparator<KeyValue> keyValueComparator(final Comparator<ByteString> keyComp) {
        return new Comparator<KeyValue>() {
            @Override
            public int compare(KeyValue k1, KeyValue k2) {
                return keyComp.compare(k1.getKey(), k2.getKey());
            }
        };
    }

    static ByteString intToBs(int i) {
        return ByteString.copyFrom(Ints.toByteArray(i));
    }

    static int bsToInt(ByteString key) {
        return Ints.fromByteArray(key.toByteArray());
    }

    static String bsToString(ByteString bs) {
        return DatatypeConverter.printHexBinary(bs.toByteArray());
    }

    static KeyValue newKeyValue(int key, int value) {
        return KeyValue.newBuilder()
            .setKey(ByteString.copyFrom(Ints.toByteArray(key)))
            .setValue(ByteString.copyFrom(Ints.toByteArray(value))).build();
    }

    static KeyValue newKeyValue(int key) {
        return newKeyValue(key, key);
    }

    static KeyValueIterator getIterator(Comparator<ByteString> keyComp,
                                        Integer... values) {
        List<KeyValue> l = new ArrayList<KeyValue>();
        for (Integer v : values) {
            l.add(newKeyValue(v));
        }
        Collections.sort(l, keyValueComparator(keyComp));
        return new KeyValueIteratorWrapper(l.iterator());
    }

    static KeyValueIterator getIterator(final int min, final int max, final int step) {
        return new KeyValueIterator() {
            int cur = min;

            public boolean hasNext() throws IOException {
                return cur + step < max;
            }

            public KeyValue peek() throws IOException {
                if (hasNext()) {
                    return newKeyValue(cur + step);
                } else {
                    throw new NoSuchElementException();
                }
            }

            public KeyValue next() throws IOException {
                cur = cur + step;
                return newKeyValue(cur);
            }

            public void close() throws IOException {}
        };
    }

    static KeyValueIterator getMaxSizeIterator(Comparator<ByteString> keyComp,
                                               final int bytes, final int seed) {
        List<KeyValue> kvs = new ArrayList<KeyValue>();
        int bytesAdded = 0;

        Random r = new Random(seed);
        while (bytesAdded < bytes) {
            KeyValue nextKV = newKeyValue(Math.abs(r.nextInt()));
            if (nextKV.getSerializedSize() + bytesAdded > bytes) {
                break;
            }
            kvs.add(nextKV);
            bytesAdded += nextKV.getSerializedSize();
        }
        Collections.sort(kvs, keyValueComparator(keyComp));
        return new KeyValueIteratorWrapper(kvs.iterator());
    }

    static void assertMatch(Comparator<ByteString> keyComp,
                            KeyValueIterator iter0, KeyValueIterator... iters)
            throws IOException {
        Comparator<KeyValue> comp = keyValueComparator(keyComp);
        while (iter0.hasNext()) {
            KeyValue kv0 = iter0.next();
            for (KeyValueIterator i : iters) {
                Assert.assertTrue("Must have more", i.hasNext());
                KeyValue kv = i.next();
                Assert.assertEquals("Keys musts match", 0, comp.compare(kv0, kv));
            }
        }
    }

    final static FilenameFilter sstFilter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.endsWith(".sst");
            }
        };
}
