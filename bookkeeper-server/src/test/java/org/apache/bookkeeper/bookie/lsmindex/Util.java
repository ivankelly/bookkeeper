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

}
