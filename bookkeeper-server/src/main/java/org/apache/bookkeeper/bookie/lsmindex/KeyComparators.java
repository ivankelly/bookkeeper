package org.apache.bookkeeper.bookie.lsmindex;

import java.util.Comparator;
import com.google.protobuf.ByteString;
import com.google.common.primitives.UnsignedBytes;

public class KeyComparators {
    static Comparator<ByteString> unsignedLex = new Comparator<ByteString>() {
        Comparator<byte[]> byteComp = UnsignedBytes.lexicographicalComparator();

        @Override
        public int compare(ByteString k1, ByteString k2) {
            return byteComp.compare(k1.toByteArray(), k2.toByteArray());
        }
    };

    static Comparator<ByteString> unsignedLexicographical() {
        return unsignedLex;
    }
}
