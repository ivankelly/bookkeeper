/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.proto;

import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;

public class PaxosValue implements Comparable<PaxosValue> {
    final ByteString value;
    final WriterId writer;
    final boolean committed;

    public PaxosValue(ByteString value, WriterId writer, boolean committed) {
        this.value = value;
        this.writer = writer;
        this.committed = committed;
    }

    public PaxosValue(ByteString value, WriterId writer) {
        this(value, writer, false);
    }

    public ByteString getValue() { return value; }
    public WriterId getWriter() { return writer; }
    public boolean isCommitted() { return committed; }

    @Override
    public int compareTo(PaxosValue other) {
        int wcomp = writer.compareTo(other.writer);
        if (wcomp != 0) {
            return wcomp;
        } else {
            // taken from https://github.com/GoogleCloudPlatform/cloud-bigtable-client/blob/master/bigtable-client-core-parent/bigtable-client-core/src/main/java/com/google/cloud/bigtable/util/ByteStringComparator.java
            int size1 = value.size();
            int size2 = other.value.size();
            int size = Math.min(size1, size2);

            for (int i = 0; i < size; i++) {
                // compare bytes as unsigned
                int byte1 = value.byteAt(i) & 0xff;
                int byte2 = other.value.byteAt(i) & 0xff;

                int comparison = Integer.compare(byte1, byte2);
                if (comparison != 0) {
                    return comparison;
                }
            }
            return Integer.compare(size1, size2);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof PaxosValue) {
            PaxosValue otherValue = (PaxosValue)other;
            return writer.equals(otherValue.writer)
                && value.equals(otherValue.value);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Hashing.goodFastHash(32).newHasher()
            .putLong(writer.hashCode())
            .putLong(value.hashCode()).hash().asInt();
    }

    @Override
    public String toString() {
        return String.format("PaxosValue(writer:%s,value:%s)", writer, value);
    }
}
