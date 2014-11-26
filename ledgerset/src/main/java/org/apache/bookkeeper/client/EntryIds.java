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
package org.apache.bookkeeper.client;

import java.io.OutputStream;
import java.io.InputStream;
import java.io.IOException;

import org.apache.bookkeeper.client.LedgerSet.EntryId;
import org.apache.bookkeeper.client.ledgerset.DirectLedgerSet;

import org.apache.bookkeeper.proto.LedgerSetFormats.EntryIdFormat;

public class EntryIds {
    public static EntryId readFrom(InputStream is) throws IOException {
        EntryIdFormat.Builder builder = EntryIdFormat.newBuilder();
        if (!builder.mergeDelimitedFrom(is)) {
            throw new IOException("Stream at EOF");
        }
        EntryIdFormat pb = builder.build();
        return new DirectLedgerSet.EntryId(pb.getLedger(), pb.getEntry());
    }

    public static void writeTo(EntryId id, OutputStream os) throws IOException {
        if (id instanceof DirectLedgerSet.EntryId) {
            DirectLedgerSet.EntryId did = (DirectLedgerSet.EntryId)id;
            EntryIdFormat pb = EntryIdFormat.newBuilder()
                .setLedger(did.getLedgerId()).setEntry(did.getEntryId()).build();
            pb.writeDelimitedTo(os);
        } else {
            throw new IOException("Unknown entryId type");
        }
    }

    public static final EntryId NullEntryId = new EntryId() {
            @Override
            public int compareTo(EntryId e) {
                return -1;
            }
        };
}
