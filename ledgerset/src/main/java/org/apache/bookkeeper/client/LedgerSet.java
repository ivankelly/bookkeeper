/*
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

import org.apache.bookkeeper.client.BookKeeper;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.ledgerset.DirectLedgerSetBuilder;

public class LedgerSet {

    public interface EntryId extends Comparable<EntryId> {}

    public interface Entry {
        public EntryId getId();
        public byte[] getBytes();
    }

    public interface Reader {
        public LedgerFuture<Void> skip(EntryId entryId);
        public LedgerFuture<Boolean> hasMoreEntries();
        public LedgerFuture<Entry> nextEntry();
        public LedgerFuture<Void> close();
    }

    public interface TailingReader extends Reader {
        public LedgerFuture<Void> waitForMoreEntries();
    }

    public interface Writer {
        public LedgerFuture<Void> resume(EntryId fromEntryId);
        public LedgerFuture<EntryId> writeEntry(byte[] data);
        public LedgerFuture<Void> close();
    }

    public interface Trimmer {
        public LedgerFuture<Void> trim(EntryId entryId);
        public LedgerFuture<Void> close();
    }

    public static Builder newBuilder() {
        return new DirectLedgerSetBuilder();
    }

    public interface Builder {
        public Builder setDigestType(BookKeeper.DigestType t);
        public Builder setPassword(String password);
        public Builder setEnsemble(int ensemble);
        public Builder setWriteQuorum(int quorum);
        public Builder setAckQuorum(int quorum);
        public Builder setReadBatchSize(int readBatchSize);
        public Builder setConfiguration(ClientConfiguration conf);
        public Builder setClient(BookKeeper bookkeeper);
        public Builder setMetadataStorage(MetadataStorage metadataStorage);

        public LedgerFuture<TailingReader> buildTailingReader(String setName);
        public LedgerFuture<Reader> buildFencingReader(String setName);
        public LedgerFuture<Writer> buildWriter(String setName);
        public LedgerFuture<Trimmer> buildTrimmer(String setName);
    }
}
