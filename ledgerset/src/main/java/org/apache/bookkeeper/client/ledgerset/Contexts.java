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
package org.apache.bookkeeper.client.ledgerset;

import org.apache.bookkeeper.statemachine.StateMachine.Fsm;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.client.MetadataStorage;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.ledgerset.DirectLedgerSet.EntryId;
import org.apache.bookkeeper.proto.LedgerSetFormats.LedgerSetFormat;

import java.util.concurrent.ExecutorService;
import java.util.Collections;
import java.util.Enumeration;

class Contexts {
    interface Context {}

    static class BaseContext implements Context {
        final String ledgerSetName;
        final MetadataStorage metadataStorage;
        final BookKeeper bookkeeper;
        final ClientConfiguration clientConfiguration;
        final ExecutorService callbackExecutor;

        static class Builder {
            String ledgerSetName = null;
            MetadataStorage metadataStorage = null;
            BookKeeper bookkeeper = null;
            ClientConfiguration clientConfiguration = null;
            ExecutorService callbackExecutor;

            Builder() {}

            Builder(BaseContext other) {
                this.ledgerSetName = other.ledgerSetName;
                this.metadataStorage = other.metadataStorage;
                this.bookkeeper = other.bookkeeper;
                this.clientConfiguration = other.clientConfiguration;
                this.callbackExecutor = other.callbackExecutor;
            }

            Builder setLedgerSetName(String ledgerSetName) {
                this.ledgerSetName = ledgerSetName;
                return this;
            }

            Builder setMetadataStorage(MetadataStorage metadataStorage) {
                this.metadataStorage = metadataStorage;
                return this;
            }

            Builder setBookKeeper(BookKeeper bookkeeper) {
                this.bookkeeper = bookkeeper;
                return this;
            }

            Builder setClientConfiguration(ClientConfiguration conf) {
                this.clientConfiguration = conf;
                return this;
            }

            Builder setCallbackExecutor(ExecutorService callbackExecutor) {
                this.callbackExecutor = callbackExecutor;
                return this;
            }

            BaseContext build() {
                return new BaseContext(this);
            }
        }

        static Builder newBuilder() {
            return new Builder();
        }

        BaseContext(Builder b) {
            assert(b.ledgerSetName != null);
            assert(b.metadataStorage != null);
            assert(b.bookkeeper != null);
            assert(b.clientConfiguration != null);
            assert(b.callbackExecutor != null);
            this.ledgerSetName = b.ledgerSetName;
            this.metadataStorage = b.metadataStorage;
            this.bookkeeper = b.bookkeeper;
            this.clientConfiguration = b.clientConfiguration;
            this.callbackExecutor = b.callbackExecutor;
        }

        String getLedgerSetName() {
            return ledgerSetName;
        }

        MetadataStorage getMetadataStorage() {
            return metadataStorage;
        }

        BookKeeper getBookkeeperClient() {
            return bookkeeper;
        }

        ClientConfiguration getConf() {
            return clientConfiguration;
        }

        ExecutorService getCallbackExecutor() {
            return callbackExecutor;
        }
    }

    static class LedgerSetContext extends BaseContext {
        final Versioned<LedgerSetFormat> metadata;
        final boolean readonly; // TODO kill this

        static class Builder extends BaseContext.Builder {
            Versioned<LedgerSetFormat> metadata = null;
            boolean readonly = true;

            Builder() {}

            Builder setMetadata(Versioned<LedgerSetFormat> metadata) {
                this.metadata = metadata;
                return this;
            }

            Builder setReadOnly(boolean readonly) {
                this.readonly = readonly;
                return this;
            }

            Builder(BaseContext other) {
                super(other);
            }

            Builder(LedgerSetContext other) {
                super(other);
                this.metadata = other.metadata;
                this.readonly = other.readonly;
            }

            LedgerSetContext build() {
                return new LedgerSetContext(this);
            }
        }

        static Builder newBuilder(BaseContext ctx) {
            return new Builder(ctx);
        }

        static Builder newBuilder(LedgerSetContext ctx) {
            return new Builder(ctx);
        }

        private LedgerSetContext(Builder builder) {
            super(builder);
            assert (builder.metadata != null);

            this.metadata = builder.metadata;
            this.readonly = builder.readonly;
        }
        Versioned<LedgerSetFormat> getMetadata() { return metadata; }
        boolean isReadOnly() { return readonly; }
    }

    static class ReadingBatchContext extends LedgerSetContext {
        final EntryId nextEntryId;

        static class Builder extends LedgerSetContext.Builder {
            EntryId nextEntryId = null;

            Builder() {}

            Builder(ReadingBatchContext other) {
                super(other);
                this.nextEntryId = other.nextEntryId;
            }

            Builder(LedgerSetContext other) {
                super(other);
            }

            Builder setNextEntryId(EntryId lastReadEntry) {
                this.nextEntryId = lastReadEntry;
                return this;
            }

            ReadingBatchContext build() {
                return new ReadingBatchContext(this);
            }
        }

        static Builder newBuilder() {
            return new Builder();
        }

        static Builder newBuilder(LedgerSetContext ctx) {
            return new Builder(ctx);
        }

        static Builder newBuilder(ReadingBatchContext ctx) {
            return new Builder(ctx);
        }

        ReadingBatchContext(Builder builder) {
            super(builder);
            assert(builder.nextEntryId != null);
            this.nextEntryId = builder.nextEntryId;
        }

        EntryId getNextEntryId() { return nextEntryId; }
    }

    static class ReadingLedgerContext extends ReadingBatchContext {
        final LedgerHandle ledger;

        static class Builder extends ReadingBatchContext.Builder {
            LedgerHandle ledger = null;

            Builder(ReadingBatchContext ctx) {
                super(ctx);
            }

            Builder() {
            }

            Builder(ReadingLedgerContext other) {
                super(other);
                this.ledger = other.ledger;
            }

            Builder setLedger(LedgerHandle ledger) {
                this.ledger = ledger;
                return this;
            }

            ReadingLedgerContext build() {
                assert (ledger != null);
                return new ReadingLedgerContext(this);
            }
        }

        static Builder newBuilder(ReadingBatchContext ctx) {
            return new Builder(ctx);
        }

        static Builder newBuilder(ReadingLedgerContext ctx) {
            return new Builder(ctx);
        }

        static Builder newBuilder() {
            return new Builder();
        }

        private ReadingLedgerContext(Builder builder) {
            super(builder);
            assert (builder.ledger != null);
            this.ledger = builder.ledger;
        }

        LedgerHandle getLedger() { return ledger; }
    }

    static class ReadingContext extends LedgerSetContext {
        final Fsm batchReaderFsm;
        final Enumeration<LedgerEntry> entries;

        static class Builder extends LedgerSetContext.Builder {
            Fsm batchReaderFsm = null;
            Enumeration<LedgerEntry> entries = Collections.<LedgerEntry>emptyEnumeration();

            Builder(LedgerSetContext other) {
                super(other);
            }

            Builder(ReadingContext other) {
                super(other);
                this.batchReaderFsm = other.batchReaderFsm;
                this.entries = other.entries;
            }

            Builder setBatchReaderFsm(Fsm fsm) {
                this.batchReaderFsm = fsm;
                return this;
            }

            Builder setEntries(Enumeration<LedgerEntry> entries) {
                this.entries = entries;
                return this;
            }

            ReadingContext build() {
                return new ReadingContext(this);
            }
        }

        static Builder newBuilder(LedgerSetContext ctx) {
            return new Builder(ctx);
        }

        static Builder newBuilder(ReadingContext ctx) {
            return new Builder(ctx);
        }

        ReadingContext(Builder b) {
            super(b);
            assert(b.entries != null);
            assert(b.batchReaderFsm != null);
            this.entries = b.entries;
            this.batchReaderFsm = b.batchReaderFsm;
        }

        Fsm getBatchReaderFsm() {
            return batchReaderFsm;
        }

        Enumeration<LedgerEntry> getEntries() {
            return entries;
        }
    }

}
