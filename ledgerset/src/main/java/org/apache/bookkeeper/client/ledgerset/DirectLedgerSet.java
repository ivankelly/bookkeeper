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

import java.util.concurrent.ExecutorService;
import com.google.common.hash.Hashing;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerFuture;
import org.apache.bookkeeper.client.LedgerSet;
import org.apache.bookkeeper.client.EntryIds;
import org.apache.bookkeeper.client.MetadataStorage;
import org.apache.bookkeeper.client.ledgerset.Events.CloseEvent;
import org.apache.bookkeeper.client.ledgerset.Events.HasMoreEvent;
import org.apache.bookkeeper.client.ledgerset.Events.OpenAndFenceEvent;
import org.apache.bookkeeper.client.ledgerset.Events.OpenReadOnlyEvent;
import org.apache.bookkeeper.client.ledgerset.Events.ReadEntryEvent;
import org.apache.bookkeeper.client.ledgerset.Events.SkipEvent;
import org.apache.bookkeeper.client.ledgerset.Events.TrimEvent;
import org.apache.bookkeeper.client.ledgerset.Events.WaitMoreEvent;
import org.apache.bookkeeper.client.ledgerset.Events.WriteEntryEvent;
import org.apache.bookkeeper.client.ledgerset.Events.ResumeEvent;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.statemachine.StateMachine.Fsm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectLedgerSet {
    static final Logger LOG = LoggerFactory.getLogger(DirectLedgerSet.class);

    public static class Reader implements LedgerSet.Reader {
        protected final Fsm fsm;

        public Reader(String setName, ClientConfiguration conf,
                      BookKeeper bookkeeper,
                      ExecutorService callbackExecutor,
                      MetadataStorage metadataStorage,
                      Fsm fsm) {
            this.fsm = fsm;
            Contexts.BaseContext ctx = Contexts.BaseContext.newBuilder()
                    .setLedgerSetName(setName).setClientConfiguration(conf)
                    .setBookKeeper(bookkeeper).setMetadataStorage(metadataStorage)
                    .setCallbackExecutor(callbackExecutor).build();
            this.fsm.setInitState(new ReaderStates.ReaderInitState(fsm, ctx));
        }

        public LedgerFuture<Void> open() {
            OpenAndFenceEvent openEvent = new OpenAndFenceEvent();
            fsm.sendEvent(openEvent);
            return new ForwardingLedgerFuture<Void>(openEvent);
        }

        @Override
        public LedgerFuture<Void> skip(LedgerSet.EntryId entryId) {
            if (!(entryId instanceof EntryId)) {
                throw new IllegalArgumentException("Invalid entry id type");
            }
            EntryId id = (EntryId) entryId;
            SkipEvent skip = new SkipEvent(id.getLedgerId(), id.getEntryId());
            fsm.sendEvent(skip);
            return new ForwardingLedgerFuture<Void>(skip);
        }

        @Override
        public LedgerFuture<LedgerSet.Entry> nextEntry() {
            ReadEntryEvent ree = new ReadEntryEvent();
            fsm.sendEvent(ree);
            return new ForwardingLedgerFuture<LedgerSet.Entry>(ree);
        }

        @Override
        public LedgerFuture<Boolean> hasMoreEntries() {
            HasMoreEvent hme = new HasMoreEvent();
            fsm.sendEvent(hme);
            return new ForwardingLedgerFuture<Boolean>(hme);
        }

        @Override
        public LedgerFuture<Void> close() {
            CloseEvent ce = new CloseEvent();
            fsm.sendEvent(ce);
            return new ForwardingLedgerFuture<Void>(ce);
        }
    }

    public static class TailingReader extends Reader implements LedgerSet.TailingReader {

        public TailingReader(String setName, ClientConfiguration conf,
                             BookKeeper bookkeeper,
                             ExecutorService callbackExecutor,
                             MetadataStorage metadataStorage,
                             Fsm fsm) {
            super(setName, conf, bookkeeper, callbackExecutor, metadataStorage, fsm);
        }

        public LedgerFuture<Void> open() {
            OpenReadOnlyEvent openEvent = new OpenReadOnlyEvent();
            fsm.sendEvent(openEvent);
            return new ForwardingLedgerFuture<Void>(openEvent);
        }

        @Override
        public LedgerFuture<Void> waitForMoreEntries() {
            WaitMoreEvent wme = new WaitMoreEvent();
            fsm.sendEvent(wme);
            return new ForwardingLedgerFuture<Void>(wme);
        }
    }

    public static class Writer implements LedgerSet.Writer {
        private final Fsm fsm;

        public Writer(String setName, ClientConfiguration conf,
                      BookKeeper bookkeeper,
                      ExecutorService callbackExecutor,
                      MetadataStorage metadataStorage,
                      Fsm fsm) {
            this.fsm = fsm;
            Contexts.BaseContext ctx = Contexts.BaseContext.newBuilder()
                    .setLedgerSetName(setName).setClientConfiguration(conf)
                    .setBookKeeper(bookkeeper).setMetadataStorage(metadataStorage)
                    .setCallbackExecutor(callbackExecutor).build();
            this.fsm.setInitState(new WriterStates.WritingInitState(fsm, ctx));
        }

        @Override
        public LedgerFuture<Void> resume(LedgerSet.EntryId fromEntryId) {
            if (!(fromEntryId instanceof EntryId)) {
                throw new IllegalArgumentException("Invalid entry id type");
            }
            EntryId id = (EntryId) fromEntryId;

            ResumeEvent e = new ResumeEvent(id);
            fsm.sendEvent(e);
            return new ForwardingLedgerFuture<Void>(e);
        }

        @Override
        public LedgerFuture<LedgerSet.EntryId> writeEntry(byte[] data) {
            WriteEntryEvent writeEvent = new WriteEntryEvent(data);
            fsm.sendEvent(writeEvent);
            return new ForwardingLedgerFuture<LedgerSet.EntryId>(writeEvent);
        }

        @Override
        public LedgerFuture<Void> close() {
            CloseEvent ce = new CloseEvent();
            fsm.sendEvent(ce);
            return new ForwardingLedgerFuture<Void>(ce);
        }
    }

    public static class Trimmer implements LedgerSet.Trimmer {
        protected final Fsm fsm;

        public Trimmer(String setName, ClientConfiguration conf,
                      BookKeeper bookkeeper,
                      ExecutorService callbackExecutor,
                      MetadataStorage metadataStorage,
                      Fsm fsm) {
            this.fsm = fsm;
            Contexts.BaseContext ctx = Contexts.BaseContext.newBuilder()
                    .setLedgerSetName(setName).setClientConfiguration(conf)
                    .setBookKeeper(bookkeeper).setMetadataStorage(metadataStorage)
                    .setCallbackExecutor(callbackExecutor).build();
            this.fsm.setInitState(new TrimmingStates.TrimInitState(fsm, ctx));
        }

        @Override
        public LedgerFuture<Void> trim(LedgerSet.EntryId entryId) {
            if (!(entryId instanceof EntryId)) {
                throw new IllegalArgumentException("Invalid entry id type");
            }
            EntryId id = (EntryId) entryId;
            TrimEvent trim = new TrimEvent(id.getLedgerId(), id.getEntryId());
            fsm.sendEvent(trim);
            return new ForwardingLedgerFuture<Void>(trim);
        }

        @Override
        public LedgerFuture<Void> close() {
            CloseEvent ce = new CloseEvent();
            fsm.sendEvent(ce);
            return new ForwardingLedgerFuture<Void>(ce);
        }
    }

    public static class EntryId implements LedgerSet.EntryId {
        final long ledgerId;
        final long entryId;

        public EntryId(long ledgerId, long entryId) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }

        public long getLedgerId() {
            return ledgerId;
        }

        public long getEntryId() {
            return entryId;
        }

        @Override
        public String toString() {
            return "LedgerSet.EntryID(lid:" + ledgerId + ",eid:" + entryId + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof EntryId) {
                EntryId other = (EntryId) o;
                return other.ledgerId == ledgerId && other.entryId == entryId;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Hashing.goodFastHash(32).newHasher()
                    .putLong(ledgerId).putLong(entryId).hash().asInt();
        }

        @Override
        public int compareTo(org.apache.bookkeeper.client.LedgerSet.EntryId other) {
            if (other instanceof EntryId) {
                EntryId otherId = (EntryId) other;
                if (equals(otherId)) {
                    return 0;
                } else if (otherId.ledgerId == ledgerId) {
                    if (otherId.entryId < entryId) { // this is bigger
                        return 1;
                    } else {
                        return -1;
                    }
                } else if (otherId.ledgerId < ledgerId) {  // this is bigger
                    return 1;
                } else {
                    return -1;
                }
            } else if (other == EntryIds.NullEntryId) {
                return 1;
            } else {
                throw new IllegalArgumentException("Tried to compare wrong type of entryid");
            }
        }
    }

    public static class Entry implements LedgerSet.Entry {
        final EntryId id;
        final byte[] bytes;

        Entry(LedgerEntry e) {
            id = new EntryId(e.getLedgerId(), e.getEntryId());
            bytes = e.getEntry();
        }

        @Override
        public EntryId getId() {
            return id;
        }

        @Override
        public byte[] getBytes() {
            return bytes;
        }
    }
}
