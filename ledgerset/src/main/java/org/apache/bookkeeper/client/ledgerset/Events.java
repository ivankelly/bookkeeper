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
package org.apache.bookkeeper.client.ledgerset;

import org.apache.bookkeeper.statemachine.StateMachine.Fsm;
import org.apache.bookkeeper.statemachine.StateMachine.Event;
import org.apache.bookkeeper.statemachine.StateMachine.DeferrableEvent;

import com.google.common.util.concurrent.AbstractFuture;

import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerSet.EntryId;
import org.apache.bookkeeper.client.LedgerSet.Entry;
import org.apache.bookkeeper.proto.LedgerSetFormats.LedgerSetFormat;

import java.util.Enumeration;

class Events {

    public static class UserEvent<T> extends AbstractFuture<T> implements DeferrableEvent {
        public void error(Throwable exception) {
            setException(exception);
        }

        public void success(T value) {
            set(value);
        }
    }

    static abstract class OpenEvent extends UserEvent<Void> {}
    static class OpenReadOnlyEvent extends OpenEvent {}
    static class OpenAndFenceEvent extends OpenEvent {}

    /**
     * User Events - all top level state must deal with all of these.
     * If you add an event here, it must also be added to BaseStates#UserState
     */
    static class HasMoreEvent extends UserEvent<Boolean> {}
    static class ReadEntryEvent extends UserEvent<Entry> {}
    static class WaitMoreEvent extends UserEvent<Void> { }
    static class CloseEvent extends UserEvent<Void> { }

    static class ResumeEvent extends UserEvent<Void> {
        final DirectLedgerSet.EntryId resumeFrom;

        ResumeEvent(DirectLedgerSet.EntryId resumeFrom) {
            this.resumeFrom = resumeFrom;
        }

        DirectLedgerSet.EntryId getResumeFromEntryId() {
            return resumeFrom;
        }
    }

    static class WriteEntryEvent extends UserEvent<EntryId> {
        final byte[] data;

        WriteEntryEvent(byte[] data) {
            this.data = data;
        }

        byte[] getData() {
            return data;
        }
    }

    static class SkipEvent extends UserEvent<Void> {
        final long ledgerId;
        final long entryId;

        SkipEvent(long ledgerId, long entryId) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }

        long getLedgerId() {
            return ledgerId;
        }

        long getEntryId() {
            return entryId;
        }
    }

    static class TrimEvent extends UserEvent<Void> {
        final long ledgerId;
        final long entryId;

        TrimEvent(long ledgerId, long entryId) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }

        long getLedgerId() {
            return ledgerId;
        }

        long getEntryId() {
            return entryId;
        }
    }

    ///////////////////////////////////////////////////////////////
    // non user events
    static class OpenNextLedgerEvent implements Event {}
    static class ReopenLedgerEvent implements Event {}
    static class NoMoreEntriesEvent implements Event {}
    static class WaitForNewLedgerEvent implements Event {}
    static class WaitForEntriesEvent implements Event {}
    static class CheckForUpdatesEvent implements Event {}
    static class ForceLedgerCloseEvent implements Event {}

    static class CallbackEvent<T> implements Event {
        final int rc;
        final T value;
        CallbackEvent(int rc, T value) {
            this.rc = rc;
            this.value = value;
        }
        int getReturnCode() { return rc; }
        T getValue() { return value; }
    }

    static class MetadataReadEvent implements Event {
        final Versioned<byte[]> value;
        final Throwable exception;

        MetadataReadEvent(Versioned<byte[]> value) {
            this.value = value;
            this.exception = null;
        }

        MetadataReadEvent(Throwable e) {
            this.value = null;
            this.exception = e;
        }

        Versioned<byte[]> getReadValue() throws Throwable {
            if (exception != null) {
                throw exception;
            }
            return value;
        }
    }

    static class MetadataUpdatedEvent implements Event {
        final Version version;
        final Throwable exception;

        MetadataUpdatedEvent(Version version) {
            this.version = version;
            this.exception = null;
        }

        MetadataUpdatedEvent(Throwable e) {
            this.version = null;
            this.exception = e;
        }

        Version getNewVersion() throws Throwable {
            if (exception != null) {
                throw exception;
            }
            return version;
        }
    }

    static class LedgerCreatedEvent extends CallbackEvent<LedgerHandle> {
        LedgerCreatedEvent(int rc, LedgerHandle value) {
            super(rc, value);
        }
    }

    static class WriteCompleteEvent extends UserEvent<EntryId> {
        int rc;
        EntryId eid;
        WriteEntryEvent wee;

        WriteCompleteEvent(int rc, WriteEntryEvent wee, EntryId eid) {
            this.rc = rc;
            this.wee = wee;
            this.eid = eid;
        }

        EntryId getEntryId() {
            return eid;
        }

        int getReturnCode() {
            return rc;
        }

        void success() {
            this.wee.success(eid);
        }

        @Override
        public void error(Throwable e) {
            this.wee.error(e);
        }
    }

    static class LedgerClosedEvent extends CallbackEvent<LedgerHandle> {
        LedgerClosedEvent(int rc, LedgerHandle value) {
            super(rc, value);
        }
    }

    static class LedgerOpenedEvent extends CallbackEvent<LedgerHandle> {
        LedgerOpenedEvent(int rc, LedgerHandle value) {
            super(rc, value);
        }
    }

    static class EntriesReadEvent extends CallbackEvent<Enumeration<LedgerEntry>> {
        EntriesReadEvent(int rc, Enumeration<LedgerEntry> value) {
            super(rc, value);
        }
    }

    static class LACReadEvent extends CallbackEvent<Long> {
        LACReadEvent(int rc, Long value) {
            super(rc, value);
        }
    }

    ///////////////////////
    // Ledger creation events
    static class CreateLedgerEvent implements DeferrableEvent {
        final Fsm fsm;
        CreateLedgerEvent(Fsm fsm) {
            this.fsm = fsm;
        }

        void newLedgerAvailable(LedgerHandle lh, Versioned<LedgerSetFormat> metadata) {
            fsm.sendEvent(new NewLedgerAvailableEvent(lh, metadata));
        }

        void nonfatalError(Throwable t) {
            fsm.sendEvent(new NewLedgerErrorEvent(t));
        }

        @Override
        public void error(Throwable t) {
            fsm.sendEvent(new ErrorEvent(t));
        }
    }

    static class NewLedgerAvailableEvent implements Event {
        final LedgerHandle ledger;
        final Versioned<LedgerSetFormat> metadata;

        NewLedgerAvailableEvent(LedgerHandle lh, Versioned<LedgerSetFormat> metadata) {
            this.ledger = lh;
            this.metadata = metadata;
        }
    }

    static class NewLedgerErrorEvent implements Event {
        final Throwable cause;

        NewLedgerErrorEvent(Throwable t) {
            this.cause = t;
        }
    }

    // Skipping events
    static class SkipEntryEvent implements DeferrableEvent {
        final Fsm fsm;
        final DirectLedgerSet.EntryId eid;

        SkipEntryEvent(Fsm fsm, DirectLedgerSet.EntryId eid) {
            this.fsm = fsm;
            this.eid = eid;
        }

        long getLedgerId() {
            return eid.getLedgerId();
        }

        long getEntryId() {
            return eid.getEntryId();
        }

        void skipSuccess() {
            fsm.sendEvent(new SkipCompleteEvent());
        }

        @Override
        public void error(Throwable t) {
            fsm.sendEvent(new ErrorEvent(t));
        }
    }

    static class SkipCompleteEvent implements Event {}

    // Trimming events
    static class TrimSuccessEvent implements Event {
        final Versioned<LedgerSetFormat> metadata;

        TrimSuccessEvent(Versioned<LedgerSetFormat> metadata) {
            this.metadata = metadata;
        }
    }

    static class TrimErrorEvent implements Event {
        final Throwable cause;

        TrimErrorEvent(Throwable t) {
            this.cause = t;
        }
    }

    static class ErrorEvent implements Event {
        final Throwable cause;

        ErrorEvent(Throwable t) {
            this.cause = t;
        }
    }

    // Batch reading events
    static class ReadBatchEvent implements DeferrableEvent {
        final Fsm fsm;

        ReadBatchEvent(Fsm fsm) {
            this.fsm = fsm;
        }

        void entriesAvailable(Versioned<LedgerSetFormat> metadata,
                              Enumeration<LedgerEntry> entries) {
            fsm.sendEvent(new BatchAvailableEvent(metadata, entries));
        }

        void noMoreEntries() {
            fsm.sendEvent(new NoMoreEntriesEvent());
        }

        @Override
        public void error(Throwable t) {
            fsm.sendEvent(new ErrorEvent(t));
        }
    }

    static class BatchAvailableEvent implements Event {
        final Versioned<LedgerSetFormat> metadata;
        final Enumeration<LedgerEntry> entries;

        BatchAvailableEvent(Versioned<LedgerSetFormat> metadata,
                            Enumeration<LedgerEntry> entries) {
            this.metadata = metadata;
            this.entries = entries;
        }
    }

    static class WaitMoreEntriesEvent extends ReadBatchEvent {
        WaitMoreEntriesEvent(Fsm fsm) {
            super(fsm);
        }
    }
}
