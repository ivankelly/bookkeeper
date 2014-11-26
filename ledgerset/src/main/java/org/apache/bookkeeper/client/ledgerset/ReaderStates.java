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
import org.apache.bookkeeper.statemachine.StateMachine.State;
import org.apache.bookkeeper.client.ledgerset.DirectLedgerSet.Entry;

import java.util.Enumeration;
import java.util.NoSuchElementException;

import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.MetadataStorage;
import org.apache.bookkeeper.client.ledgerset.WriterStates;
import org.apache.bookkeeper.client.ledgerset.BaseStates.DeferringState;
import org.apache.bookkeeper.client.ledgerset.BaseStates.ErrorState;
import org.apache.bookkeeper.client.ledgerset.BaseStates.ReaderBaseState;
import org.apache.bookkeeper.client.ledgerset.Events.*;
import org.apache.bookkeeper.client.ledgerset.Contexts.*;
import org.apache.bookkeeper.client.ledgerset.BatchReadingStates.ReadBatchInitState;
import static org.apache.bookkeeper.client.ledgerset.Utils.*;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.proto.LedgerSetFormats.LedgerSetFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReaderStates {
    static final Logger LOG = LoggerFactory.getLogger(ReaderStates.class);

    static class ReaderInitState extends DeferringState<BaseContext> {
        ReaderInitState(Fsm fsm, BaseContext ctx) {
            super(fsm, ctx);
        }

        public State handleEvent(OpenEvent e) {
            return new OpeningSetState(fsm, ctx, e, e instanceof OpenAndFenceEvent);
        }
    }

    static class OpeningSetState extends DeferringState<BaseContext> {
        final OpenEvent openEvent;
        final boolean fencing;

        OpeningSetState(Fsm fsm, BaseContext ctx, OpenEvent oe, boolean fencing) {
            super(fsm, ctx);
            this.openEvent = oe;
            this.fencing = fencing;

            readMetadata(ctx, fsm);
        }

        public State handleEvent(MetadataReadEvent ose) {
            Versioned<LedgerSetFormat> metadata;
            try {
                metadata = parseMetadata(ose.getReadValue());

                openEvent.success(null);
            } catch (MetadataStorage.NoKeyException nke) {
                openEvent.success(null);
                metadata = new Versioned<LedgerSetFormat>(LedgerSetFormat.newBuilder().build(),
                                                          Version.NEW);
            } catch (MetadataCorruptException mce) {
                openEvent.error(mce);
                return new ErrorState(fsm, mce);
            } catch (Throwable t) {
                openEvent.error(t);
                return new ErrorState(fsm, t);
            }

            LedgerSetContext newCtx = LedgerSetContext.newBuilder(ctx)
                .setMetadata(metadata)
                .setReadOnly(!fencing)
                .build();
            return new ReaderStates.ReadingState(fsm, newCtx);
        }
    }

    static class ReadingState extends ReaderBaseState<ReadingContext> {
        final Enumeration<LedgerEntry> entries;

        ReadingState(Fsm fsm, ReadingContext ctx) {
            super(fsm, ctx);
            entries = ctx.getEntries();
        }

        ReadingState(Fsm fsm, LedgerSetContext ctx) {
            this(fsm, ReadingContext.newBuilder(ctx)
                  .setBatchReaderFsm(fsm.newChildFsm()).build());

            LedgerSetContext subCtx = LedgerSetContext.newBuilder(ctx).build();
            this.ctx.getBatchReaderFsm().setInitState(
                    new ReadBatchInitState(this.ctx.getBatchReaderFsm(), subCtx));
        }

        @Override
        public final State handleEvent(SkipEvent e) {
            ctx.getBatchReaderFsm().sendEvent(
                    new SkipEntryEvent(fsm,
                            new DirectLedgerSet.EntryId(e.getLedgerId(), e.getEntryId())));
            return new WaitSkipCompleteState(fsm, ctx, e);
        }

        @Override
        public final State handleEvent(CloseEvent e) {
            e.success(null);
            return new ErrorState(fsm, new BKException.BKLedgerClosedException());
        }

        private State readBatch(UserEvent<?> e) {
            fsm.deferEvent(e);
            ctx.getBatchReaderFsm().sendEvent(new ReadBatchEvent(fsm));
            return new WaitForBatchState(fsm, ctx);
        }

        @Override
        public final State handleEvent(HasMoreEvent e) {
            if (entries.hasMoreElements()) {
                e.success(true);
                return this;
            } else {
                return readBatch(e);
            }
        }

        @Override
        public final State handleEvent(ReadEntryEvent e) {
            if (entries.hasMoreElements()) {
                e.success(new Entry(entries.nextElement()));
                return this;
            } else {
                return readBatch(e);
            }
        }

        @Override
        public final State handleEvent(WaitMoreEvent e) {
            if (entries.hasMoreElements()) {
                e.success(null);
                return this;
            } else {
                return readBatch(e);
            }
        }
    }

    static class WaitForBatchState extends DeferringState<ReadingContext> {
        WaitForBatchState(Fsm fsm, ReadingContext ctx) {
            super(fsm, ctx);
        }

        public final State handleEvent(BatchAvailableEvent bae) {
            ReadingContext.Builder newCtx = ReadingContext.newBuilder(ctx).setEntries(bae.entries);
            newCtx.setMetadata(bae.metadata);
            return new ReadingState(fsm, newCtx.build());
        }

        public final State handleEvent(NoMoreEntriesEvent e) {
            if (ctx.isReadOnly()) {
                return new ReadyToPollState(fsm, ctx);
            } else {
                return new NoMoreEntriesState(fsm, ctx);
            }
        }

        public final State handleEvent(ErrorEvent e) {
            return new ErrorState(fsm, e.cause);
        }
    }

    static class NoMoreEntriesState extends ReaderBaseState<ReadingContext> {
        NoMoreEntriesState(Fsm fsm, ReadingContext ctx) {
            super(fsm, ctx);
        }

        @Override
        public final State handleEvent(SkipEvent e) {
            Throwable t = new IllegalArgumentException("Cannot skip, at end");
            e.error(t);
            return new ErrorState(fsm, t);
        }

        @Override
        public final State handleEvent(HasMoreEvent e) {
            e.success(false);
            return this;
        }

        @Override
        public final State handleEvent(ReadEntryEvent e) {
            e.error(new NoSuchElementException("No more entries in set"));
            return this;
        }

        @Override
        public final State handleEvent(CloseEvent e) {
            e.success(null);
            return new ErrorState(fsm, new BKException.BKLedgerClosedException());
        }

        @Override
        public final State handleEvent(WaitMoreEvent e) {
            e.error(new IllegalArgumentException("Can only wait for more entries in tailing mode"));
            return this;
        }
    }

    static class WaitSkipCompleteState extends DeferringState<ReadingContext> {
        SkipEvent skipEvent;

        WaitSkipCompleteState(Fsm fsm, ReadingContext ctx, SkipEvent e) {
            super(fsm, ctx);
            this.skipEvent = e;
        }

        public State handleEvent(SkipCompleteEvent e) {
            skipEvent.success(null);
            return new ReadingState(fsm, ctx);
        }

        public State handleEvent(ErrorEvent e) {
            skipEvent.error(e.cause);
            return new ErrorState(fsm, e.cause);
        }

    }

    static class ReadyToPollState extends ReaderBaseState<ReadingContext> {
        ReadyToPollState(Fsm fsm, ReadingContext ctx) {
            super(fsm, ctx);
        }

        @Override
        public final State handleEvent(SkipEvent e) {
            Throwable t = new IllegalArgumentException("Cannot skip, at end");
            e.error(t);
            return new ErrorState(fsm, t);
        }

        @Override
        public final State handleEvent(HasMoreEvent e) {
            e.success(false);
            return this;
        }

        @Override
        public final State handleEvent(ReadEntryEvent e) {
            e.error(new NoSuchElementException("No more entries in set"));
            return this;
        }

        @Override
        public final State handleEvent(CloseEvent e) {
            e.success(null);
            return new ErrorState(fsm, new BKException.BKLedgerClosedException());
        }

        @Override
        public State handleEvent(WaitMoreEvent e) {
            return new WaitMoreEntriesState(fsm, ctx, e);
        }
    }

    static class WaitMoreEntriesState extends DeferringState<ReadingContext> {
        WaitMoreEvent waitEvent;

        public WaitMoreEntriesState(Fsm fsm, ReadingContext ctx, WaitMoreEvent e) {
            super(fsm, ctx);
            waitEvent = e;
            ctx.getBatchReaderFsm().sendEvent(new WaitMoreEntriesEvent(fsm));
        }

        public State handleEvent(HasMoreEvent e) {
            e.success(false);
            return this;
        }

        public State handleEvent(ReadEntryEvent e) {
            e.error(new NoSuchElementException("Tried to read past end of ledger."));
            return this;
        }

        public final State handleEvent(BatchAvailableEvent bae) {
            waitEvent.success(null);
            ReadingContext.Builder newCtx = ReadingContext.newBuilder(ctx)
                .setEntries(bae.entries);
            newCtx.setMetadata(bae.metadata);
            return new ReadingState(fsm, newCtx.build());
        }

        public final State handleEvent(NoMoreEntriesEvent e) {
            ctx.getBatchReaderFsm().sendEvent(new WaitMoreEntriesEvent(fsm));
            return this;
        }

        public final State handleEvent(ErrorEvent e) {
            waitEvent.error(e.cause);
            return new ErrorState(fsm, e.cause);
        }
    }
}
