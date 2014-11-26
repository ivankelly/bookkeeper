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
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.ledgerset.DirectLedgerSet.EntryId;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.client.EntryIds;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.MetadataStorage;
import org.apache.bookkeeper.client.ledgerset.BaseStates.DeferringState;
import org.apache.bookkeeper.client.ledgerset.BaseStates.ErrorState;
import org.apache.bookkeeper.client.ledgerset.BaseStates.WriterBaseState;
import org.apache.bookkeeper.client.ledgerset.Events.*;
import org.apache.bookkeeper.client.ledgerset.Contexts.*;
import org.apache.bookkeeper.client.ledgerset.CreateLedgerStates.CreatingNewLedgerState;
import org.apache.bookkeeper.proto.LedgerSetFormats.LedgerSetFormat;

import com.google.common.base.Charsets;

import static org.apache.bookkeeper.client.ledgerset.Utils.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WriterStates {
    static final Logger LOG = LoggerFactory.getLogger(WriterStates.class);

    static class WritingInitState extends DeferringState<BaseContext> {
        WritingInitState(Fsm fsm, BaseContext ctx) {
            super(fsm, ctx);
        }

        public State handleEvent(ResumeEvent e) {
            fsm.deferEvent(e);
            return new ReadingMetadataState(fsm, ctx, e.getResumeFromEntryId());
        }

        public State handleEvent(WriteEntryEvent e) {
            fsm.deferEvent(e);
            return new ReadingMetadataExpectingEmptyState(fsm, ctx);
        }
    }

    static class ReadingMetadataState extends DeferringState<BaseContext> {
        final EntryId resumeFrom;

        ReadingMetadataState(Fsm fsm, BaseContext ctx, EntryId resumeFrom) {
            super(fsm, ctx);
            this.resumeFrom = resumeFrom;

            readMetadata(ctx, fsm);
        }

        public State handleEvent(MetadataReadEvent ose) {
            try {
                Versioned<LedgerSetFormat> metadata = parseMetadata(ose.getReadValue());
                List<Long> ledgers = metadata.getValue().getLedgerList();
                LedgerSetContext newCtx = LedgerSetContext.newBuilder(ctx)
                    .setMetadata(metadata)
                    .build();

                if (resumeFrom == EntryIds.NullEntryId) {
                    return new EnsuringEmptyState(fsm, newCtx);
                }

                if (!ledgers.contains(resumeFrom.getLedgerId())) {
                    return new ErrorState(fsm,
                            new IllegalArgumentException("Resume point does not exist"));
                }

                return new OpeningPreviousLedgersState(fsm, newCtx, resumeFrom);
            } catch (Throwable t) {
                return new ErrorState(fsm, t);
            }
        }
    }

    static class ReadingMetadataExpectingEmptyState extends DeferringState<BaseContext> {
        ReadingMetadataExpectingEmptyState(Fsm fsm, BaseContext ctx) {
            super(fsm, ctx);

            readMetadata(ctx, fsm);
        }

        public State handleEvent(MetadataReadEvent ose) {
            try {
                LedgerSetContext newCtx = LedgerSetContext.newBuilder(ctx)
                    .setMetadata(parseMetadata(ose.getReadValue()))
                    .build();
                return new EnsuringEmptyState(fsm, newCtx);
            } catch (MetadataStorage.NoKeyException e) {
                // doesn't exist
                Versioned<LedgerSetFormat> metadata
                    = new Versioned<LedgerSetFormat>(LedgerSetFormat.newBuilder().build(),
                                                     Version.NEW);
                LedgerSetContext newCtx = LedgerSetContext.newBuilder(ctx)
                    .setMetadata(metadata)
                    .build();
                return new OpeningForWriteState(fsm, newCtx);
            } catch (Throwable t) {
                return new ErrorState(fsm, t);
            }
        }
    }

    static abstract class FencingLedgersState extends DeferringState<LedgerSetContext>
        implements AsyncCallback.OpenCallback, AsyncCallback.CloseCallback {
        FencingLedgersState(Fsm fsm, LedgerSetContext ctx) {
            super(fsm, ctx);
        }

        protected void openLedgers(List<Long> ledgers) {
            BookKeeper bk = ctx.getBookkeeperClient();
            BookKeeper.DigestType type
                = (BookKeeper.DigestType)ctx.getConf().getProperty(Constants.LEDGERSET_DIGEST_TYPE);
            String passwd = ctx.getConf().getString(Constants.LEDGERSET_PASSWD);

            for (Long ledger : ledgers) {
                bk.asyncOpenLedger(ledger,
                        type, passwd.getBytes(Charsets.UTF_8), this, null);
            }
        }

        protected void closeLedger(LedgerHandle lh) {
            lh.asyncClose(this, null);
        }

        @Override
        public void openComplete(int rc, LedgerHandle lh, Object ctx2) {
            fsm.sendEvent(new LedgerOpenedEvent(rc, lh));
        }

        @Override
        public void closeComplete(int rc, LedgerHandle lh, Object ctx2) {
            // noop
        }
    }

    static class OpeningPreviousLedgersState extends FencingLedgersState {
        final List<Long> opened;
        final EntryId resumeFrom;

        OpeningPreviousLedgersState(Fsm fsm, LedgerSetContext ctx, EntryId resumeFrom) {
            super(fsm, ctx);
            this.resumeFrom = resumeFrom;

            LedgerSetFormat metadata = ctx.getMetadata().getValue();
            List<Long> ledgers = metadata.getLedgerList();

            int index = ledgers.indexOf(resumeFrom.getLedgerId());
            assert(index > -1); // should only enter state if exists

            opened = new ArrayList<Long>(ledgers.subList(index, ledgers.size()));
            openLedgers(opened);
        }

        public State handleEvent(LedgerOpenedEvent loe) {
            if (loe.getReturnCode() == BKException.Code.OK) {
                LedgerHandle lh = loe.getValue();

                if (lh.getId() == resumeFrom.getLedgerId()) {
                    if (lh.getLastAddConfirmed() != resumeFrom.getEntryId()) {
                        return new ErrorState(fsm,
                                new IllegalArgumentException("Not resuming from end of ledger set"));
                    }
                } else {
                    // any ledger after the resume point
                    // must be empty, otherwise we're not resuming from end
                    if (lh.getLastAddConfirmed() != LedgerHandle.INVALID_ENTRY_ID) {
                        return new ErrorState(fsm,
                                new IllegalArgumentException("Not resuming from end of ledger set"));
                    }
                }
                opened.remove(lh.getId());
                closeLedger(lh);

                if (opened.size() == 0) {
                    return new OpeningForWriteState(fsm, ctx);
                } else {
                    return this;
                }
            } else {
                Throwable t = BKException.create(loe.getReturnCode());
                LOG.error("Error opening ledger", t);
                return new ErrorState(fsm, t);
            }
        }
    }

    static class EnsuringEmptyState extends FencingLedgersState {
        final List<Long> opened;

        EnsuringEmptyState(Fsm fsm, LedgerSetContext ctx) {
            super(fsm, ctx);
            opened = new ArrayList<Long>(ctx.getMetadata().getValue().getLedgerList());
            openLedgers(opened);
        }

        public State handleEvent(LedgerOpenedEvent loe) {
            if (loe.getReturnCode() == BKException.Code.OK) {
                LedgerHandle lh = loe.getValue();

                // any ledger after the resume point
                // must be empty, otherwise we're not resuming from end
                if (lh.getLastAddConfirmed() != LedgerHandle.INVALID_ENTRY_ID) {
                    return new ErrorState(fsm,
                            new IllegalArgumentException("Not resuming from end of ledger set"));
                }
                opened.remove(lh.getId());
                closeLedger(lh);

                if (opened.size() == 0) {
                    return new OpeningForWriteState(fsm, ctx);
                } else {
                    return this;
                }
            } else {
                Throwable t = BKException.create(loe.getReturnCode());
                LOG.error("Error opening ledger", t);
                return new ErrorState(fsm, t);
            }
        }
    }

    static class OpeningForWriteState extends DeferringState<LedgerSetContext> {
        OpeningForWriteState(Fsm fsm, LedgerSetContext ctx) {
            super(fsm, ctx);
            Fsm createLedgerFsm = fsm.newChildFsm();
            createLedgerFsm.setInitState(new CreatingNewLedgerState(createLedgerFsm, ctx));
            createLedgerFsm.sendEvent(new CreateLedgerEvent(fsm));
        }

        public State handleEvent(ResumeEvent e) {
            e.success(null);
            return this;
        }

        public State handleEvent(NewLedgerAvailableEvent e) {
            LedgerSetContext.Builder builder = LedgerSetContext.newBuilder(ctx)
                .setMetadata(e.metadata);
            return new WritingState(fsm, builder.build(), e.ledger);
        }

        public State handleEvent(NewLedgerErrorEvent e) {
            LOG.error("Failed to create new ledger", e.cause);
            return new ErrorState(fsm, e.cause);
        }
    }

    static class WritingState extends WriterBaseState<LedgerSetContext>
        implements AsyncCallback.AddCallback {
        protected LedgerHandle ledger;
        protected Set<WriteEntryEvent> outstandingWrites;
        private final int maxLedgerSize;

        WritingState(Fsm fsm, LedgerSetContext ctx, LedgerHandle lh) {
            this(fsm, ctx, lh, new HashSet<WriteEntryEvent>());
        }

        WritingState(Fsm fsm, LedgerSetContext ctx, LedgerHandle lh,
                     Set<WriteEntryEvent> outstandingWrites) {
            super(fsm, ctx);
            this.ledger = lh;
            this.outstandingWrites = outstandingWrites;
            maxLedgerSize = ctx.getConf().getInt(Constants.LEDGERSET_MAX_LEDGER_SIZE_CONFKEY,
                                                 Constants.LEDGERSET_MAX_LEDGER_SIZE_DEFAULT);
        }

        @Override
        public State handleEvent(ResumeEvent e) {
            Throwable t = new IllegalStateException("Cannot resume at this point");
            e.error(t);
            return new ErrorState(fsm, t);
        }

        @Override
        public State handleEvent(CloseEvent e) {
            return new ClosingPermanentlyState(fsm, ctx, e, ledger, outstandingWrites);
        }

        @Override
        public State handleEvent(WriteEntryEvent wee) {
            ledger.asyncAddEntry(wee.getData(), this, wee);
            outstandingWrites.add(wee);
            return this;
        }

        boolean shouldRoll() {
            return ledger.getLength() >= maxLedgerSize;
        }

        public State handleEvent(WriteCompleteEvent wce) {
            int rc = wce.getReturnCode();
            if (!outstandingWrites.remove(wce.wee)) {
                LOG.warn("Write {} missing from outstandingWrites", wce.wee);
                return this;
            }

            if (rc == BKException.Code.OK) {
                wce.success();

                if (shouldRoll()) {
                    return new WritingRollingLedgerState(fsm, ctx, ledger, outstandingWrites);
                } else {
                    return this;
                }
            } else {
                Throwable t = BKException.create(wce.getReturnCode());
                wce.error(t);
                return new ErrorState(fsm, t);
            }
        }

        @Override
        public void addComplete(int rc, LedgerHandle lh,
                                long entryId, Object ctx2) {
            assert(ctx2 instanceof WriteEntryEvent);
            WriteEntryEvent wee = (WriteEntryEvent)ctx2;
            fsm.sendEvent(new WriteCompleteEvent(rc, wee,
                                           new EntryId(lh.getId(), entryId)));
        }
    }

    static class WritingRollingLedgerState extends WritingState {
        WritingRollingLedgerState(Fsm fsm, LedgerSetContext ctx, LedgerHandle ledger,
                                  Set<WriteEntryEvent> outstandingWrites) {
            super(fsm, ctx, ledger, outstandingWrites);

            Fsm rollingFsm = fsm.newChildFsm();
            rollingFsm.setInitState(new CreatingNewLedgerState(rollingFsm, ctx));
            rollingFsm.sendEvent(new CreateLedgerEvent(fsm));
        }

        // block close
        @Override
        public State handleEvent(CloseEvent e) {
            fsm.deferEvent(e);
            return this;
        }

        @Override
        public boolean shouldRoll() {
            return false;
        }

        public State handleEvent(NewLedgerAvailableEvent e) {
            LedgerSetContext.Builder builder = LedgerSetContext.newBuilder(ctx)
                .setMetadata(e.metadata);
            return new ClosingCurrentLedgerState(fsm, builder.build(),
                                                 ledger, outstandingWrites,
                                                 e.ledger);
        }

        public State handleEvent(NewLedgerErrorEvent e) {
            // non-fatal, just log it
            LOG.info("Failed to create new ledger", e.cause);
            return new WritingState(fsm, ctx, ledger, outstandingWrites);
        }

        public State handleEvent(ErrorEvent e) {
            return new ClosingErrorState(fsm, ctx, ledger, outstandingWrites, e.cause);
        }
    }

    static abstract class WaitAndCloseLedgerState
        extends DeferringState<LedgerSetContext> implements AsyncCallback.CloseCallback {
        final LedgerHandle oldLedger;
        final Future<?> closeTimeout;
        Set<WriteEntryEvent> outstandingWrites;

        WaitAndCloseLedgerState(Fsm fsm, LedgerSetContext ctx,
                                LedgerHandle oldLedger,
                                Set<WriteEntryEvent> outstandingWrites) {
            super(fsm, ctx);
            this.oldLedger = oldLedger;
            this.outstandingWrites = outstandingWrites;
            // hack around BOOKKEEPER-795
            // sometimes writes can go missing during fencing so make sure that doesn't
            // stop all progress
            closeTimeout = fsm.sendEvent(new ForceLedgerCloseEvent(), 10, TimeUnit.SECONDS);
            closeIfComplete();
        }

        private void closeIfComplete() {
            // once all acknowledged, close the ledger
            assert (outstandingWrites.size() >= 0);
            if (outstandingWrites.size() == 0) {
                oldLedger.asyncClose(this, null);
                closeTimeout.cancel(false);
            }
        }

        @Override
        public void closeComplete(int rc, LedgerHandle lh, Object ctx2) {
            fsm.sendEvent(new LedgerClosedEvent(rc, lh));
        }

        public State handleEvent(ForceLedgerCloseEvent forceClose) {
            for (WriteEntryEvent e : outstandingWrites) {
                e.error(new BKException.BKLedgerClosedException());
            }
            outstandingWrites.clear();
            closeIfComplete();
            return this;
        }

        public State handleEvent(WriteCompleteEvent wce) {
            int rc = wce.getReturnCode();

            if (outstandingWrites.remove(wce.wee)) {
                if (rc == BKException.Code.OK) {
                    wce.success();
                } else {
                    wce.error(BKException.create(rc));
                }
            } else {
                LOG.warn("Write {} missing from outstandingWrites", wce.wee);
            }

            closeIfComplete();
            return this;
        }

        public abstract State handleEvent(LedgerClosedEvent ce);
    }

    static class ClosingErrorState extends WaitAndCloseLedgerState {
        final Throwable error;
        ClosingErrorState(Fsm fsm,
                          LedgerSetContext ctx,
                          LedgerHandle oldLedger,
                          Set<WriteEntryEvent> outstandingWrites,
                          Throwable error) {
            super(fsm, ctx, oldLedger, outstandingWrites);
            this.error = error;
        }

        @Override
        public State handleEvent(LedgerClosedEvent ce) {
            return new ErrorState(fsm, error);
        }
    }

    static class ClosingCurrentLedgerState extends WaitAndCloseLedgerState {

        final LedgerHandle newLedger;

        ClosingCurrentLedgerState(Fsm fsm,
                                  LedgerSetContext ctx,
                                  LedgerHandle oldLedger,
                                  Set<WriteEntryEvent> outstandingWrites,
                                  LedgerHandle newLedger) {
            super(fsm, ctx, oldLedger, outstandingWrites);
            this.newLedger = newLedger;
        }

        @Override
        public State handleEvent(LedgerClosedEvent ce) {
            if (ce.getReturnCode() == BKException.Code.OK) {
                return new WritingState(fsm, ctx, newLedger);
            } else {
                LOG.error("Error closing old ledger, rc {}", ce.getReturnCode());
                return new ErrorState(fsm, BKException.create(ce.getReturnCode()));
            }
        }
    }

    static class ClosingPermanentlyState extends WaitAndCloseLedgerState {
        final CloseEvent closeEvent;

        ClosingPermanentlyState(Fsm fsm,
                                LedgerSetContext ctx,
                                CloseEvent closeEvent,
                                LedgerHandle oldLedger,
                                Set<WriteEntryEvent> outstandingWrites) {
            super(fsm, ctx, oldLedger, outstandingWrites);
            this.closeEvent = closeEvent;
        }

        @Override
        public State handleEvent(LedgerClosedEvent ce) {
            if (ce.getReturnCode() != BKException.Code.OK) {
                LOG.error("Error closing old ledger, rc {}", ce.getReturnCode());
                closeEvent.error(BKException.create(ce.getReturnCode()));
            } else {
                closeEvent.success(null);
            }
            return new ErrorState(fsm, new BKException.BKLedgerClosedException());
        }
    }
}
