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
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.client.ledgerset.DirectLedgerSet.EntryId;
import org.apache.bookkeeper.proto.LedgerSetFormats.LedgerSetFormat;

import java.util.List;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;

import static org.apache.bookkeeper.client.ledgerset.Utils.*;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.HackAroundProtectionLevel;
import org.apache.bookkeeper.client.MetadataStorage;
import org.apache.bookkeeper.client.ledgerset.BaseStates.BaseState;
import org.apache.bookkeeper.client.ledgerset.BaseStates.DeferringState;
import org.apache.bookkeeper.client.ledgerset.BaseStates.ErrorState;
import org.apache.bookkeeper.client.ledgerset.Events.*;
import org.apache.bookkeeper.client.ledgerset.Contexts.*;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BatchReadingStates {
    static final Logger LOG = LoggerFactory.getLogger(BatchReadingStates.class);

    final static long LEDGER_FIRST_ENTRY = 0L;

    static abstract class BatchReadingBaseState<T extends LedgerSetContext> extends BaseState<T> {
        BatchReadingBaseState(Fsm fsm, T ctx) {
            super(fsm, ctx);
        }

        public abstract State handleEvent(SkipEntryEvent e);
        public abstract State handleEvent(ReadBatchEvent e);
    }

    static class ReadBatchInitState extends DeferringState<LedgerSetContext> {
        ReadBatchInitState(Fsm fsm, LedgerSetContext ctx) {
            super(fsm, ctx);
        }

        public final State handleEvent(SkipEntryEvent e) {
            LedgerSetFormat md = ctx.getMetadata().getValue();
            fsm.deferEvent(e);

            if (md.getLedgerList().contains(e.getLedgerId())) {
                ReadingBatchContext newCtx = ReadingBatchContext.newBuilder(ctx)
                    .setNextEntryId(new DirectLedgerSet.EntryId(e.getLedgerId(), e.getEntryId() + 1))
                    .build();
                return new OpenNextLedgerState(fsm, newCtx);
            } else {
                return new ErrorState(fsm, new IllegalArgumentException(
                                              "Entry does not exist in this set"));
            }
        }

        public final State handleEvent(ReadBatchEvent e) {
            List<Long> ledgers = ctx.getMetadata().getValue().getLedgerList();
            if (ledgers.size() > 0) {
                fsm.deferEvent(e);

                ReadingBatchContext newCtx = ReadingBatchContext.newBuilder(ctx)
                    .setNextEntryId(new DirectLedgerSet.EntryId(ledgers.get(0),
                                                                LEDGER_FIRST_ENTRY))
                    .build();
                return new OpenNextLedgerState(fsm, newCtx);
            } else {
                fsm.deferEvent(e);
                return new NoMoreLedgersState(fsm, ctx);
            }
        }
    }

    static class LedgerClosedState extends BatchReadingBaseState<ReadingBatchContext> {
        LedgerClosedState(Fsm fsm, ReadingBatchContext ctx) {
            super(fsm, ctx);
        }

        @Override
        public final State handleEvent(SkipEntryEvent e) {
            fsm.deferEvent(e);

            List<Long> remaining = getRemainingLedgersAfter(ctx.getMetadata(),
                                                            ctx.getNextEntryId().getLedgerId());
            if (ctx.getNextEntryId().getLedgerId() == e.getLedgerId()
                || remaining.contains(e.getLedgerId())) {
                ReadingBatchContext newCtx = ReadingBatchContext.newBuilder(ctx)
                    .setNextEntryId(new DirectLedgerSet.EntryId(e.getLedgerId(), e.getEntryId() + 1))
                    .build();
                return new OpenNextLedgerState(fsm, newCtx);
            } else {
                return new ErrorState(fsm,new IllegalArgumentException(
                                              "Entry does not exist in this set"));
            }
        }

        @Override
        public final State handleEvent(ReadBatchEvent e) {
            fsm.deferEvent(e);
            return new OpenNextLedgerState(fsm, ctx);
        }
    }

    static class OpenNextLedgerState
        extends DeferringState<ReadingBatchContext> implements AsyncCallback.OpenCallback {
        final long ledgerId;

        OpenNextLedgerState(Fsm fsm, ReadingBatchContext ctx) {
            super(fsm, ctx);

            ledgerId = ctx.getNextEntryId().getLedgerId();
            LOG.debug("Opening ledger {}", ledgerId);
            BookKeeper.DigestType type
                = (BookKeeper.DigestType)ctx.getConf().getProperty(Constants.LEDGERSET_DIGEST_TYPE);
            String passwd = ctx.getConf().getString(Constants.LEDGERSET_PASSWD);

            if (ctx.isReadOnly()) {
                ctx.getBookkeeperClient().asyncOpenLedgerNoRecovery(ledgerId,
                        type, passwd.getBytes(Charsets.UTF_8), this, null);
            } else {
                ctx.getBookkeeperClient().asyncOpenLedger(ledgerId,
                        type, passwd.getBytes(Charsets.UTF_8), this, null);
            }
        }

        @Override
        public void openComplete(int rc, LedgerHandle lh, Object ctx2) {
            fsm.sendEvent(new LedgerOpenedEvent(rc, lh));
        }

        public State handleEvent(LedgerOpenedEvent loe) {
            if (loe.getReturnCode() == BKException.Code.OK) {
                LedgerHandle lh = loe.getValue();
                if (lh.getLastAddConfirmed() == LedgerHandle.INVALID_ENTRY_ID) { // ledger is empty
                    if (!HackAroundProtectionLevel.isClosed(lh)) {
                        ReadingLedgerContext.Builder newCtx = ReadingLedgerContext.newBuilder(ctx)
                            .setLedger(lh);
                        newCtx.setNextEntryId(new EntryId(lh.getId(), LEDGER_FIRST_ENTRY));
                        return new NoMoreEntriesInUnclosedLedgerState(fsm, newCtx.build());
                    }
                    Optional<Long> nextLedger = nextLedgerAfter(ctx.getMetadata(), lh.getId());
                    if (!nextLedger.isPresent()) {
                        return new NoMoreLedgersState(fsm, ctx);
                    }
                    ReadingBatchContext.Builder newCtx = ReadingBatchContext.newBuilder(ctx);
                    newCtx.setNextEntryId(new EntryId(nextLedger.get(), LEDGER_FIRST_ENTRY));
                    return new OpenNextLedgerState(fsm, newCtx.build());
                }
                ReadingLedgerContext.Builder newCtx = ReadingLedgerContext.newBuilder(ctx)
                    .setLedger(lh);
                return new LedgerOpenState(fsm, newCtx.build());
            } else {
                Throwable t = BKException.create(loe.getReturnCode());
                LOG.error("Error opening ledger {}", ledgerId, t);
                return new ErrorState(fsm, t);
            }
        }
    }

    static class LedgerOpenState
        extends BatchReadingBaseState<ReadingLedgerContext> {
        LedgerOpenState(Fsm fsm, ReadingLedgerContext ctx) {
            super(fsm, ctx);
        }

        @Override
        public final State handleEvent(SkipEntryEvent e) {
            long ledgerId = e.getLedgerId();
            long entryId = e.getEntryId();

            if (ledgerId != ctx.getLedger().getId()) {
                fsm.deferEvent(e);
                closeLedger(ctx.getLedger());
                return new LedgerClosedState(fsm, ctx);
            }

            // else it is this ledger
            if (entryId > ctx.getLedger().getLastAddConfirmed()) {
                fsm.deferEvent(e);
                return new ErrorState(fsm, new IllegalArgumentException(
                                              "Entry does not exist in this set"));
            }

            // skip successful
            e.skipSuccess();

            // if entry is last in current ledger, close the ledger,
            // and wait for more requests
            if (entryId == ctx.getLedger().getLastAddConfirmed()) {
                Optional<Long> nextLedger = nextLedgerAfter(ctx.getMetadata(),
                                                            ctx.getLedger().getId());
                if (!nextLedger.isPresent()) {
                    closeLedger(ctx.getLedger());
                    return new NoMoreLedgersState(fsm, ctx);
                } else {
                    ReadingBatchContext.Builder newCtx = ReadingBatchContext.newBuilder(ctx);
                    newCtx.setNextEntryId(new EntryId(nextLedger.get(), LEDGER_FIRST_ENTRY));
                    return new LedgerClosedState(fsm, newCtx.build());
                }
            } else {
                assert(entryId+1 <= ctx.getLedger().getLastAddConfirmed());
                ReadingLedgerContext.Builder newCtx = ReadingLedgerContext.newBuilder(ctx);
                newCtx.setNextEntryId(new DirectLedgerSet.EntryId(ledgerId, entryId + 1));

                return new LedgerOpenState(fsm, newCtx.build());
            }
        }

        @Override
        public final State handleEvent(ReadBatchEvent e) {
            fsm.deferEvent(e);
            return new ReadingBatchState(fsm, ctx);
        }
    }

    static class NoMoreLedgersState extends BatchReadingBaseState<LedgerSetContext> {
        NoMoreLedgersState(Fsm fsm, LedgerSetContext ctx) {
            super(fsm, ctx);
        }

        @Override
        public State handleEvent(SkipEntryEvent e) {
            e.error(new IllegalStateException("Cannot skip, at end"));
            return this;
        }

        @Override
        public final State handleEvent(ReadBatchEvent e) {
            e.noMoreEntries();
            return this;
        }

        public final State handleEvent(WaitMoreEntriesEvent e) {
            fsm.deferEvent(e);
            return new WaitingForNewLedgerState(fsm, ctx);
        }
    }

    static class NoMoreEntriesInUnclosedLedgerState
        extends BatchReadingBaseState<ReadingLedgerContext> {
        NoMoreEntriesInUnclosedLedgerState(Fsm fsm, ReadingLedgerContext ctx) {
            super(fsm, ctx);
        }

        @Override
        public State handleEvent(SkipEntryEvent e) {
            e.error(new IllegalStateException("Cannot skip, at end"));
            return this;
        }

        @Override
        public final State handleEvent(ReadBatchEvent e) {
            e.noMoreEntries();
            return this;
        }

        public final State handleEvent(WaitMoreEntriesEvent e) {
            fsm.deferEvent(e);
            return new WaitingForEntriesState(fsm, ctx);
        }
    }

    static class ReadingBatchState extends DeferringState<ReadingLedgerContext>
        implements AsyncCallback.ReadCallback {
        final long firstEntry;
        final long lastEntry;

        ReadingBatchState(Fsm fsm, ReadingLedgerContext ctx) {
            super(fsm, ctx);

            final int maxEntries = ctx.getConf().getInt(Constants.LEDGERSET_READ_BATCH_SIZE);
            EntryId nextEntry = ctx.getNextEntryId();
            LedgerHandle ledger = ctx.getLedger();
            assert(ctx.getNextEntryId().getLedgerId() == ledger.getId());

            assert(ledger.getLastAddConfirmed() >= nextEntry.getEntryId());
            firstEntry = nextEntry.getEntryId();

            lastEntry = Math.min(ledger.getLastAddConfirmed(),
                                 (long)(firstEntry + maxEntries));

            if (LOG.isDebugEnabled()) {
                LOG.debug("Reading {}-{} from ledger {}",
                        new Object[] { firstEntry, lastEntry, ledger.getId() });
            }
            ledger.asyncReadEntries(firstEntry, lastEntry, this, null);
        }

        @Override
        public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq,
                                 Object ctx2) {
            fsm.sendEvent(new EntriesReadEvent(rc, seq));
        }

        public State handleEvent(EntriesReadEvent ere) {
            if (ere.getReturnCode() == BKException.Code.OK) {
                return new BatchAvailableState(fsm, ctx, ere.getValue(), lastEntry);
            } else {
                Throwable t = BKException.create(ere.getReturnCode());
                LOG.error("Error reading entries {}-{} from ledger {}",
                          new Object[] { firstEntry, lastEntry,
                                         ctx.getLedger().getId(), ere.getReturnCode() }, t);
                return new ErrorState(fsm, t);
            }
        }
    }

    static class BatchAvailableState extends DeferringState<ReadingLedgerContext> {
        final Enumeration<LedgerEntry> entries;
        final long lastEntryRead;

        BatchAvailableState(Fsm fsm, ReadingLedgerContext ctx, Enumeration<LedgerEntry> entries,
                            long lastEntryRead) {
            super(fsm, ctx);
            this.lastEntryRead = lastEntryRead;
            this.entries = entries;
        }

        public State handleEvent(ReadBatchEvent e) {
            e.entriesAvailable(ctx.getMetadata(), entries);

            LedgerHandle ledger = ctx.getLedger();
            if (lastEntryRead == ledger.getLastAddConfirmed()) { // end of ledger
                if (!HackAroundProtectionLevel.isClosed(ledger)) {
                    ReadingLedgerContext.Builder newCtx = ReadingLedgerContext.newBuilder(ctx);
                    newCtx.setNextEntryId(new EntryId(ledger.getId(), lastEntryRead+1)).build();
                    return new NoMoreEntriesInUnclosedLedgerState(fsm, newCtx.build());
                }
                Optional<Long> nextLedger = nextLedgerAfter(ctx.getMetadata(), ledger.getId());
                if (!nextLedger.isPresent()) {
                    return new NoMoreLedgersState(fsm, ctx);
                }

                ReadingBatchContext newCtx = ReadingBatchContext.newBuilder(ctx)
                    .setNextEntryId(new EntryId(nextLedger.get(), LEDGER_FIRST_ENTRY)).build();
                closeLedger(ledger);
                return new LedgerClosedState(fsm, newCtx);
            } else {
                ReadingLedgerContext.Builder newCtx = ReadingLedgerContext.newBuilder(ctx);
                newCtx.setNextEntryId(new EntryId(ledger.getId(), lastEntryRead+1)).build();
                return new LedgerOpenState(fsm, newCtx.build());
            }
        }
    }

    static class WaitingForEntriesState extends DeferringState<ReadingLedgerContext>
        implements AsyncCallback.OpenCallback {
        static final int MAX_DELAY = 120;
        long numTries = 0;

        WaitingForEntriesState(Fsm fsm, ReadingLedgerContext ctx) {
            super(fsm, ctx);
            fsm.sendEvent(new CheckForUpdatesEvent());
        }

        @Override
        public void openComplete(int rc, LedgerHandle lh, Object ctx2) {
            fsm.sendEvent(new LedgerOpenedEvent(rc, lh));
        }

        private void scheduleRetry() {
            int delay = Math.min(MAX_DELAY, (int)Math.pow(2, ++numTries));
            fsm.sendEvent(new CheckForUpdatesEvent(), delay, TimeUnit.SECONDS);
        }

        public State handleEvent(LedgerOpenedEvent loe) {
            if (loe.getReturnCode() == BKException.Code.OK) {
                LedgerHandle newLedger = loe.getValue();
                if (newLedger.getLastAddConfirmed() > ctx.getLedger().getLastAddConfirmed()
                    || HackAroundProtectionLevel.isClosed(newLedger)) {

                    ReadingLedgerContext newCtx = ReadingLedgerContext.newBuilder(ctx)
                        .setLedger(newLedger).build();
                    return new LedgerOpenState(fsm, newCtx);
                    //
                } else {
                    scheduleRetry();
                    return this;
                }
            } else {
                Throwable t = BKException.create(loe.getReturnCode());
                LOG.error("Error opening ledger {}", ctx.getLedger().getId(), t);
                return new ErrorState(fsm, t);
            }
        }

        public State handleEvent(CheckForUpdatesEvent e) {
            BookKeeper.DigestType type
                = (BookKeeper.DigestType)ctx.getConf().getProperty(Constants.LEDGERSET_DIGEST_TYPE);
            String passwd = ctx.getConf().getString(Constants.LEDGERSET_PASSWD);

            ctx.getBookkeeperClient().asyncOpenLedgerNoRecovery(ctx.getLedger().getId(),
                    type, passwd.getBytes(Charsets.UTF_8), this, null);
            return this;
        }
    }

    static class WaitingForNewLedgerState extends DeferringState<LedgerSetContext> {
        long numTries = 0;
        final Optional<Long> lastLedger;

        WaitingForNewLedgerState(Fsm fsm, LedgerSetContext ctx) {
            super(fsm, ctx);

            fsm.sendEvent(new CheckForUpdatesEvent());
            List<Long> currentLedgers = ctx.getMetadata().getValue().getLedgerList();
            if (currentLedgers.size() > 0) {
                lastLedger = Optional.of(currentLedgers.get(currentLedgers.size()-1));
            } else {
                lastLedger = Optional.absent();
            }
        }

        private void scheduleRetry() {
            int delay = Math.min(WaitingForEntriesState.MAX_DELAY, (int)Math.pow(2, ++numTries));
            fsm.sendEvent(new CheckForUpdatesEvent(), delay, TimeUnit.SECONDS);
        }

        public State handleEvent(MetadataReadEvent ose) {
            try {
                Versioned<LedgerSetFormat> newMetadata = parseMetadata(ose.getReadValue());
                List<Long> newLedgers = new ArrayList<Long>(
                        newMetadata.getValue().getLedgerList());

                if (newMetadata.getVersion().equals(ctx.getMetadata().getVersion())
                    || newLedgers.size() == 0) {
                    scheduleRetry();
                    return this;
                }

                Optional<Long> nextLedger = Optional.absent();
                if (lastLedger.isPresent()) {
                    if (!newLedgers.contains(lastLedger.get())) {
                        Throwable t = new IllegalStateException("Gap in reading");
                        return new ErrorState(fsm, t);
                    }

                    nextLedger = nextLedgerAfter(newMetadata, lastLedger.get());
                } else {
                    nextLedger = Optional.of(newMetadata.getValue().getLedgerList().get(0));
                }

                // Can happen if metadata is updated without adding a ledger
                if (!nextLedger.isPresent()) {
                    scheduleRetry();
                    return this;
                }
                ReadingBatchContext.Builder newCtx = ReadingBatchContext.newBuilder(ctx);
                newCtx.setNextEntryId(new EntryId(nextLedger.get(), LEDGER_FIRST_ENTRY));
                newCtx.setMetadata(newMetadata);

                return new OpenNextLedgerState(fsm, newCtx.build());
            } catch (MetadataCorruptException mce) {
                return new ErrorState(fsm, mce);
            } catch (MetadataStorage.NoKeyException e) {
                scheduleRetry();
                return this;
            } catch (Throwable t) {
                return new ErrorState(fsm, t);
            }
        }

        public State handleEvent(CheckForUpdatesEvent e) {
            readMetadata(ctx, fsm);
            return this;
        }
    }

    static void closeLedger(LedgerHandle lh) {
        lh.asyncClose(new AsyncCallback.CloseCallback() {
                @Override
                public void closeComplete(int rc, LedgerHandle lh, Object ctx2) {
                    // noop
                }
            }, null);
    }

    static List<Long> getRemainingLedgersAfter(Versioned<LedgerSetFormat> metadata, long ledger) {
        List<Long> ledgers = new ArrayList<Long>(metadata.getValue().getLedgerList());
        while (ledger != ledgers.remove(0)) {}
        return ledgers;
    }

    static Optional<Long> nextLedgerAfter(Versioned<LedgerSetFormat> metadata, long ledger) {
        List<Long> ledgers = getRemainingLedgersAfter(metadata, ledger);
        if (ledgers.size() > 0) {
            return Optional.of(ledgers.get(0));
        } else {
            return Optional.absent();
        }
    }
}
