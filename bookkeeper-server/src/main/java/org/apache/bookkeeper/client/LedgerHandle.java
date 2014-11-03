/*
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

import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Queue;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.util.OrderedSafeExecutor.OrderedSafeGenericCallback;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.bookkeeper.versioning.Version;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.RateLimiter;

/**
 * Ledger handle contains ledger metadata and is used to access the read and
 * write operations to a ledger.
 */
public class LedgerHandle {
    final static Logger LOG = LoggerFactory.getLogger(LedgerHandle.class);

    final byte[] ledgerKey;
    AtomicReference<LedgerMetadata> metadataRef;
    final BookKeeper bk;
    final long ledgerId;
    long lastAddPushed;
    long lastAddConfirmed;
    long length;
    final DigestManager macManager;
    final DistributionSchedule distributionSchedule;

    final RateLimiter throttler;

    /**
     * Invalid entry id. This value is returned from methods which
     * should return an entry id but there is no valid entry available.
     */
    final static public long INVALID_ENTRY_ID = BookieProtocol.INVALID_ENTRY_ID;

    final AtomicInteger blockAddCompletions = new AtomicInteger(0);
    final Queue<PendingAddOp> pendingAddOps = new ConcurrentLinkedQueue<PendingAddOp>();

    final Counter ensembleChangeCounter;
    final Counter lacUpdateHitsCounter;
    final Counter lacUpdateMissesCounter;

    LedgerHandle(BookKeeper bk, long ledgerId, LedgerMetadata metadata,
                 DigestType digestType, byte[] password)
            throws GeneralSecurityException, NumberFormatException {
        this.bk = bk;
        this.metadataRef = new AtomicReference<LedgerMetadata>(metadata);

        if (metadata.isClosed()) {
            lastAddConfirmed = lastAddPushed = metadata.getLastEntryId();
            length = metadata.getLength();
        } else {
            lastAddConfirmed = lastAddPushed = INVALID_ENTRY_ID;
            length = 0;
        }

        this.ledgerId = ledgerId;

        this.throttler = RateLimiter.create(bk.getConf().getThrottleValue());

        macManager = DigestManager.instantiate(ledgerId, password, digestType);
        this.ledgerKey = MacDigestManager.genDigest("ledger", password);
        distributionSchedule = new RoundRobinDistributionSchedule(
                metadata.getWriteQuorumSize(),
                metadata.getAckQuorumSize(),
                metadata.getEnsembleSize());

        ensembleChangeCounter = bk.getStatsLogger().getCounter(BookKeeperClientStats.ENSEMBLE_CHANGES);
        lacUpdateHitsCounter = bk.getStatsLogger().getCounter(BookKeeperClientStats.LAC_UPDATE_HITS);
        lacUpdateMissesCounter = bk.getStatsLogger().getCounter(BookKeeperClientStats.LAC_UPDATE_MISSES);
        bk.getStatsLogger().registerGauge(BookKeeperClientStats.PENDING_ADDS,
                                          new Gauge<Integer>() {
                                              public Integer getDefaultValue() {
                                                  return 0;
                                              }
                                              public Integer getSample() {
                                                  return pendingAddOps.size();
                                              }
                                          });
    }

    /**
     * Get the id of the current ledger
     *
     * @return the id of the ledger
     */
    public long getId() {
        return ledgerId;
    }

    /**
     * Get the last confirmed entry id on this ledger. It reads
     * the local state of the ledger handle, which is different
     * from the readLastConfirmed call. In the case the ledger
     * is not closed and the client is a reader, it is necessary
     * to call readLastConfirmed to obtain an estimate of the
     * last add operation that has been confirmed.
     *
     * @see #readLastConfirmed()
     *
     * @return the last confirmed entry id or {@link #INVALID_ENTRY_ID INVALID_ENTRY_ID} if no entry has been confirmed
     */
    public long getLastAddConfirmed() {
        return lastAddConfirmed;
    }

    /**
     * Get the entry id of the last entry that has been enqueued for addition (but
     * may not have possibly been persited to the ledger)
     *
     * @return the id of the last entry pushed or {@link #INVALID_ENTRY_ID INVALID_ENTRY_ID} if no entry has been pushed
     */
    synchronized public long getLastAddPushed() {
        return lastAddPushed;
    }

    /**
     * Get the Ledger's key/password.
     *
     * @return byte array for the ledger's key/password.
     */
    public byte[] getLedgerKey() {
        return Arrays.copyOf(ledgerKey, ledgerKey.length);
    }

    /**
     * Get the LedgerMetadata
     *
     * @return LedgerMetadata for the LedgerHandle
     */
    LedgerMetadata getLedgerMetadata() {
        return metadataRef.get();
    }

    /**
     * Get the DigestManager
     *
     * @return DigestManager for the LedgerHandle
     */
    DigestManager getDigestManager() {
        return macManager;
    }

    /**
     *  Add to the length of the ledger in bytes.
     *
     * @param delta
     * @return the length of the ledger after the addition
     */
    long addToLength(long delta) {
        this.length += delta;
        return this.length;
    }

    /**
     * Returns the length of the ledger in bytes.
     *
     * @return the length of the ledger in bytes
     */
    synchronized public long getLength() {
        return this.length;
    }

    /**
     * Get the Distribution Schedule
     *
     * @return DistributionSchedule for the LedgerHandle
     */
    DistributionSchedule getDistributionSchedule() {
        return distributionSchedule;
    }

    void writeLedgerConfig(LedgerMetadata ledgerMetadata, GenericCallback<Version> writeCb) {
        LOG.info("Writing metadata to ledger manager: {}, {}, {}, {}",
                new Object[] { this.ledgerId, ledgerMetadata.getVersion(), ledgerMetadata, writeCb.getClass() });

        bk.getLedgerManager().writeLedgerMetadata(ledgerId, ledgerMetadata, writeCb);
    }

    /**
     * Close this ledger synchronously.
     * @see #asyncClose
     */
    public void close()
            throws InterruptedException, BKException {
        SyncCounter counter = new SyncCounter();
        counter.inc();

        asyncClose(new SyncCloseCallback(), counter);

        counter.block(0);
        if (counter.getrc() != BKException.Code.OK) {
            throw BKException.create(counter.getrc());
        }
    }

    /**
     * Asynchronous close, any adds in flight will return errors.
     *
     * Closing a ledger will ensure that all clients agree on what the last entry
     * of the ledger is. This ensures that, once the ledger has been closed, all
     * reads from the ledger will return the same set of entries.
     *
     * @param cb
     *          callback implementation
     * @param ctx
     *          control object
     */
    public void asyncClose(CloseCallback cb, Object ctx) {
        asyncCloseInternal(cb, ctx, BKException.Code.LedgerClosedException);
    }

    /**
     * Has the ledger been closed?
     */
    public boolean isClosed() {
        return metadataRef.get().isClosed();
    }

    void asyncCloseInternal(final CloseCallback cb, final Object ctx, final int rc) {
        try {
            doAsyncCloseInternal(cb, ctx, rc);
        } catch (RejectedExecutionException re) {
            LOG.debug("Failed to close ledger {} : ", ledgerId, re);
            errorOutPendingAdds(bk.getReturnRc(rc));
            cb.closeComplete(bk.getReturnRc(BKException.Code.InterruptedException), this, ctx);
        }
    }

    /**
     * Same as public version of asyncClose except that this one takes an
     * additional parameter which is the return code to hand to all the pending
     * add ops
     *
     * @param cb
     * @param ctx
     * @param rc
     */
    void doAsyncCloseInternal(final CloseCallback cb, final Object ctx, final int rc) {
        bk.mainWorkerPool.submitOrdered(ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {
                List<PendingAddOp> pendingAdds;

                final LedgerMetadata currentMetadata = metadataRef.get();
                LedgerMetadata.Builder metadataBuilder = LedgerMetadata.copyFrom(currentMetadata);
                boolean alreadyClosed = false;
                synchronized(LedgerHandle.this) {
                    // drain pending adds first
                    pendingAdds = drainPendingAddsToErrorOut();

                    // if the metadata is already closed, we don't need to proceed the process
                    // otherwise, it might end up encountering bad version error log messages when updating metadata
                    if (currentMetadata.isClosed()) {
                        alreadyClosed = true;
                    } else {
                        // synchronized on LedgerHandle.this to ensure that
                        // lastAddPushed can not be updated after the metadata
                        // is closed.
                        metadataBuilder.setLength(length);
                        metadataBuilder.closeLedger(lastAddConfirmed);
                        lastAddPushed = lastAddConfirmed;
                    }
                }

                // error out all pending adds during closing, the callbacks shouldn't be
                // running under any bk locks.
                errorOutPendingAdds(rc, pendingAdds);

                if (alreadyClosed) {
                    cb.closeComplete(BKException.Code.OK, LedgerHandle.this, ctx);
                    return;
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Closing ledger: " + ledgerId + " at entryId: "
                              + currentMetadata.getLastEntryId()
                              + " with this many bytes: " + currentMetadata.getLength());
                }

                final LedgerMetadata newMetadata = metadataBuilder.build();
                final class CloseCb extends OrderedSafeGenericCallback<Version> {
                    CloseCb() {
                        super(bk.mainWorkerPool, ledgerId);
                    }

                    @Override
                    public void safeOperationComplete(final int rc1, Version version) {
                        if (rc1 == BKException.Code.MetadataVersionException) {
                            rereadMetadata(new OrderedSafeGenericCallback<LedgerMetadata>(bk.mainWorkerPool,
                                                                                          ledgerId) {
                                @Override
                                public void safeOperationComplete(int newrc, LedgerMetadata readMetadata) {
                                    if (newrc != BKException.Code.OK) {
                                        LOG.error("Error reading new metadata from ledger " + ledgerId
                                                  + " when closing, code=" + newrc);
                                        cb.closeComplete(rc, LedgerHandle.this, ctx);
                                    } else {
                                        if (!currentMetadata.isNewerThan(readMetadata)
                                            && ((readMetadata.isClosed()
                                                 && readMetadata.getLastEntryId() == lastAddConfirmed)
                                                || !currentMetadata.isConflictWith(readMetadata))) {
                                            metadataRef.compareAndSet(currentMetadata, readMetadata);
                                            // retry with the new metadata
                                            asyncCloseInternal(cb, ctx, rc);
                                            return;
                                        } else {
                                            LOG.warn("Conditional update ledger metadata for ledger " + ledgerId + " failed.");
                                            cb.closeComplete(rc1, LedgerHandle.this, ctx);
                                        }
                                    }
                                }
                            });
                        } else if (rc1 != BKException.Code.OK) {
                            LOG.error("Error update ledger metadata for ledger " + ledgerId + " : " + rc1);
                            cb.closeComplete(rc1, LedgerHandle.this, ctx);
                        } else {
                            LedgerMetadata newMetadata2 = LedgerMetadata.copyFrom(newMetadata)
                                .setVersion(version).build();
                            metadataRef.compareAndSet(currentMetadata, newMetadata2);
                            cb.closeComplete(BKException.Code.OK, LedgerHandle.this, ctx);
                        }
                    }
                };

                writeLedgerConfig(newMetadata, new CloseCb());
            }
        });
    }

    /**
     * Read a sequence of entries synchronously.
     *
     * @param firstEntry
     *          id of first entry of sequence (included)
     * @param lastEntry
     *          id of last entry of sequence (included)
     *
     */
    public Enumeration<LedgerEntry> readEntries(long firstEntry, long lastEntry)
            throws InterruptedException, BKException {
        SyncCounter counter = new SyncCounter();
        counter.inc();

        asyncReadEntries(firstEntry, lastEntry, new SyncReadCallback(), counter);

        counter.block(0);
        if (counter.getrc() != BKException.Code.OK) {
            throw BKException.create(counter.getrc());
        }

        return counter.getSequence();
    }

    /**
     * Read a sequence of entries asynchronously.
     *
     * @param firstEntry
     *          id of first entry of sequence
     * @param lastEntry
     *          id of last entry of sequence
     * @param cb
     *          object implementing read callback interface
     * @param ctx
     *          control object
     */
    public void asyncReadEntries(long firstEntry, long lastEntry,
                                 ReadCallback cb, Object ctx) {
        // Little sanity check
        if (firstEntry < 0 || lastEntry > lastAddConfirmed
                || firstEntry > lastEntry) {
            cb.readComplete(BKException.Code.ReadException, this, null, ctx);
            return;
        }

        try {
            new PendingReadOp(this, bk.scheduler,
                              firstEntry, lastEntry, cb, ctx).initiate();
        } catch (InterruptedException e) {
            cb.readComplete(BKException.Code.InterruptedException, this, null, ctx);
        }
    }

    /**
     * Add entry synchronously to an open ledger.
     *
     * @param data
     *         array of bytes to be written to the ledger
     * @return the entryId of the new inserted entry
     */
    public long addEntry(byte[] data) throws InterruptedException, BKException {
        return addEntry(data, 0, data.length);
    }

    /**
     * Add entry synchronously to an open ledger.
     *
     * @param data
     *         array of bytes to be written to the ledger
     * @param offset
     *          offset from which to take bytes from data
     * @param length
     *          number of bytes to take from data
     * @return the entryId of the new inserted entry
     */
    public long addEntry(byte[] data, int offset, int length)
            throws InterruptedException, BKException {
        LOG.debug("Adding entry {}", data);

        SyncCounter counter = new SyncCounter();
        counter.inc();

        SyncAddCallback callback = new SyncAddCallback();
        asyncAddEntry(data, offset, length, callback, counter);
        counter.block(0);

        if (counter.getrc() != BKException.Code.OK) {
            throw BKException.create(counter.getrc());
        }

        return callback.entryId;
    }

    /**
     * Add entry asynchronously to an open ledger.
     *
     * @param data
     *          array of bytes to be written
     * @param cb
     *          object implementing callbackinterface
     * @param ctx
     *          some control object
     */
    public void asyncAddEntry(final byte[] data, final AddCallback cb,
                              final Object ctx) {
        asyncAddEntry(data, 0, data.length, cb, ctx);
    }

    /**
     * Add entry asynchronously to an open ledger, using an offset and range.
     *
     * @param data
     *          array of bytes to be written
     * @param offset
     *          offset from which to take bytes from data
     * @param length
     *          number of bytes to take from data
     * @param cb
     *          object implementing callbackinterface
     * @param ctx
     *          some control object
     * @throws ArrayIndexOutOfBoundsException if offset or length is negative or
     *          offset and length sum to a value higher than the length of data.
     */
    public void asyncAddEntry(final byte[] data, final int offset, final int length,
                              final AddCallback cb, final Object ctx) {
        PendingAddOp op = new PendingAddOp(LedgerHandle.this, cb, ctx);
        doAsyncAddEntry(op, data, offset, length, cb, ctx);
    }

    /**
     * Make a recovery add entry request. Recovery adds can add to a ledger even if
     * it has been fenced.
     *
     * This is only valid for bookie and ledger recovery, which may need to replicate
     * entries to a quorum of bookies to ensure data safety.
     *
     * Normal client should never call this method.
     */
    void asyncRecoveryAddEntry(final byte[] data, final int offset, final int length,
                               final AddCallback cb, final Object ctx) {
        PendingAddOp op = new PendingAddOp(LedgerHandle.this, cb, ctx).enableRecoveryAdd();
        doAsyncAddEntry(op, data, offset, length, cb, ctx);
    }

    private void doAsyncAddEntry(final PendingAddOp op, final byte[] data, final int offset, final int length,
                                 final AddCallback cb, final Object ctx) {
        if (offset < 0 || length < 0
                || (offset + length) > data.length) {
            throw new ArrayIndexOutOfBoundsException(
                "Invalid values for offset("+offset
                +") or length("+length+")");
        }
        throttler.acquire();

        final long entryId;
        final long currentLength;
        boolean wasClosed = false;
        synchronized(this) {
            // synchronized on this to ensure that
            // the ledger isn't closed between checking and
            // updating lastAddPushed
            if (metadataRef.get().isClosed()) {
                wasClosed = true;
                entryId = -1;
                currentLength = 0;
            } else {
                entryId = ++lastAddPushed;
                currentLength = addToLength(length);
                op.setEntryId(entryId);
                pendingAddOps.add(op);
            }
        }

        if (wasClosed) {
            // make sure the callback is triggered in main worker pool
            try {
                bk.mainWorkerPool.submit(new SafeRunnable() {
                    @Override
                    public void safeRun() {
                        LOG.warn("Attempt to add to closed ledger: {}", ledgerId);
                        cb.addComplete(BKException.Code.LedgerClosedException,
                                LedgerHandle.this, INVALID_ENTRY_ID, ctx);
                    }
                    @Override
                    public String toString() {
                        return String.format("AsyncAddEntryToClosedLedger(lid=%d)", ledgerId);
                    }
                });
            } catch (RejectedExecutionException e) {
                cb.addComplete(bk.getReturnRc(BKException.Code.InterruptedException),
                        LedgerHandle.this, INVALID_ENTRY_ID, ctx);
            }
            return;
        }

        try {
            bk.mainWorkerPool.submit(new SafeRunnable() {
                @Override
                public void safeRun() {
                    ChannelBuffer toSend = macManager.computeDigestAndPackageForSending(
                                               entryId, lastAddConfirmed, currentLength, data, offset, length);
                    op.initiate(toSend, length);
                }
            });
        } catch (RejectedExecutionException e) {
            cb.addComplete(bk.getReturnRc(BKException.Code.InterruptedException),
                    LedgerHandle.this, INVALID_ENTRY_ID, ctx);
        }
    }

    synchronized void updateLastConfirmed(long lac, long len) {
        if (lac > lastAddConfirmed) {
            lastAddConfirmed = lac;
            lacUpdateHitsCounter.inc();
        } else {
            lacUpdateMissesCounter.inc();
        }
        lastAddPushed = Math.max(lastAddPushed, lac);
        length = Math.max(length, len);
    }

    /**
     * Obtains asynchronously the last confirmed write from a quorum of bookies. This
     * call obtains the the last add confirmed each bookie has received for this ledger
     * and returns the maximum. If the ledger has been closed, the value returned by this
     * call may not correspond to the id of the last entry of the ledger, since it reads
     * the hint of bookies. Consequently, in the case the ledger has been closed, it may
     * return a different value than getLastAddConfirmed, which returns the local value
     * of the ledger handle.
     *
     * @see #getLastAddConfirmed()
     *
     * @param cb
     * @param ctx
     */

    public void asyncReadLastConfirmed(final ReadLastConfirmedCallback cb, final Object ctx) {
        boolean isClosed;
        long lastEntryId;
        synchronized (this) {
            LedgerMetadata metadata = metadataRef.get();
            isClosed = metadata.isClosed();
            lastEntryId = metadata.getLastEntryId();
        }
        if (isClosed) {
            cb.readLastConfirmedComplete(BKException.Code.OK, lastEntryId, ctx);
            return;
        }
        ReadLastConfirmedOp.LastConfirmedDataCallback innercb = new ReadLastConfirmedOp.LastConfirmedDataCallback() {
                @Override
                public void readLastConfirmedDataComplete(int rc, DigestManager.RecoveryData data) {
                    if (rc == BKException.Code.OK) {
                        updateLastConfirmed(data.lastAddConfirmed, data.length);
                        cb.readLastConfirmedComplete(rc, data.lastAddConfirmed, ctx);
                    } else {
                        cb.readLastConfirmedComplete(rc, INVALID_ENTRY_ID, ctx);
                    }
                }
            };
        new ReadLastConfirmedOp(this, innercb).initiate();
    }

    /**
     * Obtains asynchronously the last confirmed write from a quorum of bookies.
     * It is similar as
     * {@link #asyncTryReadLastConfirmed(org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback, Object)},
     * but it doesn't wait all the responses from the quorum. It would callback
     * immediately if it received a LAC which is larger than current LAC.
     *
     * @see #asyncTryReadLastConfirmed(org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback, Object)
     *
     * @param cb
     *          callback to return read last confirmed
     * @param ctx
     *          callback context
     */
    public void asyncTryReadLastConfirmed(final ReadLastConfirmedCallback cb, final Object ctx) {
        boolean isClosed;
        long lastEntryId;
        synchronized (this) {
            LedgerMetadata metadata = metadataRef.get();
            isClosed = metadata.isClosed();
            lastEntryId = metadata.getLastEntryId();
        }
        if (isClosed) {
            cb.readLastConfirmedComplete(BKException.Code.OK, lastEntryId, ctx);
            return;
        }
        ReadLastConfirmedOp.LastConfirmedDataCallback innercb = new ReadLastConfirmedOp.LastConfirmedDataCallback() {
            AtomicBoolean completed = new AtomicBoolean(false);
            @Override
            public void readLastConfirmedDataComplete(int rc, DigestManager.RecoveryData data) {
                if (rc == BKException.Code.OK) {
                    updateLastConfirmed(data.lastAddConfirmed, data.length);
                    if (completed.compareAndSet(false, true)) {
                        cb.readLastConfirmedComplete(rc, data.lastAddConfirmed, ctx);
                    }
                } else {
                    if (completed.compareAndSet(false, true)) {
                        cb.readLastConfirmedComplete(rc, INVALID_ENTRY_ID, ctx);
                    }
                }
            }
        };
        new TryReadLastConfirmedOp(this, innercb, getLastAddConfirmed()).initiate();
    }

    /**
     * Context objects for synchronous call to read last confirmed.
     */
    static class LastConfirmedCtx {
        final static long ENTRY_ID_PENDING = -10;
        long response;
        int rc;

        LastConfirmedCtx() {
            this.response = ENTRY_ID_PENDING;
        }

        void setLastConfirmed(long lastConfirmed) {
            this.response = lastConfirmed;
        }

        long getlastConfirmed() {
            return this.response;
        }

        void setRC(int rc) {
            this.rc = rc;
        }

        int getRC() {
            return this.rc;
        }

        boolean ready() {
            return (this.response != ENTRY_ID_PENDING);
        }
    }

    /**
     * Obtains synchronously the last confirmed write from a quorum of bookies. This call
     * obtains the the last add confirmed each bookie has received for this ledger
     * and returns the maximum. If the ledger has been closed, the value returned by this
     * call may not correspond to the id of the last entry of the ledger, since it reads
     * the hint of bookies. Consequently, in the case the ledger has been closed, it may
     * return a different value than getLastAddConfirmed, which returns the local value
     * of the ledger handle.
     *
     * @see #getLastAddConfirmed()
     *
     * @return The entry id of the last confirmed write or {@link #INVALID_ENTRY_ID INVALID_ENTRY_ID}
     *         if no entry has been confirmed
     * @throws InterruptedException
     * @throws BKException
     */
    public long readLastConfirmed()
            throws InterruptedException, BKException {
        LastConfirmedCtx ctx = new LastConfirmedCtx();
        asyncReadLastConfirmed(new SyncReadLastConfirmedCallback(), ctx);
        synchronized(ctx) {
            while(!ctx.ready()) {
                ctx.wait();
            }
        }

        if(ctx.getRC() != BKException.Code.OK) throw BKException.create(ctx.getRC());
        return ctx.getlastConfirmed();
    }

    /**
     * Obtains synchronously the last confirmed write from a quorum of bookies.
     * It is similar as {@link #readLastConfirmed()}, but it doesn't wait all the responses
     * from the quorum. It would callback immediately if it received a LAC which is larger
     * than current LAC.
     *
     * @see #readLastConfirmed()
     *
     * @return The entry id of the last confirmed write or {@link #INVALID_ENTRY_ID INVALID_ENTRY_ID}
     *         if no entry has been confirmed
     * @throws InterruptedException
     * @throws BKException
     */
    public long tryReadLastConfirmed() throws InterruptedException, BKException {
        LastConfirmedCtx ctx = new LastConfirmedCtx();
        asyncTryReadLastConfirmed(new SyncReadLastConfirmedCallback(), ctx);
        synchronized (ctx) {
            while (!ctx.ready()) {
                ctx.wait();
            }
        }
        if (ctx.getRC() != BKException.Code.OK) throw BKException.create(ctx.getRC());
        return ctx.getlastConfirmed();
    }

    // close the ledger and send fails to all the adds in the pipeline
    void handleUnrecoverableErrorDuringAdd(int rc) {
        LedgerMetadata metadata = metadataRef.get();
        if (metadata.isInRecovery()) {
            // we should not close ledger if ledger is recovery mode
            // otherwise we may lose entry.
            errorOutPendingAdds(rc);
            return;
        }
        LOG.error("Closing ledger {} due to error {}", ledgerId, rc);
        asyncCloseInternal(NoopCloseCallback.instance, null, rc);
    }

    void errorOutPendingAdds(int rc) {
        errorOutPendingAdds(rc, drainPendingAddsToErrorOut());
    }

    synchronized List<PendingAddOp> drainPendingAddsToErrorOut() {
        PendingAddOp pendingAddOp;
        List<PendingAddOp> opsDrained = new ArrayList<PendingAddOp>(pendingAddOps.size());
        while ((pendingAddOp = pendingAddOps.poll()) != null) {
            addToLength(-pendingAddOp.entryLength);
            opsDrained.add(pendingAddOp);
        }
        return opsDrained;
    }

    void errorOutPendingAdds(int rc, List<PendingAddOp> ops) {
        for (PendingAddOp op : ops) {
            op.submitCallback(rc);
        }
    }

    void sendAddSuccessCallbacks() {
        // Start from the head of the queue and proceed while there are
        // entries that have had all their responses come back
        PendingAddOp pendingAddOp;
        while ((pendingAddOp = pendingAddOps.peek()) != null
               && blockAddCompletions.get() == 0) {
            if (!pendingAddOp.completed) {
                return;
            }
            pendingAddOps.remove();
            lastAddConfirmed = pendingAddOp.entryId;
            pendingAddOp.submitCallback(BKException.Code.OK);
        }

    }

    ImmutableList<BookieSocketAddress> replaceBookieInEnsemble(
            final ImmutableList<BookieSocketAddress> ensemble, final int bookieIndex)
            throws BKException.BKNotEnoughBookiesException {
        BookieSocketAddress newBookie;
        LOG.info("Handling failure of bookie: {} index: {}", ensemble.get(bookieIndex), bookieIndex);
        newBookie = bk.bookieWatcher.replaceBookie(ensemble, bookieIndex);

        final ArrayList<BookieSocketAddress> newEnsemble = new ArrayList<>(ensemble);
        newEnsemble.set(bookieIndex, newBookie);
        return ImmutableList.copyOf(newEnsemble);
    }

    static class BookieFailureEvent {
        final BookieSocketAddress addr;
        final int bookieIndex;

        BookieFailureEvent(final BookieSocketAddress addr, final int bookieIndex) {
            this.addr = addr;
            this.bookieIndex = bookieIndex;
        }

        BookieSocketAddress getAddress() {
            return addr;
        }

        int getBookieIndex() {
            return bookieIndex;
        }

        @Override
        public int hashCode() {
            return Hashing.goodFastHash(32).newHasher()
                .putString(addr.toString()).putInt(bookieIndex)
                .hash().asInt();
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof BookieFailureEvent) {
                BookieFailureEvent other = (BookieFailureEvent)o;
                return addr == other.addr
                    && bookieIndex == other.bookieIndex;
            }
            return false;
        }
    }

    List<BookieFailureEvent> failures = new ArrayList<BookieFailureEvent>();
    synchronized void handleBookieFailure(final BookieSocketAddress addr, final int bookieIndex) {
        BookieFailureEvent event = new BookieFailureEvent(addr, bookieIndex);
        if (failures.contains(event)) {
            return;
        }
        failures.add(event);
        blockAddCompletions.incrementAndGet();
        replaceFailedBookie(event);
    }

    synchronized void bookieFailuredHandled(BookieFailureEvent event) {
        assert(failures.remove(event));
        blockAddCompletions.decrementAndGet();

        // the failed bookie has been replaced
        unsetSuccessAndSendWriteRequest(event.getBookieIndex());
    }

    void replaceFailedBookie(BookieFailureEvent failure) {
        final BookieSocketAddress addr = failure.getAddress();
        final int bookieIndex = failure.getBookieIndex();

        LedgerMetadata metadata = metadataRef.get();

        final long newEnsembleStartEntry = lastAddConfirmed + 1;
        final ImmutableList<BookieSocketAddress> currentEnsemble = metadata.getEnsemble(newEnsembleStartEntry);
        if (!currentEnsemble.get(bookieIndex).equals(addr)) {
            // ensemble has already changed, failure of this addr is immaterial
            LOG.warn("Write did not succeed to {}, bookieIndex {}, but we have already fixed it.",
                     addr, bookieIndex);
            bookieFailuredHandled(failure);
            return;
        }

        try {
            ImmutableList<BookieSocketAddress> newEnsemble = replaceBookieInEnsemble(
                    currentEnsemble, bookieIndex);
            LedgerMetadata.Builder builder = LedgerMetadata.copyFrom(metadata);
            ImmutableSortedMap.Builder<Long, ImmutableList<BookieSocketAddress>> ensembles
                = ImmutableSortedMap.naturalOrder();
            for (Map.Entry<Long, ImmutableList<BookieSocketAddress>> e : metadata.getEnsembles().entrySet()) {
                if (e.getKey() < newEnsembleStartEntry) {
                    ensembles.put(e.getKey(), e.getValue());
                }
            }
            ensembles.put(newEnsembleStartEntry, newEnsemble);
            builder.setEnsembles(ensembles.build());

            LedgerMetadata newMetadata = builder.build();
            writeLedgerConfig(newMetadata,
                    new ChangeEnsembleCb(metadata, newMetadata, failure));
        } catch (BKException.BKNotEnoughBookiesException e) {
            LOG.error("Could not get additional bookie to "
                      + "remake ensemble, closing ledger: " + ledgerId);
            handleUnrecoverableErrorDuringAdd(e.getCode());
            return;
        }
    }

    /**
     * Callback which is updating the ledgerMetadata in zk with the newly
     * reformed ensemble. On MetadataVersionException, will reread latest
     * ledgerMetadata and act upon.
     */
    private final class ChangeEnsembleCb extends OrderedSafeGenericCallback<Version> {
        private final LedgerMetadata currentMetadata;
        private final LedgerMetadata newMetadata;
        private final BookieFailureEvent failure;

        ChangeEnsembleCb(LedgerMetadata currentMetadata,
                         LedgerMetadata newMetadata,
                         BookieFailureEvent failure) {
            super(bk.mainWorkerPool, ledgerId);
            this.currentMetadata = currentMetadata;
            this.newMetadata = newMetadata;
            this.failure = failure;
        }

        @Override
        public void safeOperationComplete(final int rc, Version version) {
            if (rc == BKException.Code.MetadataVersionException) {
                // We changed the ensemble, but got a version exception. We
                // should still consider this as an ensemble change
                ensembleChangeCounter.inc();
                rereadMetadata(new RereadLedgerMetadataCb(failure));
                return;
            } else if (rc != BKException.Code.OK) {
                LOG.error("Could not persist ledger metadata currentMetadata {}"
                          + " , newMetadata {}, closing ledger",
                          new Object[] { currentMetadata, newMetadata, BKException.create(rc) });
                handleUnrecoverableErrorDuringAdd(rc);
                return;
            }

            LedgerMetadata tmpMetadata = LedgerMetadata.copyFrom(newMetadata)
                .setVersion(version).build();
            if (!metadataRef.compareAndSet(currentMetadata, tmpMetadata)) {
                // retry
                replaceFailedBookie(failure);
                return;
            } else {
                bookieFailuredHandled(failure);
                // We've successfully changed an ensemble
                ensembleChangeCounter.inc();
            }
        }
    };

    /**
     * Callback which is reading the ledgerMetadata present in zk. This will try
     * to resolve the version conflicts.
     */
    private final class RereadLedgerMetadataCb extends OrderedSafeGenericCallback<LedgerMetadata> {
        private final BookieFailureEvent failure;

        RereadLedgerMetadataCb(BookieFailureEvent failure) {
            super(bk.mainWorkerPool, ledgerId);
            this.failure = failure;
        }

        @Override
        public void safeOperationComplete(int newrc, LedgerMetadata newMetadata) {
            if (newrc != BKException.Code.OK) {
                LOG.error("Error reading new metadata from ledger "
                        + "after changing ensemble, code=" + newrc);
                handleUnrecoverableErrorDuringAdd(newrc);
            } else {
                LedgerMetadata currentMetadata = metadataRef.get();
                if (newMetadata.getState() == currentMetadata.getState()
                    && !metadataRef.compareAndSet(currentMetadata, newMetadata)) {
                    LOG.error("Could not resolve ledger metadata conflict handling bookie failure:"
                              + " Failed bookie ({}:{}) currentMetadata {} newMetadata {}",
                              new Object[] { failure.getBookieIndex(), failure.getAddress(),
                                             currentMetadata, newMetadata });
                    handleUnrecoverableErrorDuringAdd(newrc);
                } else {
                    replaceFailedBookie(failure);
                }
            }
        }
    };

    void unsetSuccessAndSendWriteRequest(final int bookieIndex) {
        for (PendingAddOp pendingAddOp : pendingAddOps) {
            pendingAddOp.unsetSuccessAndSendWriteRequest(bookieIndex);
        }
    }

    void rereadMetadata(final GenericCallback<LedgerMetadata> cb) {
        bk.getLedgerManager().readLedgerMetadata(ledgerId, cb);
    }

    void recover(final GenericCallback<Void> cb) {
        final LedgerMetadata metadata = metadataRef.get();
        LedgerMetadata.Builder builder = LedgerMetadata.copyFrom(metadata);
        if (metadata.isClosed()) {
            lastAddConfirmed = metadata.getLastEntryId();
            synchronized (this) {
                lastAddPushed = lastAddConfirmed;
                length = metadata.getLength();
            }

            // We are already closed, nothing to do
            cb.operationComplete(BKException.Code.OK, null);
            return;
        } else if (metadata.isInRecovery()) {
            // if metadata is already in recover, dont try to write again,
            // just do the recovery from the starting point
            new LedgerRecoveryOp(LedgerHandle.this, cb).initiate();
            return;
        } else {
            builder.markLedgerInRecovery();
        }

        final GenericCallback<LedgerMetadata> rereadCb
            = new OrderedSafeGenericCallback<LedgerMetadata>(bk.mainWorkerPool, ledgerId) {
            @Override
            public void safeOperationComplete(int rc, LedgerMetadata readMetadata) {
                if (rc != BKException.Code.OK) {
                    cb.operationComplete(rc, null);
                } else {
                    // we don't care if compare and set is successful, we'll retry recovery anyhow
                    metadataRef.set(readMetadata);
                    recover(cb);
                }
            }
        };

        final LedgerMetadata newMetadata = builder.build();
        GenericCallback<Version> updateMetaCb = new OrderedSafeGenericCallback<Version>(
                bk.mainWorkerPool, ledgerId) {
            @Override
            public void safeOperationComplete(final int rc, Version version) {
                LedgerMetadata tmpMetadata = LedgerMetadata.copyFrom(newMetadata).setVersion(version).build();
                if (rc == BKException.Code.MetadataVersionException) {
                    rereadMetadata(rereadCb);
                } else if (rc == BKException.Code.OK
                           && metadataRef.compareAndSet(metadata, tmpMetadata)) {
                    new LedgerRecoveryOp(LedgerHandle.this, cb).initiate();
                } else {
                    LOG.error("Error writing ledger config " + rc + " of ledger " + ledgerId);
                    cb.operationComplete(rc, null);
                }
            }
        };
        writeLedgerConfig(newMetadata, updateMetaCb);
    }

    static class NoopCloseCallback implements CloseCallback {
        static NoopCloseCallback instance = new NoopCloseCallback();

        @Override
        public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
            if (rc != BKException.Code.OK) {
                LOG.warn("Close failed: " + BKException.getMessage(rc));
            }
            // noop
        }
    }

    private static class SyncReadCallback implements ReadCallback {
        /**
         * Implementation of callback interface for synchronous read method.
         *
         * @param rc
         *          return code
         * @param leder
         *          ledger identifier
         * @param seq
         *          sequence of entries
         * @param ctx
         *          control object
         */
        @Override
        public void readComplete(int rc, LedgerHandle lh,
                                 Enumeration<LedgerEntry> seq, Object ctx) {

            SyncCounter counter = (SyncCounter) ctx;
            synchronized (counter) {
                counter.setSequence(seq);
                counter.setrc(rc);
                counter.dec();
                counter.notify();
            }
        }
    }

    private static class SyncAddCallback implements AddCallback {
        long entryId = -1;

        /**
         * Implementation of callback interface for synchronous read method.
         *
         * @param rc
         *          return code
         * @param leder
         *          ledger identifier
         * @param entry
         *          entry identifier
         * @param ctx
         *          control object
         */
        @Override
        public void addComplete(int rc, LedgerHandle lh, long entry, Object ctx) {
            SyncCounter counter = (SyncCounter) ctx;

            this.entryId = entry;
            counter.setrc(rc);
            counter.dec();
        }
    }

    private static class SyncReadLastConfirmedCallback implements ReadLastConfirmedCallback {
        /**
         * Implementation of  callback interface for synchronous read last confirmed method.
         */
        @Override
        public void readLastConfirmedComplete(int rc, long lastConfirmed, Object ctx) {
            LastConfirmedCtx lcCtx = (LastConfirmedCtx) ctx;

            synchronized(lcCtx) {
                lcCtx.setRC(rc);
                lcCtx.setLastConfirmed(lastConfirmed);
                lcCtx.notify();
            }
        }
    }

    private static class SyncCloseCallback implements CloseCallback {
        /**
         * Close callback method
         *
         * @param rc
         * @param lh
         * @param ctx
         */
        @Override
        public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
            SyncCounter counter = (SyncCounter) ctx;
            counter.setrc(rc);
            synchronized (counter) {
                counter.dec();
                counter.notify();
            }
        }
    }
}
