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
package org.apache.bookkeeper.bookie.sls;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.GarbageCollectorThread;
import org.apache.bookkeeper.bookie.LedgerDirsManager;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookieProtocol;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SortedLedgerStorage extends InterleavedLedgerStorage
        implements LedgerStorage, CacheCallback, SkipListFlusher {
    private final static Logger LOG = LoggerFactory.getLogger(SortedLedgerStorage.class);

    private final EntryMemTable memTable;
    private final ScheduledExecutorService scheduler;

    public SortedLedgerStorage(ServerConfiguration conf, LedgerManager ledgerManager,
                               LedgerDirsManager ledgerDirsManager, CheckpointSource checkpointSource,
                               GarbageCollectorThread.SafeEntryAdder safeEntryAdder)
                                       throws IOException {
        super(conf, ledgerManager, ledgerDirsManager, checkpointSource, safeEntryAdder);
        this.memTable = new EntryMemTable(conf, checkpointSource);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                .setNameFormat("SortedLedgerStorage-%d")
                .setPriority((Thread.NORM_PRIORITY + Thread.MAX_PRIORITY)/2).build());
    }

    @Override
    public void start() {
        try {
            flush();
        } catch (IOException e) {
            LOG.error("Exception thrown while flushing ledger cache.", e);
        }
        super.start();
    }

    @Override
    public void shutdown() throws InterruptedException {
        // Wait for any jobs currently scheduled to be completed and then shut down.
        scheduler.shutdown();
        if (!scheduler.awaitTermination(3, TimeUnit.SECONDS)) {
            scheduler.shutdownNow();
        }
        super.shutdown();
    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        // Done this way because checking the skip list is an O(logN) operation compared to
        // the O(1) for the ledgerCache.
        if (!super.ledgerExists(ledgerId)) {
            EntryKeyValue kv = memTable.getLastEntry(ledgerId);
            if (null == kv) {
                return super.ledgerExists(ledgerId);
            }
        }
        return true;
    }

    @Override
    public long addEntry(ByteBuffer entry) throws IOException {
        long ledgerId = entry.getLong();
        long entryId = entry.getLong();
        entry.rewind();
        memTable.addEntry(ledgerId, entryId, entry, this);
        return entryId;
    }

    /**
     * Get the last entry id for a particular ledger.
     * @param ledgerId
     * @return
     */
    private ByteBuffer getLastEntryId(long ledgerId) throws IOException {
        EntryKeyValue kv = memTable.getLastEntry(ledgerId);
        if (null != kv) {
            return kv.getValueAsByteBuffer();
        }
        // If it doesn't exist in the skip list, then fallback to the ledger cache+index.
        return super.getEntry(ledgerId, BookieProtocol.LAST_ADD_CONFIRMED);
    }

    @Override
    public ByteBuffer getEntry(long ledgerId, long entryId) throws IOException {
        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            return getLastEntryId(ledgerId);
        }
        ByteBuffer buffToRet;
        try {
            buffToRet = super.getEntry(ledgerId, entryId);
        } catch (Bookie.NoEntryException nee) {
            EntryKeyValue kv = memTable.getEntry(ledgerId, entryId);
            if (null == kv) {
                // The entry might have been flushed since we last checked, so query the ledger cache again.
                // If the entry truly doesn't exist, then this will throw a NoEntryException
                buffToRet = super.getEntry(ledgerId, entryId);
            } else {
                buffToRet = kv.getValueAsByteBuffer();
            }
        }
        // buffToRet will not be null when we reach here.
        return buffToRet;
    }

    @Override
    public Checkpoint checkpoint(final Checkpoint checkpoint) throws IOException {
        memTable.flush(this, checkpoint);
        return super.checkpoint(checkpoint);
    }

    @Override
    public void process(long ledgerId, long entryId, ByteBuffer buffer) throws IOException {
        processEntry(ledgerId, entryId, buffer, false);
    }

    @Override
    public void flush() throws IOException {
        memTable.flush(this, Checkpoint.MAX);
        super.flush();
    }

    // CacheCallback functions.
    @Override
    public void onSizeLimitReached() throws IOException {
        // when size limit reached, we get the previous checkpoint from snapshot mem-table.
        // at this point, we are safer to schedule a checkpoint, since the entries added before
        // this checkpoint already written to entry logger.
        // but it would be better not to let mem-table flush to different entry log files,
        // so we roll entry log files in SortedLedgerStorage itself.
        // After that, we could make the process writing data to entry logger file not bound with checkpoint.
        // otherwise, it hurts add performance.
        scheduler.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    LOG.info("Started flushing mem table.");
                    memTable.flush(SortedLedgerStorage.this);
                    if (entryLogger.reachEntryLogLimit(0)) {
                        entryLogger.rollLog();
                        LOG.info("Rolling entry logger since it reached size limitation");
                    }
                } catch (IOException e) {
                    // TODO: if we failed to flush data, we should switch the bookie back to readonly mode
                    //       or shutdown it.
                    LOG.error("Exception thrown while flushing skip list cache.", e);
                }
            }
        });
    }
}
