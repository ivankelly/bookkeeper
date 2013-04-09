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

package org.apache.bookkeeper.bookie;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.bookkeeper.util.IOUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.bookie.EntryLogger.EntryLogListener;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.jmx.BKMBeanInfo;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.util.SnapshotMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interleave ledger storage
 * This ledger storage implementation stores all entries in a single
 * file and maintains an index file for each ledger.
 */
class InterleavedLedgerStorage implements LedgerStorage, EntryLogListener {
    final static Logger LOG = LoggerFactory.getLogger(InterleavedLedgerStorage.class);

    EntryLogger entryLogger;
    LedgerCache ledgerCache;

    // A sorted map to stored all active ledger ids
    protected final SnapshotMap<Long, Boolean> activeLedgers;

    // This is the thread that garbage collects the entry logs that do not
    // contain any active ledgers in them; and compacts the entry logs that
    // has lower remaining percentage to reclaim disk space.
    final GarbageCollectorThread gcThread;
    final ExecutorService syncExecutor;

    // this indicates that a write has happened since the last flush
    private volatile boolean somethingWritten = false;

    LedgerDirsManager ledgerDirsManager;

    private final Object markLock = new Object();
    private LogMark lastAddedMark = new LogMark();

    private final Object lastSyncedMarkLock = new Object();
    private LogMark lastSyncedMark;

    InterleavedLedgerStorage(ServerConfiguration conf, LedgerManager ledgerManager,
            LedgerDirsManager ledgerDirsManager,
            GarbageCollectorThread.SafeEntryAdder safeEntryAdder) throws IOException {
        activeLedgers = new SnapshotMap<Long, Boolean>();
        this.syncExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
                                                              .setNameFormat("SyncThread-%d")
                                                              .build());
        entryLogger = new EntryLogger(conf, ledgerDirsManager, this);
        ledgerCache = new LedgerCacheImpl(conf, activeLedgers, ledgerDirsManager);
        gcThread = new GarbageCollectorThread(conf, ledgerCache, entryLogger,
                activeLedgers, safeEntryAdder, ledgerManager);
        this.ledgerDirsManager = ledgerDirsManager;
        synchronized (lastSyncedMarkLock) {
            lastSyncedMark = readLastSyncedMark(ledgerDirsManager);
        }
    }

    @Override
    public void start() {
        gcThread.start();
    }

    @Override
    public void shutdown() throws InterruptedException {
        // shut down gc thread, which depends on zookeeper client
        // also compaction will write entries again to entry log file
        gcThread.shutdown();

        syncExecutor.shutdown();
        while (!syncExecutor.awaitTermination(5, TimeUnit.MINUTES)) {
            LOG.warn("Sync thread taking a long time to terminate");
        }
        try {
            flush();
        } catch (IOException ioe) {
            LOG.error("Error flushing storage on shutdown", ioe);
        }

        entryLogger.shutdown();

        try {
            ledgerCache.close();
        } catch (IOException e) {
            LOG.error("Error while closing the ledger cache", e);
        }
    }

    @Override
    public boolean setFenced(long ledgerId) throws IOException {
        return ledgerCache.setFenced(ledgerId);
    }

    @Override
    public boolean isFenced(long ledgerId) throws IOException {
        return ledgerCache.isFenced(ledgerId);
    }

    @Override
    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        ledgerCache.setMasterKey(ledgerId, masterKey);
    }

    @Override
    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        return ledgerCache.readMasterKey(ledgerId);
    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        return ledgerCache.ledgerExists(ledgerId);
    }

    @Override
    synchronized public long addEntry(ByteBuffer entry) throws IOException {
        long ledgerId = entry.getLong();
        long entryId = entry.getLong();
        entry.rewind();

        processEntry(ledgerId, entryId, entry);

        return entryId;
    }

    @Override
    public ByteBuffer getEntry(long ledgerId, long entryId) throws IOException {
        long offset;
        /*
         * If entryId is BookieProtocol.LAST_ADD_CONFIRMED, then return the last written.
         */
        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            entryId = ledgerCache.getLastEntry(ledgerId);
        }

        offset = ledgerCache.getEntryOffset(ledgerId, entryId);
        if (offset == 0) {
            throw new Bookie.NoEntryException(ledgerId, entryId);
        }
        return ByteBuffer.wrap(entryLogger.readEntry(ledgerId, entryId, offset));
    }

    /**
     * Flush the index cache
     * @return whether the flush succeeded
     */
    private boolean flushIndexCache() throws IOException {
        try {
            ledgerCache.flushLedger(true);
        } catch (LedgerDirsManager.NoWritableLedgerDirException e) {
            throw e;
        } catch (IOException ioe) {
            LOG.error("Exception flushing Ledger cache", ioe);
            return false;
        }
        return true;
    }

    private void checkpoint(LogMark mark) throws IOException {
        // we don't need to check somethingwritten since checkpoint
        // is scheduled when rotate an entry logger file. and we could
        // not set somethingWritten to false after checkpoint, since
        // current entry logger file isn't flushed yet.
        boolean flushFailed = !flushIndexCache();

        try {
            entryLogger.checkpoint();
        } catch (LedgerDirsManager.NoWritableLedgerDirException e) {
            throw e;
        } catch (IOException ioe) {
            LOG.error("Exception flushing Ledger", ioe);
            flushFailed = true;
        }
        if (flushFailed) {
            throw new IOException("Flushing to storage failed, check logs");
        }
        synchronized (lastSyncedMarkLock) {
            lastSyncedMark = mark;
        }
    }

    void flush() throws IOException {
        if (!somethingWritten) {
            return;
        }
        somethingWritten = false;
        LogMark mark;
        synchronized (markLock) {
            mark = lastAddedMark;
        }

        boolean flushFailed = !flushIndexCache();

        try {
            entryLogger.flush();
        } catch (LedgerDirsManager.NoWritableLedgerDirException e) {
            throw e;
        } catch (IOException ioe) {
            LOG.error("Exception flushing Ledger", ioe);
            flushFailed = true;
        }
        if (flushFailed) {
            throw new IOException("Flushing to storage failed, check logs");
        }
        synchronized (lastSyncedMarkLock) {
            lastSyncedMark = mark;
        }
        writeLastSyncedMark();
    }

    @Override
    public BKMBeanInfo getJMXBean() {
        return ledgerCache.getJMXBean();
    }

    protected void processEntry(long ledgerId, long entryId, ByteBuffer entry) throws IOException {
        processEntry(ledgerId, entryId, entry, true);
    }

    synchronized protected void processEntry(long ledgerId, long entryId,
                                             ByteBuffer entry, boolean rollLog)
            throws IOException {
        /*
         * Touch dirty flag
         */
        somethingWritten = true;

        /*
         * Log the entry
         */
        long pos = entryLogger.addEntry(ledgerId, entry, rollLog);

        /*
         * Set offset of entry id to be the current ledger position
         */
        ledgerCache.putEntryOffset(ledgerId, entryId, pos);
    }

    @Override
    public void onRotateEntryLog() {
        final LogMark mark = this.lastAddedMark;

        syncExecutor.submit(new Runnable() {
                public void run() {
                    try {
                        checkpoint(mark);
                        writeLastSyncedMark();
                    } catch (IOException ioe) {
                        LOG.warn("Failed to flush ledger storage", ioe);
                    }
                }
            });
    }

    @Override
    public void setLastAddedMark(LogMark logMark) {
        synchronized (markLock) {
            if (logMark.compareTo(this.lastAddedMark) > 0) {
                this.lastAddedMark = logMark;
            }
        }
    }

    @Override
    public LogMark getLastSyncedMark() throws IOException {
        synchronized (lastSyncedMarkLock) {
            return this.lastSyncedMark;
        }
    }

    private void writeLastSyncedMark() throws NoWritableLedgerDirException {
        synchronized (lastSyncedMarkLock) {
            byte buff[] = new byte[16];
            ByteBuffer bb = ByteBuffer.wrap(buff);
            // we should record <logId, logPosition> marked in markLog
            // which is safe since records before lastMark have been
            // persisted to disk (both index & entry logger)
            LogMark mark = lastSyncedMark;
            mark.writeLogMark(bb);
            LOG.debug("RollLog to persist last marked log : {}", mark);
            List<File> writableLedgerDirs = ledgerDirsManager
                .getWritableLedgerDirs();
            for (File dir : writableLedgerDirs) {
                File file = new File(dir, "lastMark");
                FileOutputStream fos = null;
                try {
                    fos = new FileOutputStream(file);
                    fos.write(buff);
                    fos.getChannel().force(true);
                    fos.close();
                    fos = null;
                } catch (IOException e) {
                    LOG.error("Problems writing to " + file, e);
                } finally {
                    // if stream already closed in try block successfully,
                    // stream might have nullified, in such case below
                    // call will simply returns
                    IOUtils.close(LOG, fos);
                }
            }
        }
    }

    /**
     * Read last mark from lastMark file.
     * The last mark should first be max journal log id,
     * and then max log position in max journal log.
     */
    static LogMark readLastSyncedMark(LedgerDirsManager ledgerDirsManager) {
        byte buff[] = new byte[16];
        ByteBuffer bb = ByteBuffer.wrap(buff);
        LogMark mark = new LogMark();
        for(File dir: ledgerDirsManager.getAllLedgerDirs()) {
            File file = new File(dir, "lastMark");
            try {
                FileInputStream fis = new FileInputStream(file);
                try {
                    int bytesRead = fis.read(buff);
                    if (bytesRead != 16) {
                        throw new IOException("Couldn't read enough bytes from lastMark."
                                              + " Wanted " + 16 + ", got " + bytesRead);
                    }
                } finally {
                    fis.close();
                }
                bb.clear();
                LogMark newMark = LogMark.readLogMark(bb);

                if (newMark.compareTo(mark) > 0) {
                    mark = newMark;
                }
            } catch (IOException e) {
                LOG.error("Problems reading from " + file
                          + " (this is okay if it is the first time starting this bookie");
            }
        }
        return mark;
    }
}
