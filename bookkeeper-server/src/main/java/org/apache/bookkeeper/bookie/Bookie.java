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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.FilenameFilter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.jmx.BKMBeanInfo;
import org.apache.bookkeeper.jmx.BKMBeanRegistry;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * Implements a bookie.
 *
 */

public class Bookie extends Thread {
    static Logger LOG = LoggerFactory.getLogger(Bookie.class);

    final static long MB = 1024 * 1024L;
    // max journal file size
    final long maxJournalSize;
    // number journal files kept before marked journal
    final int maxBackupJournals;

    final File journalDirectory;

    final File ledgerDirectories[];

    final ServerConfiguration conf;

    final SyncThread syncThread;
    final LedgerManager ledgerManager;
    final HandleRegistry handles;

    static final long METAENTRY_ID_LEDGER_KEY = -0x1000;

    // ZK registration path for this bookie
    static final String BOOKIE_REGISTRATION_PATH = "/ledgers/available/";
    static final String CURRENT_DIR = "current";

    // ZooKeeper client instance for the Bookie
    ZooKeeper zk;
    private volatile boolean isZkExpired = true;

    // Running flag
    private volatile boolean running = false;
    // Flag identify whether it is in shutting down progress
    private volatile boolean shuttingdown = false;

    private int exitCode = ExitCode.OK;

    // jmx related beans
    BookieBean jmxBookieBean;
    LedgerCacheBean jmxLedgerCacheBean;

    Map<Long, byte[]> masterKeyCache = Collections.synchronizedMap(new HashMap<Long, byte[]>());

    public static class NoLedgerException extends IOException {
        private static final long serialVersionUID = 1L;
        private long ledgerId;
        public NoLedgerException(long ledgerId) {
            this.ledgerId = ledgerId;
        }
        public long getLedgerId() {
            return ledgerId;
        }
    }
    public static class NoEntryException extends IOException {
        private static final long serialVersionUID = 1L;
        private long ledgerId;
        private long entryId;
        public NoEntryException(long ledgerId, long entryId) {
            super("Entry " + entryId + " not found in " + ledgerId);
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }
        public long getLedger() {
            return ledgerId;
        }
        public long getEntry() {
            return entryId;
        }
    }

    EntryLogger entryLogger;
    LedgerCache ledgerCache;
    // This is the thread that garbage collects the entry logs that do not
    // contain any active ledgers in them; and compacts the entry logs that
    // has lower remaining percentage to reclaim disk space.
    final GarbageCollectorThread gcThread;

    /**
     * SyncThread is a background thread which flushes ledger index pages periodically.
     * Also it takes responsibility of garbage collecting journal files.
     *
     * <p>
     * Before flushing, SyncThread first records a log marker {journalId, journalPos} in memory,
     * which indicates entries before this log marker would be persisted to ledger files.
     * Then sync thread begins flushing ledger index pages to ledger index files, flush entry
     * logger to ensure all entries persisted to entry loggers for future reads.
     * </p>
     * <p>
     * After all data has been persisted to ledger index files and entry loggers, it is safe
     * to persist the log marker to disk. If bookie failed after persist log mark,
     * bookie is able to relay journal entries started from last log mark without losing
     * any entries.
     * </p>
     * <p>
     * Those journal files whose id are less than the log id in last log mark, could be
     * removed safely after persisting last log mark. We provide a setting to let user keeping
     * number of old journal files which may be used for manual recovery in critical disaster.
     * </p>
     */
    class SyncThread extends Thread {
        volatile boolean running = true;
        // flag to ensure sync thread will not be interrupted during flush
        final AtomicBoolean flushing = new AtomicBoolean(false);
        // make flush interval as a parameter
        final int flushInterval;
        public SyncThread(ServerConfiguration conf) {
            super("SyncThread");
            flushInterval = conf.getFlushInterval();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Flush Interval : " + flushInterval);
            }
        }
        @Override
        public void run() {
            while(running) {
                synchronized(this) {
                    try {
                        wait(flushInterval);
                        if (!entryLogger.testAndClearSomethingWritten()) {
                            continue;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        continue;
                    }
                }

                // try to mark flushing flag to make sure it would not be interrupted
                // by shutdown during flushing. otherwise it will receive
                // ClosedByInterruptException which may cause index file & entry logger
                // closed and corrupted.
                if (!flushing.compareAndSet(false, true)) {
                    // set flushing flag failed, means flushing is true now
                    // indicates another thread wants to interrupt sync thread to exit
                    break;
                }

                lastLogMark.markLog();

                boolean flushFailed = false;
                try {
                    ledgerCache.flushLedger(true);
                } catch (IOException e) {
                    LOG.error("Exception flushing Ledger", e);
                    flushFailed = true;
                }
                try {
                    entryLogger.flush();
                } catch (IOException e) {
                    LOG.error("Exception flushing entry logger", e);
                    flushFailed = true;
                }

                // if flush failed, we should not roll last mark, otherwise we would
                // have some ledgers are not flushed and their journal entries were lost
                if (!flushFailed) {

                    lastLogMark.rollLog();

                    // list the journals that have been marked
                    List<Long> logs = listJournalIds(journalDirectory, new JournalIdFilter() {
                        @Override
                        public boolean accept(long journalId) {
                            if (journalId < lastLogMark.lastMark.txnLogId) {
                                return true;
                            } else {
                                return false;
                            }
                        }
                    });

                    // keep MAX_BACKUP_JOURNALS journal files before marked journal
                    if (logs.size() >= maxBackupJournals) {
                        int maxIdx = logs.size() - maxBackupJournals;
                        for (int i=0; i<maxIdx; i++) {
                            long id = logs.get(i);
                            // make sure the journal id is smaller than marked journal id
                            if (id < lastLogMark.lastMark.txnLogId) {
                                File journalFile = new File(journalDirectory, Long.toHexString(id) + ".txn");
                                journalFile.delete();
                                LOG.info("garbage collected journal " + journalFile.getName());
                            }
                        }
                    }

                }

                // clear flushing flag
                flushing.set(false);
            }
        }

        // shutdown sync thread
        void shutdown() throws InterruptedException {
            running = false;
            if (flushing.compareAndSet(false, true)) {
                // if setting flushing flag succeed, means syncThread is not flushing now
                // it is safe to interrupt itself now 
                this.interrupt();
            }
            this.join();
        }
    }

    public static void checkDirectoryStructure(File dir) throws IOException {
        if (!dir.exists()) {
            File parent = dir.getParentFile();
            File preV3versionFile = new File(dir.getParent(), Cookie.VERSION_FILENAME);

            final AtomicBoolean oldDataExists = new AtomicBoolean(false);
            parent.list(new FilenameFilter() {
                    public boolean accept(File dir, String name) {
                        if (name.endsWith(".txn") || name.endsWith(".idx") || name.endsWith(".log")) {
                            oldDataExists.set(true);
                        }
                        return true;
                    }
                });
            if (preV3versionFile.exists() || oldDataExists.get()) {
                String err = "Directory layout version is less than 3, upgrade needed";
                LOG.error(err);
                throw new IOException(err);
            }
            dir.mkdirs();
        }
    }

    /**
     * Check that the environment for the bookie is correct.
     * This means that the configuration has stayed the same as the
     * first run and the filesystem structure is up to date.
     */
    private void checkEnvironment(ZooKeeper zk) throws BookieException, IOException {
        if (zk == null) { // exists only for testing, just make sure directories are correct
            checkDirectoryStructure(journalDirectory);
            for (File dir : ledgerDirectories) {
                    checkDirectoryStructure(dir);
            }
            return;
        }
        try {
            boolean newEnv = false;
            Cookie masterCookie = Cookie.generateCookie(conf);
            try {
                Cookie zkCookie = Cookie.readFromZooKeeper(zk, conf);
                masterCookie.verify(zkCookie);
            } catch (KeeperException.NoNodeException nne) {
                newEnv = true;
            }
            try {
                checkDirectoryStructure(journalDirectory);

                Cookie journalCookie = Cookie.readFromDirectory(journalDirectory);
                journalCookie.verify(masterCookie);
                for (File dir : ledgerDirectories) {
                    checkDirectoryStructure(dir);
                    Cookie c = Cookie.readFromDirectory(dir);
                    c.verify(masterCookie);
                }
            } catch (FileNotFoundException fnf) {
                if (!newEnv){
                    LOG.error("Cookie exists in zookeeper, but not in all local directories", fnf);
                    throw new BookieException.InvalidCookieException();
                }

                masterCookie.writeToDirectory(journalDirectory);
                for (File dir : ledgerDirectories) {
                    masterCookie.writeToDirectory(dir);
                }
                masterCookie.writeToZooKeeper(zk, conf);
            }
        } catch (KeeperException ke) {
            LOG.error("Couldn't access cookie in zookeeper", ke);
            throw new BookieException.InvalidCookieException(ke);
        } catch (UnknownHostException uhe) {
            LOG.error("Couldn't check cookies, networking is broken", uhe);
            throw new BookieException.InvalidCookieException(uhe);
        } catch (IOException ioe) {
            LOG.error("Error accessing cookie on disks", ioe);
            throw new BookieException.InvalidCookieException(ioe);
        } catch (InterruptedException ie) {
            LOG.error("Thread interrupted while checking cookies, exiting", ie);
            throw new BookieException.InvalidCookieException(ie);
        }
    }

    public static File getCurrentDirectory(File dir) {
        return new File(dir, CURRENT_DIR);
    }

    public static File[] getCurrentDirectories(File[] dirs) {
        File[] currentDirs = new File[dirs.length];
        for (int i = 0; i < dirs.length; i++) {
            currentDirs[i] = getCurrentDirectory(dirs[i]);
        }
        return currentDirs;
    }

    /**
     * Scanner used to do entry log compaction
     */
    class EntryLogCompactionScanner implements EntryLogger.EntryLogScanner {
        @Override
        public boolean accept(long ledgerId) {
            // bookie has no knowledge about which ledger is deleted
            // so just accept all ledgers.
            return true;
        }

        @Override
        public void process(long ledgerId, ByteBuffer buffer)
            throws IOException {
            try {
                Bookie.this.addEntryByLedgerId(ledgerId, buffer);
            } catch (BookieException be) {
                throw new IOException(be);
            }
        }
    }

    public Bookie(ServerConfiguration conf)
            throws IOException, KeeperException, InterruptedException, BookieException {
        super("Bookie-" + conf.getBookiePort());
        this.conf = conf;
        this.journalDirectory = getCurrentDirectory(conf.getJournalDir());
        this.ledgerDirectories = getCurrentDirectories(conf.getLedgerDirs());
        this.maxJournalSize = conf.getMaxJournalSize() * MB;
        this.maxBackupJournals = conf.getMaxBackupJournals();

        // instantiate zookeeper client to initialize ledger manager
        this.zk = instantiateZookeeperClient(conf);
        checkEnvironment(this.zk);

        ledgerManager = LedgerManagerFactory.newLedgerManager(conf, this.zk);

        syncThread = new SyncThread(conf);
        entryLogger = new EntryLogger(conf);
        ledgerCache = new LedgerCacheImpl(conf, ledgerManager);
        gcThread = new GarbageCollectorThread(conf, this.zk, ledgerCache, entryLogger,
                ledgerManager, new EntryLogCompactionScanner());
        handles = new HandleRegistryImpl(entryLogger, ledgerCache);

        // replay journals
        readJournal();
    }

    private void readJournal() throws IOException, BookieException {
        lastLogMark.readLog();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Last Log Mark : " + lastLogMark);
        }
        final long markedLogId = lastLogMark.txnLogId;
        List<Long> logs = listJournalIds(journalDirectory, new JournalIdFilter() {
            @Override
            public boolean accept(long journalId) {
                if (journalId < markedLogId) {
                    return false;
                }
                return true;
            }
        });
        // last log mark may be missed due to no sync up before
        // validate filtered log ids only when we have markedLogId
        if (markedLogId > 0) {
            if (logs.size() == 0 || logs.get(0) != markedLogId) {
                throw new IOException("Recovery log " + markedLogId + " is missing");
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Try to relay journal logs : " + logs);
        }
        // TODO: When reading in the journal logs that need to be synced, we
        // should use BufferedChannels instead to minimize the amount of
        // system calls done.
        ByteBuffer lenBuff = ByteBuffer.allocate(4);
        ByteBuffer recBuff = ByteBuffer.allocate(64*1024);
        for(Long id: logs) {
            JournalChannel recLog;
            if(id == markedLogId) {
                long markedLogPosition = lastLogMark.txnLogPosition;
                recLog = new JournalChannel(journalDirectory, id, markedLogPosition);
            } else {
                recLog = new JournalChannel(journalDirectory, id);
            }

            while(true) {
                lenBuff.clear();
                fullRead(recLog, lenBuff);
                if (lenBuff.remaining() != 0) {
                    break;
                }
                lenBuff.flip();
                int len = lenBuff.getInt();
                if (len == 0) {
                    break;
                }
                recBuff.clear();
                if (recBuff.remaining() < len) {
                    recBuff = ByteBuffer.allocate(len);
                }
                recBuff.limit(len);
                if (fullRead(recLog, recBuff) != len) {
                    // This seems scary, but it just means that this is where we
                    // left off writing
                    break;
                }
                recBuff.flip();
                long ledgerId = recBuff.getLong();
                long entryId = recBuff.getLong();
                try {
                    LOG.debug("Replay journal - ledger id : {}, entry id : {}.", ledgerId, entryId);
                    if (entryId == METAENTRY_ID_LEDGER_KEY) {
                        if (recLog.getFormatVersion() >= 3) {
                            int masterKeyLen = recBuff.getInt();
                            byte[] masterKey = new byte[masterKeyLen];

                            recBuff.get(masterKey);
                            masterKeyCache.put(ledgerId, masterKey);
                        } else {
                            throw new IOException("Invalid journal. Contains journalKey "
                                    + " but layout version (" + recLog.getFormatVersion()
                                    + ") is too old to hold this");
                        }
                    } else {
                        byte[] key = masterKeyCache.get(ledgerId);
                        if (key == null) {
                            key = ledgerCache.readMasterKey(ledgerId);
                        }
                        LedgerDescriptor handle = handles.getHandle(ledgerId, key);

                        try {
                            recBuff.rewind();
                            handle.addEntry(recBuff);
                        } finally {
                            handles.releaseHandle(handle);
                        }
                    }
                } catch (NoLedgerException nsle) {
                    LOG.debug("Skip replaying entries of ledger {} since it was deleted.", ledgerId);
                    continue;
                }
            }
            recLog.close();
        }
    }

    synchronized public void start() {
        setDaemon(true);
        LOG.debug("I'm starting a bookie with journal directory " + journalDirectory.getName());
        super.start();
        syncThread.start();
        gcThread.start();
        // set running here.
        // since bookie server use running as a flag to tell bookie server whether it is alive
        // if setting it in bookie thread, the watcher might run before bookie thread.
        running = true;
        try {
            registerBookie(conf.getBookiePort());
        } catch (IOException e) {
            LOG.error("Couldn't register bookie with zookeeper, shutting down", e);
            shutdown(ExitCode.ZK_REG_FAIL);
        }
    }

    public static interface JournalIdFilter {
        public boolean accept(long journalId);
    }

    /**
     * List all journal ids by a specified journal id filer
     *
     * @param journalDir journal dir
     * @param filter journal id filter
     * @return list of filtered ids
     */
    public static List<Long> listJournalIds(File journalDir, JournalIdFilter filter) {
        File logFiles[] = journalDir.listFiles();
        List<Long> logs = new ArrayList<Long>();
        for(File f: logFiles) {
            String name = f.getName();
            if (!name.endsWith(".txn")) {
                continue;
            }
            String idString = name.split("\\.")[0];
            long id = Long.parseLong(idString, 16);
            if (filter != null) {
                if (filter.accept(id)) {
                    logs.add(id);
                }
            } else {
                logs.add(id);
            }
        }
        Collections.sort(logs);
        return logs;
    }

    /**
     * Register jmx with parent
     *
     * @param parent parent bk mbean info
     */
    public void registerJMX(BKMBeanInfo parent) {
        try {
            jmxBookieBean = new BookieBean(this);
            BKMBeanRegistry.getInstance().register(jmxBookieBean, parent);

            try {
                jmxLedgerCacheBean = new LedgerCacheBean((LedgerCacheImpl)this.ledgerCache);
                BKMBeanRegistry.getInstance().register(jmxLedgerCacheBean, jmxBookieBean);
            } catch (Exception e) {
                LOG.warn("Failed to register with JMX for ledger cache", e);
                jmxLedgerCacheBean = null;
            }

        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxBookieBean = null;
        }
    }

    /**
     * Unregister jmx
     */
    public void unregisterJMX() {
        try {
            if (jmxLedgerCacheBean != null) {
                BKMBeanRegistry.getInstance().unregister(jmxLedgerCacheBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        try {
            if (jmxBookieBean != null) {
                BKMBeanRegistry.getInstance().unregister(jmxBookieBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        jmxBookieBean = null;
        jmxLedgerCacheBean = null;
    }


    /**
     * Instantiate the ZooKeeper client for the Bookie.
     */
    private ZooKeeper instantiateZookeeperClient(ServerConfiguration conf) throws IOException {
        if (conf.getZkServers() == null) {
            LOG.warn("No ZK servers passed to Bookie constructor so BookKeeper clients won't know about this server!");
            isZkExpired = false;
            return null;
        }
        // Create the ZooKeeper client instance
        return newZookeeper(conf.getZkServers(), conf.getZkTimeout());
    }

    /**
     * Register as an available bookie
     */
    private void registerBookie(int port) throws IOException {
        if (null == zk) {
            // zookeeper instance is null, means not register itself to zk
            return;
        }
        // Create the ZK ephemeral node for this Bookie.
        try {
            zk.create(BOOKIE_REGISTRATION_PATH + InetAddress.getLocalHost().getHostAddress() + ":" + port, new byte[0],
                      Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            LOG.error("ZK exception registering ephemeral Znode for Bookie!", e);
            // Throw an IOException back up. This will cause the Bookie
            // constructor to error out. Alternatively, we could do a System
            // exit here as this is a fatal error.
            throw new IOException(e);
        }
    }

    /**
     * Create a new zookeeper client to zk cluster.
     *
     * <p>
     * Bookie Server just used zk client when syncing ledgers for garbage collection.
     * So when zk client is expired, it means this bookie server is not available in
     * bookie server list. The bookie client will be notified for its expiration. No
     * more bookie request will be sent to this server. So it's better to exit when zk
     * expired.
     * </p>
     * <p>
     * Since there are lots of bk operations cached in queue, so we wait for all the operations
     * are processed and quit. It is done by calling <b>shutdown</b>.
     * </p>
     *
     * @param zkServers the quorum list of zk servers
     * @param sessionTimeout session timeout of zk connection
     *
     * @return zk client instance
     */
    private ZooKeeper newZookeeper(final String zkServers,
                                   final int sessionTimeout) throws IOException {
        ZooKeeper newZk = new ZooKeeper(zkServers, sessionTimeout,
        new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // handle session disconnects and expires
                if (event.getType()
                .equals(Watcher.Event.EventType.None)) {
                    if (event.getState().equals(
                    Watcher.Event.KeeperState.Disconnected)) {
                        LOG.warn("ZK client has been disconnected to the ZK server!");
                    } else if (event.getState().equals(
                    Watcher.Event.KeeperState.SyncConnected)) {
                        LOG.info("ZK client has been reconnected to the ZK server!");
                    }
                }
                // Check for expired connection.
                if (event.getState().equals(
                Watcher.Event.KeeperState.Expired)) {
                    LOG.error("ZK client connection to the ZK server has expired!");
                    isZkExpired = true;
                    shutdown(ExitCode.ZK_EXPIRED);
                }
            }
        });
        isZkExpired = false;
        return newZk;
    }

    private static int fullRead(JournalChannel fc, ByteBuffer bb) throws IOException {
        int total = 0;
        while(bb.remaining() > 0) {
            int rc = fc.read(bb);
            if (rc <= 0) {
                return total;
            }
            total += rc;
        }
        return total;
    }

    static class QueueEntry {
        QueueEntry(ByteBuffer entry, long ledgerId, long entryId,
                   WriteCallback cb, Object ctx) {
            this.entry = entry.duplicate();
            this.cb = cb;
            this.ctx = ctx;
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }

        ByteBuffer entry;

        long ledgerId;

        long entryId;

        WriteCallback cb;

        Object ctx;
    }

    LinkedBlockingQueue<QueueEntry> queue = new LinkedBlockingQueue<QueueEntry>();

    class LastLogMark {
        long txnLogId;
        long txnLogPosition;
        LastLogMark lastMark;
        LastLogMark(long logId, long logPosition) {
            this.txnLogId = logId;
            this.txnLogPosition = logPosition;
        }
        synchronized void setLastLogMark(long logId, long logPosition) {
            txnLogId = logId;
            txnLogPosition = logPosition;
        }
        synchronized void markLog() {
            lastMark = new LastLogMark(txnLogId, txnLogPosition);
        }
        synchronized void rollLog() {
            byte buff[] = new byte[16];
            ByteBuffer bb = ByteBuffer.wrap(buff);
            // we should record <logId, logPosition> marked in markLog
            // which is safe since records before lastMark have been
            // persisted to disk (both index & entry logger)
            bb.putLong(lastMark.txnLogId);
            bb.putLong(lastMark.txnLogPosition);
            if (LOG.isDebugEnabled()) {
                LOG.debug("RollLog to persist last marked log : " + lastMark);
            }
            for(File dir: ledgerDirectories) {
                File file = new File(dir, "lastMark");
                try {
                    FileOutputStream fos = new FileOutputStream(file);
                    fos.write(buff);
                    fos.getChannel().force(true);
                    fos.close();
                } catch (IOException e) {
                    LOG.error("Problems writing to " + file, e);
                }
            }
        }

        /**
         * Read last mark from lastMark file.
         * The last mark should first be max journal log id,
         * and then max log position in max journal log.
         */
        synchronized void readLog() {
            byte buff[] = new byte[16];
            ByteBuffer bb = ByteBuffer.wrap(buff);
            for(File dir: ledgerDirectories) {
                File file = new File(dir, "lastMark");
                try {
                    FileInputStream fis = new FileInputStream(file);
                    fis.read(buff);
                    fis.close();
                    bb.clear();
                    long i = bb.getLong();
                    long p = bb.getLong();
                    if (i > txnLogId) {
                        txnLogId = i;
                        if(p > txnLogPosition) {
                          txnLogPosition = p;
                        }
                    }
                } catch (IOException e) {
                    LOG.error("Problems reading from " + file + " (this is okay if it is the first time starting this bookie");
                }
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            
            sb.append("LastMark: logId - ").append(txnLogId)
              .append(" , position - ").append(txnLogPosition);
            
            return sb.toString();
        }
    }

    private LastLogMark lastLogMark = new LastLogMark(0, 0);

    LastLogMark getLastLogMark() {
        return lastLogMark;
    }

    public boolean isRunning() {
        return running;
    }

    /**
     * A thread used for persisting journal entries to journal files.
     * 
     * <p>
     * Besides persisting journal entries, it also takes responsibility of
     * rolling journal files when a journal file reaches journal file size
     * limitation.
     * </p>
     * <p>
     * During journal rolling, it first closes the writing journal, generates
     * new journal file using current timestamp, and continue persistence logic.
     * Those journals will be garbage collected in SyncThread.
     * </p>
     */
    @Override
    public void run() {
        LinkedList<QueueEntry> toFlush = new LinkedList<QueueEntry>();
        ByteBuffer lenBuff = ByteBuffer.allocate(4);
        try {
            long logId = 0;
            JournalChannel logFile = null;
            BufferedChannel bc = null;
            long nextPrealloc = 0;
            long lastFlushPosition = 0;

            QueueEntry qe = null;
            while (true) {
                // new journal file to write
                if (null == logFile) {
                    logId = System.currentTimeMillis();
                    logFile = new JournalChannel(journalDirectory, logId);
                    bc = logFile.getBufferedChannel();

                    lastFlushPosition = 0;
                }

                if (qe == null) {
                    if (toFlush.isEmpty()) {
                        qe = queue.take();
                    } else {
                        qe = queue.poll();
                        if (qe == null || bc.position() > lastFlushPosition + 512*1024) {
                            //logFile.force(false);
                            bc.flush(true);
                            lastFlushPosition = bc.position();
                            lastLogMark.setLastLogMark(logId, lastFlushPosition);
                            for (QueueEntry e : toFlush) {
                                e.cb.writeComplete(0, e.ledgerId, e.entryId, null, e.ctx);
                            }
                            toFlush.clear();

                            // check whether journal file is over file limit
                            if (bc.position() > maxJournalSize) {
                                logFile.close();
                                logFile = null;
                                continue;
                            }
                        }
                    }
                }

                if (isZkExpired) {
                    LOG.warn("Exiting... zk client has expired.");
                    break;
                }
                if (qe == null) { // no more queue entry
                    continue;
                }
                lenBuff.clear();
                lenBuff.putInt(qe.entry.remaining());
                lenBuff.flip();
                //
                // we should be doing the following, but then we run out of
                // direct byte buffers
                // logFile.write(new ByteBuffer[] { lenBuff, qe.entry });
                bc.write(lenBuff);
                bc.write(qe.entry);

                logFile.preAllocIfNeeded();

                toFlush.add(qe);
                qe = null;
            }
        } catch (Exception e) {
            // if the bookie thread quits due to shutting down, it is ok
            if (shuttingdown) {
                LOG.warn("Bookie thread exits when shutting down", e);
            } else {
                // some error found in bookie thread and it quits
                // following add operations to it would hang unit client timeout
                // so we should let bookie server exists
                LOG.error("Exception occurred in bookie thread and it quits : ", e);
                shutdown(ExitCode.BOOKIE_EXCEPTION);
            }
        }
    }

    // provided a public shutdown method for other caller
    // to shut down bookie gracefully
    public int shutdown() {
        return shutdown(ExitCode.OK);
    }

    // internal shutdown method to let shutdown bookie gracefully
    // when encountering exception
    synchronized int shutdown(int exitCode) {
        try {
            if (running) { // avoid shutdown twice
                // the exitCode only set when first shutdown usually due to exception found
                this.exitCode = exitCode;
                // mark bookie as in shutting down progress
                shuttingdown = true;
                // shut down gc thread, which depends on zookeeper client
                // also compaction will write entries again to entry log file
                gcThread.shutdown();
                // Shutdown the ZK client
                if(zk != null) zk.close();
                this.interrupt();
                this.join();
                syncThread.shutdown();

                // Shutdown the EntryLogger which has the GarbageCollector Thread running
                entryLogger.shutdown();
                // close Ledger Manager
                ledgerManager.close();
                // setting running to false here, so watch thread in bookie server know it only after bookie shut down
                running = false;
            }
        } catch (InterruptedException ie) {
            LOG.error("Interrupted during shutting down bookie : ", ie);
        }
        return exitCode;
    }

    /** 
     * Retrieve the ledger descriptor for the ledger which entry should be added to.
     * The LedgerDescriptor returned from this method should be eventually freed with 
     * #putHandle().
     *
     * @throws BookieException if masterKey does not match the master key of the ledger
     */
    private LedgerDescriptor getLedgerForEntry(ByteBuffer entry, byte[] masterKey) 
            throws IOException, BookieException {
        long ledgerId = entry.getLong();
        LedgerDescriptor l = handles.getHandle(ledgerId, masterKey);
        if (!masterKeyCache.containsKey(ledgerId)) {
            // new handle, we should add the key to journal ensure we can rebuild
            ByteBuffer bb = ByteBuffer.allocate(8 + 8 + 4 + masterKey.length);
            bb.putLong(ledgerId);
            bb.putLong(METAENTRY_ID_LEDGER_KEY);
            bb.putInt(masterKey.length);
            bb.put(masterKey);
            bb.flip();

            queue.add(new QueueEntry(bb,
                                     ledgerId, METAENTRY_ID_LEDGER_KEY,
                                     new WriteCallback() {
                                         public void writeComplete(int rc, long ledgerId, 
                                                 long entryId, InetSocketAddress addr,
                                                 Object ctx) {
                                             // do nothing
                                         }
                                     }, null));
            masterKeyCache.put(ledgerId, masterKey);
        }
        return l;
    }

    protected void addEntryByLedgerId(long ledgerId, ByteBuffer entry)
        throws IOException, BookieException {
        /** TODOIK fix this up
           LedgerDescriptor handle = getHandle(ledgerId);
        try {
            handle.addEntry(entry);
        } finally {
            putHandle(handle);
            }*/
    }

    /**
     * Add an entry to a ledger as specified by handle. 
     */
    private void addEntryInternal(LedgerDescriptor handle, ByteBuffer entry, WriteCallback cb, Object ctx)
            throws IOException, BookieException {
        long ledgerId = handle.getLedgerId();
        entry.rewind();
        long entryId = handle.addEntry(entry);

        entry.rewind();
        if (LOG.isTraceEnabled()) {
            LOG.trace("Adding " + entryId + "@" + ledgerId);
        }
        queue.add(new QueueEntry(entry, ledgerId, entryId, cb, ctx));
    }

    /**
     * Add entry to a ledger, even if the ledger has previous been fenced. This should only
     * happen in bookie recovery or ledger recovery cases, where entries are being replicates 
     * so that they exist on a quorum of bookies. The corresponding client side call for this
     * is not exposed to users.
     */
    public void recoveryAddEntry(ByteBuffer entry, WriteCallback cb, Object ctx, byte[] masterKey) 
            throws IOException, BookieException {
        LedgerDescriptor handle = getLedgerForEntry(entry, masterKey);
        synchronized (handle) {
            try {
                addEntryInternal(handle, entry, cb, ctx);
            } finally {
                handles.releaseHandle(handle);
            }
        }
    }
    
    /** 
     * Add entry to a ledger.
     * @throws BookieException.LedgerFencedException if the ledger is fenced
     */
    public void addEntry(ByteBuffer entry, WriteCallback cb, Object ctx, byte[] masterKey)
            throws IOException, BookieException {
        LedgerDescriptor handle = getLedgerForEntry(entry, masterKey);
        synchronized (handle) {
            try {
                if (handle.isFenced()) {
                    throw BookieException.create(BookieException.Code.LedgerFencedException);
                }
                
                addEntryInternal(handle, entry, cb, ctx);
            } finally {
                handles.releaseHandle(handle);
            }
        }
    }

    /**
     * Fences a ledger. From this point on, clients will be unable to 
     * write to this ledger. Only recoveryAddEntry will be
     * able to add entries to the ledger.
     * This method is idempotent. Once a ledger is fenced, it can
     * never be unfenced. Fencing a fenced ledger has no effect.
     */
    public void fenceLedger(long ledgerId) throws IOException {
        /* IKTODO fix this up
           LedgerDescriptor handle = getHandle(ledgerId);
        synchronized (handle) {
            handle.setFenced();
            }*/
    }

    public ByteBuffer readEntry(long ledgerId, long entryId)
            throws IOException, BookieException {
        LedgerDescriptor handle = handles.getReadOnlyHandle(ledgerId);
        try {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Reading " + entryId + "@" + ledgerId);
            }
            return handle.readEntry(entryId);
        } finally {
            handles.releaseHandle(handle);
        }
    }

    // The rest of the code is test stuff
    static class CounterCallback implements WriteCallback {
        int count;

        synchronized public void writeComplete(int rc, long l, long e, InetSocketAddress addr, Object ctx) {
            count--;
            if (count == 0) {
                notifyAll();
            }
        }

        synchronized public void incCount() {
            count++;
        }

        synchronized public void waitZero() throws InterruptedException {
            while (count > 0) {
                wait();
            }
        }
    }

    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) 
            throws IOException, InterruptedException, BookieException, KeeperException {
        Bookie b = new Bookie(new ServerConfiguration());
        b.start();
        CounterCallback cb = new CounterCallback();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            ByteBuffer buff = ByteBuffer.allocate(1024);
            buff.putLong(1);
            buff.putLong(i);
            buff.limit(1024);
            buff.position(0);
            cb.incCount();
            b.addEntry(buff, cb, null, new byte[0]);
        }
        cb.waitZero();
        long end = System.currentTimeMillis();
        System.out.println("Took " + (end-start) + "ms");
    }
}
