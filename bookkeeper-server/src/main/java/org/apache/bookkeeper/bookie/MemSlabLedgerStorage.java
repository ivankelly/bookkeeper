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

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import java.nio.ByteBuffer;
import java.io.IOException;

import org.apache.bookkeeper.jmx.BKMBeanInfo;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.ActiveLedgerManager;
import org.apache.bookkeeper.proto.BookieProtocol;

import java.util.concurrent.ConcurrentSkipListMap;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Lists;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MemSlabLedgerStorage extends InterleavedLedgerStorage {
    final static Logger LOG = LoggerFactory.getLogger(MemSlabLedgerStorage.class);

    ReadWriteLock flushRWLock;
    Object flushLock;
    SlabAllocator allocator;
    ConcurrentHashMap<Long, LedgerSlab> writeSlabs;
    SetMultimap<Long, LedgerSlab> fullWriteSlabs;

    ConcurrentHashMap<Long, LedgerSlab> flushSlabs;
    SetMultimap<Long, LedgerSlab> fullFlushSlabs;
    AtomicBoolean flushRequired = new AtomicBoolean(false);

    Thread flushThread;

    MemSlabLedgerStorage(final ServerConfiguration conf,
                         ActiveLedgerManager activeLedgerManager,
                         LedgerDirsManager ledgerDirsManager) throws IOException {
        super(conf, activeLedgerManager, ledgerDirsManager);
        writeSlabs = new ConcurrentHashMap<Long, LedgerSlab>();
        SetMultimap<Long, LedgerSlab> mm = LinkedHashMultimap.create();
        fullWriteSlabs = Multimaps.synchronizedSetMultimap(mm);

        flushSlabs = new ConcurrentHashMap<Long, LedgerSlab>();
        mm = LinkedHashMultimap.create();
        fullFlushSlabs = Multimaps.synchronizedSetMultimap(mm);

        flushLock = new Object();
        allocator = new SlabAllocator(conf);
        flushRWLock = new ReentrantReadWriteLock();

        flushThread = new Thread("phase1Flush") {
                public void run() {
                    while (true) {
                        try {
                            sleep(conf.getFlushInterval());
                            softFlush();
                        } catch(Exception e) {
                            LOG.error("Flush thread died, no more entries can be written", e);
                            return;
                        }
                    }
                }
            };
        flushThread.setDaemon(true);
    }

    public void startFlushThread() {
        flushThread.start();
    }

    @Override
    public void shutdown() throws InterruptedException {
        super.shutdown();
        flushThread.interrupt();
        flushThread.join();
    }

    @Override
    public long addEntry(ByteBuffer entry) throws IOException {
        long ledgerId = entry.getLong();
        long entryId = entry.getLong();
        entry.rewind();

        flushRWLock.readLock().lock();
        try {
            LedgerSlab slab = writeSlabs.get(ledgerId);
            if (slab == null) {
                flushRWLock.readLock().unlock();
                slab = allocator.getCleanSlab(ledgerId);
                flushRWLock.readLock().lock();


                LedgerSlab curSlab = writeSlabs.putIfAbsent(ledgerId, slab);
                if (curSlab != null) {
                    allocator.freeSlab(slab);
                    slab = curSlab;
                }
            }
            slab.addEntry(entryId, entry);

            if (slab.isFull()) {
                fullWriteSlabs.put(ledgerId, slab);
                writeSlabs.remove(ledgerId, slab);
            }
        } catch (InterruptedException ie) {
            throw new IOException("Interrupted while waiting for slab", ie);
        } finally {
            flushRWLock.readLock().unlock();
        }

        return entryId;
    }

    private List<LedgerSlab> getSlabs(long ledgerId) {
        List<LedgerSlab> slabs = Lists.newArrayList();
        LedgerSlab s = writeSlabs.get(ledgerId);
        if (s != null) {
            slabs.add(s);
        }
        slabs.addAll(Lists.reverse(Lists.newArrayList(fullWriteSlabs.get(ledgerId))));

        s = flushSlabs.get(ledgerId);
        if (s != null) {
            slabs.add(s);
        }
        slabs.addAll(Lists.reverse(Lists.newArrayList(fullFlushSlabs.get(ledgerId))));
        return slabs;
    }

    @Override
    public ByteBuffer getEntry(long ledgerId, long entryId) throws IOException {
        // need to hadne         if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
        // TODO deal with the case of a slab being taken, but the flusher releases it
        // and has passed to another ledger
        /*
        LedgerSlab slab = writeSlabs.get(ledgerId);
        if (slab != null) {
            b = slab.getEntry(entryId);
        }
        if (b == null) {
            slab = flushSlabs.get(ledgerId);
            if (slab != null) {
                b = slab.getEntry(entryId);
            }
            }*/
        ByteBuffer b = null;
        for (LedgerSlab s : getSlabs(ledgerId)) {
            b = s.getEntry(entryId);
            if (b != null) {
                break;
            }
        }
        if (b == null) {
            b = super.getEntry(ledgerId, entryId);
        }
        return b;
    }

    @Override
    public boolean isFlushRequired() {
        return flushRequired.get();
    }

    private void softFlush() throws IOException {
        synchronized(flushLock) {
            flushRWLock.writeLock().lock();
            if (writeSlabs.size() == 0) { // nothing written
                flushRWLock.writeLock().unlock();
                return;
            }
            ConcurrentHashMap<Long, LedgerSlab> toFlush = writeSlabs;
            writeSlabs = flushSlabs;
            flushSlabs = toFlush;
            
            SetMultimap<Long, LedgerSlab> toFlushFullSlabs = fullWriteSlabs;
            fullWriteSlabs = fullFlushSlabs;
            fullFlushSlabs = toFlushFullSlabs;
            
            flushRWLock.writeLock().unlock();
        
            for (Map.Entry<Long, LedgerSlab> e : flushSlabs.entrySet()) {
                fullFlushSlabs.put(e.getKey(), e.getValue());
            }
            flushSlabs.clear();
            
            //            LOG.debug("Flushing entrylog");
            Map<Long, Map<Long, Long>> offsetMap = new HashMap<Long, Map<Long, Long>>();
            for (Long key : fullFlushSlabs.keySet()) {
                ConcurrentSkipListMap<Long,Long> offsets = new ConcurrentSkipListMap<Long,Long>();
                for (LedgerSlab s : fullFlushSlabs.get(key)) {
                    s.flush(entryLogger, offsets);
                }
                offsetMap.put(key, offsets);
            }
            entryLogger.flush();
            //LOG.debug("Flushing indexes");
            int num = 0;
            for (Map.Entry<Long, Map<Long,Long>> e : offsetMap.entrySet()) {
                num++;
                for (Map.Entry<Long, Long> off : e.getValue().entrySet()) {
                    ledgerCache.putEntryOffset(e.getKey(), off.getKey(), off.getValue());
                }
            
                for (LedgerSlab s : fullFlushSlabs.removeAll(e.getKey())) {
                    try {
                        allocator.freeSlab(s);
                    } catch (InterruptedException ie) {
                        LOG.error("InterruptedException ", ie);
                    }
                }
            }
            flushRequired.set(true);
        }
    }

    @Override
    public void flush() throws IOException {
        flushRequired.set(false);
        ledgerCache.flushLedger(true);
    }
}
