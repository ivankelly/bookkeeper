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
package org.apache.bookkeeper.proto;

import static org.apache.bookkeeper.util.SafeRunnable.safeRun;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.EnumSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ForceLedgerCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetBookieInfoCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadLacCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteLacCallback;
import org.apache.bookkeeper.util.ByteBufList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockBookieClient implements BookieClient {
    static final Logger LOG = LoggerFactory.getLogger(MockBookieClient.class);

    final OrderedExecutor executor;
    final ConcurrentHashMap<BookieSocketAddress, ConcurrentHashMap<Long, LedgerData>> data = new ConcurrentHashMap<>();
    final Set<BookieSocketAddress> errorBookies =
        Collections.newSetFromMap(new ConcurrentHashMap<BookieSocketAddress, Boolean>());

    final ConcurrentHashMap<BookieSocketAddress, CompletableFuture<Void>> stallPromises = new ConcurrentHashMap<>();

    public MockBookieClient(OrderedExecutor executor) {
        this.executor = executor;
    }

    public CompletableFuture<Void> stallBookie(BookieSocketAddress bookie) {
        CompletableFuture<Void> promise = new CompletableFuture<Void>();
        promise.whenComplete((res, ex) -> stallPromises.remove(bookie, promise));
        stallPromises.put(bookie, promise);
        return promise;
    }

    public void errorBookies(BookieSocketAddress... bookies) {
        for (BookieSocketAddress b : bookies) {
            errorBookies.add(b);
        }
    }

    public void removeErrors(BookieSocketAddress... bookies) {
        for (BookieSocketAddress b : bookies) {
            errorBookies.remove(b);
        }
    }

    @Override
    public List<BookieSocketAddress> getFaultyBookies() {
        return Collections.emptyList();
    }

    @Override
    public boolean isWritable(BookieSocketAddress address, long key) {
        return true;
    }

    @Override
    public PerChannelBookieClientPool lookupClient(BookieSocketAddress addr) {
        return null;
    }

    @Override
    public void forceLedger(BookieSocketAddress addr, long ledgerId,
                            ForceLedgerCallback cb, Object ctx) {
        executor.executeOrdered(ledgerId,
                safeRun(() -> {
                        cb.forceLedgerComplete(BKException.Code.IllegalOpException,
                                               ledgerId, addr, ctx);
                    }));
    }

    @Override
    public void writeLac(BookieSocketAddress addr, long ledgerId, byte[] masterKey,
                         long lac, ByteBufList toSend, WriteLacCallback cb, Object ctx) {
        executor.executeOrdered(ledgerId,
                safeRun(() -> {
                        cb.writeLacComplete(BKException.Code.IllegalOpException,
                                               ledgerId, addr, ctx);
                    }));
    }

    @Override
    public void addEntry(BookieSocketAddress addr, long ledgerId, byte[] masterKey,
                         long entryId, ByteBufList toSend, WriteCallback cb, Object ctx,
                         int options, boolean allowFastFail, EnumSet<WriteFlag> writeFlags) {

        CompletableFuture<Void> stallPromise = stallPromises.get(addr);
        if (stallPromise != null) {
            toSend.retain();
            LOG.info("[{};{}] Stalling write {}", addr, ledgerId, entryId);
            stallPromise.whenComplete((res, ex) -> {
                    stallPromises.remove(addr, stallPromise);
                    LOG.info("[{};{}] Unstalled write {}", addr, ledgerId, entryId);
                    addEntry(addr, ledgerId, masterKey, entryId, toSend,
                             cb, ctx, options, allowFastFail, writeFlags);
                    toSend.release();
                });
            return;
        }
        toSend.retain();
        executor.executeOrdered(ledgerId,
                safeRun(() -> {
                        LOG.info("[{};L{}] write entry {}", addr, ledgerId, entryId);
                        if (errorBookies.contains(addr)) {
                            LOG.warn("[{};L{}] erroring write {}", addr, ledgerId, entryId);
                            cb.writeComplete(BKException.Code.WriteException, ledgerId, entryId, addr, ctx);
                            return;
                        }
                        LedgerData ledger = getBookieData(addr).computeIfAbsent(ledgerId, LedgerData::new);
                        ledger.addEntry(entryId, copyData(toSend));
                        cb.writeComplete(BKException.Code.OK, ledgerId, entryId, addr, ctx);
                        toSend.release();
                    }));
    }

    @Override
    public void readLac(BookieSocketAddress addr, long ledgerId, ReadLacCallback cb, Object ctx) {
        executor.executeOrdered(ledgerId,
                safeRun(() -> {
                        cb.readLacComplete(BKException.Code.IllegalOpException,
                                           ledgerId, null, null, ctx);
                    }));
    }

    @Override
    public void readEntry(BookieSocketAddress addr, long ledgerId, long entryId,
                          ReadEntryCallback cb, Object ctx, int flags, byte[] masterKey,
                          boolean allowFastFail) {
        executor.executeOrdered(ledgerId,
                safeRun(() -> {
                        LOG.info("[{};L{}] read entry {}", addr, ledgerId, entryId);
                        if (errorBookies.contains(addr)) {
                            LOG.warn("[{};L{}] erroring read {}", addr, ledgerId, entryId);
                            cb.readEntryComplete(BKException.Code.ReadException, ledgerId, entryId, null, ctx);
                            return;
                        }

                        LedgerData ledger = getBookieData(addr).get(ledgerId);
                        if (ledger == null) {
                            LOG.warn("[{};L{}] ledger not found", addr, ledgerId);
                            cb.readEntryComplete(BKException.Code.NoSuchLedgerExistsException,
                                                 ledgerId, entryId, null, ctx);
                            return;
                        }

                        ByteBuf entry = ledger.getEntry(entryId);
                        if (entry == null) {
                            LOG.warn("[{};L{}] entry({}) not found", addr, ledgerId, entryId);
                            cb.readEntryComplete(BKException.Code.NoSuchEntryException,
                                                 ledgerId, entryId, null, ctx);
                            return;
                        }

                        cb.readEntryComplete(BKException.Code.OK,
                                             ledgerId, entryId, entry.slice(), ctx);
                    }));
    }

    @Override
    public void readEntryWaitForLACUpdate(BookieSocketAddress addr,
                                          long ledgerId,
                                          long entryId,
                                          long previousLAC,
                                          long timeOutInMillis,
                                          boolean piggyBackEntry,
                                          ReadEntryCallback cb,
                                          Object ctx) {
        executor.executeOrdered(ledgerId,
                safeRun(() -> {
                        cb.readEntryComplete(BKException.Code.IllegalOpException,
                                             ledgerId, entryId, null, ctx);
                    }));
    }

    @Override
    public void getBookieInfo(BookieSocketAddress addr, long requested,
                              GetBookieInfoCallback cb, Object ctx) {
        executor.executeOrdered(addr,
                safeRun(() -> {
                        cb.getBookieInfoComplete(BKException.Code.IllegalOpException,
                                                 null, ctx);
                    }));
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() {
    }

    ConcurrentHashMap<Long, LedgerData> getBookieData(BookieSocketAddress addr) {
        return data.computeIfAbsent(addr, (key) -> new ConcurrentHashMap<>());
    }

    static ByteBuf copyData(ByteBufList list) {
        ByteBuf buf = Unpooled.buffer(list.readableBytes());
        for (int i = 0; i < list.size(); i++) {
            buf.writeBytes(list.getBuffer(i).slice());
        }
        return buf;
    }

    static class LedgerData {
        final long ledgerId;
        private TreeMap<Long, ByteBuf> entries = new TreeMap<>();
        LedgerData(long ledgerId) {
            this.ledgerId = ledgerId;
        }

        void addEntry(long entryId, ByteBuf entry) {
            entries.put(entryId, entry);
        }

        ByteBuf getEntry(long entryId) {
            if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
                Map.Entry<Long, ByteBuf> lastEntry = entries.lastEntry();
                if (lastEntry != null) {
                    return lastEntry.getValue();
                } else {
                    return null;
                }
            } else {
                return entries.get(entryId);
            }
        }
    }
}
