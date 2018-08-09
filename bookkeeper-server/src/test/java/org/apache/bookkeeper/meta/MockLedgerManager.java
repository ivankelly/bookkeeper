/**
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
package org.apache.bookkeeper.meta;

import com.google.common.base.Optional;

import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.MockLedgerManager;

import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;

import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.AsyncCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockLedgerManager implements LedgerManager {
    static final Logger LOG = LoggerFactory.getLogger(MockLedgerManager.class);

    final AtomicReference<CompletableFuture<Void>> stallPromise
        = new AtomicReference<>(CompletableFuture.completedFuture(null));
    final Map<Long, Pair<LongVersion, byte[]>> metadataMap;
    final ExecutorService executor;
    final boolean ownsExecutor;

    public MockLedgerManager() {
        this(new HashMap<>(),
             Executors.newSingleThreadExecutor((r) -> new Thread(r, "MockLedgerManager")), true);
    }

    private MockLedgerManager(Map<Long, Pair<LongVersion, byte[]>> metadataMap,
                              ExecutorService executor, boolean ownsExecutor) {
        this.metadataMap = metadataMap;
        this.executor = executor;
        this.ownsExecutor = ownsExecutor;
    }

    public MockLedgerManager newClient() {
        return new MockLedgerManager(metadataMap, executor, false);
    }

    private LedgerMetadata readMetadata(long ledgerId) throws Exception {
        Pair<LongVersion, byte[]> pair = metadataMap.get(ledgerId);
        if (pair == null) {
            return null;
        } else {
            return LedgerMetadata.parseConfig(pair.getRight(), pair.getLeft(), Optional.absent());
        }
    }

    public CompletableFuture<Void> stallWrites() throws Exception {
        CompletableFuture<Void> newPromise = new CompletableFuture<>();
        CompletableFuture<Void> currentPromise = stallPromise.get();
        while (currentPromise.isDone()) {
            if (stallPromise.compareAndSet(currentPromise, newPromise)) {
                return newPromise;
            } else {
                currentPromise = stallPromise.get();
            }
        }
        return currentPromise;
    }

    public void executeCallback(Runnable r) {
        r.run();
    }

    @Override
    public void createLedgerMetadata(long ledgerId, LedgerMetadata metadata, GenericCallback<LedgerMetadata> cb) {
        executor.submit(() -> {
                if (metadataMap.containsKey(ledgerId)) {
                    executeCallback(() -> cb.operationComplete(BKException.Code.LedgerExistException, null));
                } else {
                    metadataMap.put(ledgerId, Pair.of(new LongVersion(0L), metadata.serialize()));
                    try {
                        LedgerMetadata readBack = readMetadata(ledgerId);
                        executeCallback(() -> cb.operationComplete(BKException.Code.OK, readBack));
                    } catch (Exception e) {
                        LOG.error("Error reading back written metadata", e);
                        executeCallback(() -> cb.operationComplete(BKException.Code.MetaStoreException, null));
                    }
                }
            });
    }

    @Override
    public void removeLedgerMetadata(long ledgerId, Version version, GenericCallback<Void> cb) {}

    @Override
    public void readLedgerMetadata(long ledgerId, GenericCallback<LedgerMetadata> cb) {
        executor.submit(() -> {
                try {
                    LedgerMetadata metadata = readMetadata(ledgerId);
                    if (metadata == null) {
                        executeCallback(
                                        () -> cb.operationComplete(BKException.Code.NoSuchLedgerExistsException, null));
                    } else {
                        executeCallback(() -> cb.operationComplete(BKException.Code.OK, metadata));
                    }
                } catch (Exception e) {
                    LOG.error("Error reading metadata", e);
                    executeCallback(() -> cb.operationComplete(BKException.Code.MetaStoreException, null));
                }
            });
    }

    @Override
    public void writeLedgerMetadata(long ledgerId, LedgerMetadata metadata, GenericCallback<LedgerMetadata> cb) {
        LOG.info("Writing {}", ledgerId);
        CompletableFuture<Void> promise = stallPromise.get();
        if (!promise.isDone()) {
            LOG.info("[L{}, stallId={}] Stalling write of metadata", ledgerId, System.identityHashCode(promise));

            promise.whenCompleteAsync((res, ex) -> {
                    LOG.info("[L{}, stallid={}] Unstalled write, ex = {}",
                             ledgerId, System.identityHashCode(promise), ex);
                    if (ex != null) {
                        executeCallback(() -> cb.operationComplete(BKException.Code.MetaStoreException, null));
                    } else {
                        writeLedgerMetadata(ledgerId, metadata, cb);
                    }
                }, executor);
            return;
        }
        executor.submit(() -> {
                try {
                    LedgerMetadata oldMetadata = readMetadata(ledgerId);
                    if (oldMetadata == null) {
                        executeCallback(
                                        () -> cb.operationComplete(BKException.Code.NoSuchLedgerExistsException, null));
                    } else if (!oldMetadata.getVersion().equals(metadata.getVersion())) {
                        executeCallback(
                                        () -> cb.operationComplete(BKException.Code.MetadataVersionException, null));
                    } else {
                        LongVersion oldVersion = (LongVersion) oldMetadata.getVersion();
                        metadataMap.put(ledgerId, Pair.of(new LongVersion(oldVersion.getLongVersion() + 1),
                                                          metadata.serialize()));
                        LedgerMetadata readBack = readMetadata(ledgerId);
                        executeCallback(() -> cb.operationComplete(BKException.Code.OK, readBack));
                    }
                } catch (Exception e) {
                    LOG.error("Error writing metadata", e);
                    executeCallback(
                                    () -> cb.operationComplete(BKException.Code.MetaStoreException, null));
                }
            });

    }

    @Override
    public void registerLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {}

    @Override
    public void unregisterLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {}

    @Override
    public void asyncProcessLedgers(Processor<Long> processor, AsyncCallback.VoidCallback finalCb,
                                    Object context, int successRc, int failureRc) {
    }

    @Override
    public LedgerRangeIterator getLedgerRanges() {
        return null;
    }

    @Override
    public void close() {
        if (ownsExecutor) {
            executor.shutdownNow();
        }
    }

}
