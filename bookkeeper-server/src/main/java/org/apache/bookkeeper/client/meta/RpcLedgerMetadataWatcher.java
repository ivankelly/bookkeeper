/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.client.meta;

import static org.apache.bookkeeper.client.utils.RpcUtils.DEFAULT_BACKOFF_START_MS;
import static org.apache.bookkeeper.client.utils.RpcUtils.readLedgerMetadataRequest;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import io.grpc.stub.StreamObserver;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.DataFormats;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataRequest;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataResponse;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataServiceGrpc.LedgerMetadataServiceStub;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version.Occurred;

/**
 * A watcher that watches the changes of the ledger metadata for a given ledger.
 */
@Slf4j
class RpcLedgerMetadataWatcher implements AutoCloseable {

    private final long ledgerId;
    private final LedgerMetadataServiceStub lmService;
    private final Set<LedgerMetadataListener> listeners;
    private final OrderedScheduler scheduler;

    // close state
    private boolean closed = false;

    // cache the latest metadata
    private LedgerMetadata metadata;
    private StreamObserver<LedgerMetadataRequest> watcher;

    RpcLedgerMetadataWatcher(long ledgerId,
                             LedgerMetadataServiceStub lmService,
                             OrderedScheduler scheduler) {
        this.ledgerId = ledgerId;
        this.lmService = lmService;
        this.listeners = Sets.newHashSet();
        this.scheduler = scheduler;
        this.metadata = null;
    }

    public synchronized boolean hasListeners() {
        return !listeners.isEmpty();
    }

    public synchronized RpcLedgerMetadataWatcher addListener(LedgerMetadataListener listener) {
        listeners.add(listener);
        if (null != metadata) {
            for (LedgerMetadataListener l : listeners) {
                l.onChanged( ledgerId, metadata );
            }
        } else if (listeners.size() == 1) { // the first listener will trigger the watch operation
            watch();
        }
        return this;
    }

    public synchronized RpcLedgerMetadataWatcher removeListener(LedgerMetadataListener listener) {
        listeners.remove(listener);
        return this;
    }

    public synchronized void watch() {
        if (null != this.watcher) {
            // end the watcher
            this.watcher.onCompleted();
            this.watcher = null;
        }
        this.watcher = this.lmService.watchOne(
            new StreamObserver<LedgerMetadataResponse>() {
                @Override
                public void onNext(LedgerMetadataResponse response) {
                    try {
                        LedgerMetadata newMetadata = null;
                        if (response.getMetadata() != DataFormats.LedgerMetadataFormat.getDefaultInstance()) {
                            newMetadata = LedgerMetadata.fromLedgerMetadataFormat(
                              response.getMetadata(),
                              new LongVersion(response.getVersion()),
                              Optional.absent());
                        }
                        updateLedgerMetadata(
                          response.getLedgerId(),
                          newMetadata);
                    } catch (Exception ioe) {
                        // the ledger metadata response is bad
                        log.error("Received bad ledger metadata response from watching ledger {} : {}",
                            ledgerId, response, ioe);
                        scheduleWatch();
                    }
                }

                @Override
                public void onError(Throwable cause) {
                    scheduleWatch();
                }

                @Override
                public void onCompleted() {
                    scheduleWatch();
                }
            });
        this.watcher.onNext(readLedgerMetadataRequest(ledgerId));
    }

    public synchronized void scheduleWatch() {
        if (closed) {
            return;
        }
        if (null != this.watcher) {
            // end the watcher
            this.watcher.onCompleted();
            this.watcher = null;
        }
        this.scheduler.schedule(
            () -> watch(),
            DEFAULT_BACKOFF_START_MS,
            TimeUnit.MILLISECONDS);
    }


    void updateLedgerMetadata(long lid, LedgerMetadata newMetadata) {
        if (log.isDebugEnabled()) {
            log.debug("Received ledger metadata update on {} : {}",
              lid, newMetadata == null ? "null": newMetadata.toString());
        }
        if (this.ledgerId != lid) {
            return;
        }
        Occurred occurred;
        synchronized (this) {
            if (newMetadata != null && metadata != null) {
                occurred = this.metadata.getVersion().compare(newMetadata.getVersion());
            } else {
                occurred = Occurred.AFTER;
            }
            // listener added || metadata deleted || metadata updated
            if (this.metadata == null || newMetadata == null || Occurred.BEFORE == occurred) {
                this.metadata = newMetadata;
                for (LedgerMetadataListener listener : listeners) {
                    listener.onChanged(lid, newMetadata);
                }
            }
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (null != watcher) {
            watcher.onCompleted();
            watcher = null;
        }
    }
}
