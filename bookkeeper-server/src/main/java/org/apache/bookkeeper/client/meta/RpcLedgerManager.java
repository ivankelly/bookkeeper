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

import static org.apache.bookkeeper.client.utils.RpcUtils.createLedgerMetadataRequest;
import static org.apache.bookkeeper.client.utils.RpcUtils.getLedgerRangesRequest;
import static org.apache.bookkeeper.client.utils.RpcUtils.readLedgerMetadataRequest;
import static org.apache.bookkeeper.client.utils.RpcUtils.removeLedgerMetadataRequest;
import static org.apache.bookkeeper.client.utils.RpcUtils.writeLedgerMetadataRequest;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.client.utils.RpcUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SharedResourceManager;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.proto.rpc.common.StatusCode;
import org.apache.bookkeeper.proto.rpc.metadata.GetLedgerRangesRequest;
import org.apache.bookkeeper.proto.rpc.metadata.GetLedgerRangesResponse;
import org.apache.bookkeeper.proto.rpc.metadata.GetLedgerRangesResponse.LedgerRangeFormat;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataRequest;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataResponse;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataServiceGrpc.LedgerMetadataServiceFutureStub;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataServiceGrpc.LedgerMetadataServiceStub;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.zookeeper.AsyncCallback.VoidCallback;

/**
 * A RPC based {@link LedgerManager}.
 */
@Slf4j
public class RpcLedgerManager implements LedgerManager {

    private static final int DEFAULT_MAX_NUM_RETRIES = 3;

    private final LedgerMetadataServiceStub lmService;
    private final LedgerMetadataServiceFutureStub lmFutureService;
    private final Resource<OrderedScheduler> schedulerResource;
    private final OrderedScheduler scheduler;
    private final int maxNumRetries;

    // listeners
    private final Map<Long, RpcLedgerMetadataWatcher> watchers = Maps.newHashMap();

    RpcLedgerManager(LedgerMetadataServiceStub lmService,
                     LedgerMetadataServiceFutureStub lmFutureService,
                     Resource<OrderedScheduler> schedulerResource) {
        this.lmService = lmService;
        this.lmFutureService = lmFutureService;
        this.schedulerResource = schedulerResource;
        this.scheduler = SharedResourceManager.shared().get(schedulerResource);
        this.maxNumRetries = DEFAULT_MAX_NUM_RETRIES;
    }

    @Override
    public void createLedgerMetadata(long ledgerId, LedgerMetadata metadata, GenericCallback<Void> cb) {
        LedgerMetadataRequest request = createLedgerMetadataRequest(ledgerId, metadata);
        RpcUtils.retry(
            () -> lmFutureService.create(request),
            scheduler,
            maxNumRetries
        ).whenComplete((resp, cause) -> {
            if (null != cause) {
                cb.operationComplete(Code.MetaStoreException, null);
                return;
            }

            processCreateLedgerMetadataResponse(metadata, resp, cb);
        });
    }

    void processCreateLedgerMetadataResponse(LedgerMetadata metadata,
                                             LedgerMetadataResponse resp,
                                             GenericCallback<Void> cb) {
        switch (resp.getCode()) {
            case SUCCESS:
                LongVersion version = new LongVersion(resp.getVersion());
                metadata.setVersion(version);
                cb.operationComplete(Code.OK, null);
                return;
            case LEDGER_EXISTS:
                cb.operationComplete(Code.LedgerExistException, null);
                return;
            default:
                cb.operationComplete(Code.MetaStoreException, null);
                return;
        }
    }

    @Override
    public void removeLedgerMetadata(long ledgerId, Version version, GenericCallback<Void> cb) {
        LedgerMetadataRequest request = removeLedgerMetadataRequest(ledgerId, version);
        RpcUtils.retry(
            () -> lmFutureService.remove(request),
            scheduler,
            maxNumRetries
        ).whenComplete((resp, cause) -> {
            if (null != cause) {
                cb.operationComplete(Code.MetaStoreException, null);
                return;
            }

            processRemoveLedgerMetadataResponse(resp, cb);
        });
    }

    void processRemoveLedgerMetadataResponse(LedgerMetadataResponse resp,
                                             GenericCallback<Void> cb) {
        switch (resp.getCode()) {
            case SUCCESS:
                cb.operationComplete(Code.OK, null);
                return;
            case LEDGER_NOT_FOUND:
                cb.operationComplete(Code.NoSuchLedgerExistsException, null);
                return;
            default:
                cb.operationComplete(Code.MetaStoreException, null);
                return;
        }
    }

    @Override
    public void readLedgerMetadata(long ledgerId, GenericCallback<LedgerMetadata> cb) {
        LedgerMetadataRequest request = readLedgerMetadataRequest(ledgerId);
        RpcUtils.retry(
            () -> lmFutureService.read(request),
            scheduler,
            maxNumRetries
        ).whenComplete((resp, cause) -> {
            if (null != cause) {
                cb.operationComplete(Code.MetaStoreException, null);
                return;
            }

            processReadLedgerMetadataResponse(resp, cb);
        });
    }

    void processReadLedgerMetadataResponse(LedgerMetadataResponse resp,
                                           GenericCallback<LedgerMetadata> cb) {
        switch (resp.getCode()) {
            case SUCCESS:
                LedgerMetadata metadata;
                try {
                    metadata = LedgerMetadata.fromLedgerMetadataFormat(
                        resp.getMetadata(),
                        new LongVersion(resp.getVersion()),
                        Optional.absent());
                    cb.operationComplete(Code.OK, metadata);
                } catch (IOException e) {
                    log.error("Received an invalid ledger metadata from response : {}", resp);
                    cb.operationComplete(Code.MetaStoreException, null);
                }
                return;
            case LEDGER_NOT_FOUND:
                cb.operationComplete(Code.NoSuchLedgerExistsException, null);
                return;
            default:
                cb.operationComplete(Code.MetaStoreException, null);
                return;
        }
    }

    @Override
    public void writeLedgerMetadata(long ledgerId, LedgerMetadata metadata, GenericCallback<Void> cb) {
        LedgerMetadataRequest request = writeLedgerMetadataRequest(ledgerId, metadata);
        RpcUtils.retry(
            () -> lmFutureService.write(request),
            scheduler,
            maxNumRetries
        ).whenComplete((resp, cause) -> {
            if (null != cause) {
                cb.operationComplete(Code.MetaStoreException, null);
                return;
            }

            processWriteLedgerMetadataResponse(metadata, resp, cb);
        });
    }

    void processWriteLedgerMetadataResponse(LedgerMetadata metadata,
                                            LedgerMetadataResponse resp,
                                            GenericCallback<Void> cb) {
        switch (resp.getCode()) {
            case SUCCESS:
                metadata.setVersion(new LongVersion(resp.getVersion()));
                cb.operationComplete(Code.OK, null);
                return;
            case LEDGER_NOT_FOUND:
                cb.operationComplete(Code.NoSuchLedgerExistsException, null);
                return;
            default:
                cb.operationComplete(Code.MetaStoreException, null);
                return;
        }
    }

    @Override
    public void registerLedgerMetadataListener(long ledgerId,
                                               LedgerMetadataListener listener) {
        RpcLedgerMetadataWatcher watcher;
        synchronized (watchers) {
            watcher = watchers.get(ledgerId);
            if (null == watcher) {
                watcher = new RpcLedgerMetadataWatcher(
                    ledgerId,
                    lmService,
                    scheduler);
                watchers.put(ledgerId, watcher);
            }
        }
        watcher.addListener(listener);
    }

    @Override
    public void unregisterLedgerMetadataListener(long ledgerId,
                                                 LedgerMetadataListener listener) {
        RpcLedgerMetadataWatcher watcher;
        boolean closeWatcher = false;
        synchronized (watchers) {
            watcher = watchers.get(ledgerId);
            if (null == watcher) {
                return;
            }
            watcher.removeListener(listener);
            if (!watcher.hasListeners()) { // remove watcher if there is no listeners
                closeWatcher = watchers.remove(ledgerId, watcher);
            }
        }
        if (closeWatcher) {
            watcher.close();
        }
    }

    /**
     * AsyncProcessLedgers is most called by replicator and not by end-user client,
     * So not support asyncProcessLedgers currently
     */
    @Override
    public void asyncProcessLedgers(Processor<Long> processor,
                                    VoidCallback finalCb,
                                    Object context,
                                    int successRc,
                                    int failureRc) {
        throw new UnsupportedOperationException("Iterating ledgers is not supported by RPC yet.");
    }

    public static final int DEFAULT_GET_LEDGER_RANGES_LIMIT = 100;

    @Override
    public LedgerRangeIterator getLedgerRanges() {
        final List<LedgerRange> nextRanges = Collections.synchronizedList(new ArrayList<LedgerRange>());
        GetLedgerRangesRequest request = getLedgerRangesRequest(DEFAULT_GET_LEDGER_RANGES_LIMIT);
        StreamObserver<GetLedgerRangesResponse> observer = new StreamObserver<GetLedgerRangesResponse>() {
            LedgerRange nextRange;
            @Override
            public void onNext(GetLedgerRangesResponse response) {
                if (response.getCode() == StatusCode.SUCCESS) {
                    LedgerRangeFormat encodedBytes = response.getLedgerRange();
                    nextRange = new LedgerRange(new HashSet<>(encodedBytes.getLedgerIdsList()));
                    nextRanges.add(nextRange);
                } else {
                    log.error("response.getCode: {}", response.getCode());
                }
            }

            @Override
            public void onError(Throwable cause) {
                nextRange = null;
                log.error(cause.getMessage());
            }

            @Override
            public void onCompleted() {
                nextRange = null;
            }
        };

        lmService.iterate(request, observer);

        return new LedgerRangeIterator() {
            @Override
            synchronized public boolean hasNext() throws IOException {
                return nextRanges.size() > 0;
            }

            @Override
            synchronized public LedgerRange next() throws IOException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                // always return the first item in the range list.
                return nextRanges.remove(0);
            }
        };
    }

    @Override
    public void close() throws IOException {
        List<RpcLedgerMetadataWatcher> watchersToClose;
        synchronized (watchers) {
            watchersToClose = Lists.newArrayListWithExpectedSize(watchers.size());
            watchersToClose.addAll(watchers.values());
        }
        watchersToClose.forEach(RpcLedgerMetadataWatcher::close);
        // close the scheduler
        SharedResourceManager.shared().release(schedulerResource, scheduler);
    }
}
