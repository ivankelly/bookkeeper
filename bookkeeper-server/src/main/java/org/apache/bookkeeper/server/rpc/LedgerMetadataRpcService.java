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

package org.apache.bookkeeper.server.rpc;

import static org.apache.bookkeeper.util.BookKeeperConstants.INVALID_LEDGER_ID;
import static org.apache.bookkeeper.versioning.LongVersion.ANY_LONG_VALUE;
import static org.apache.bookkeeper.versioning.Version.ANY;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat;
import org.apache.bookkeeper.proto.rpc.common.StatusCode;
import org.apache.bookkeeper.proto.rpc.metadata.GetLedgerRangesRequest;
import org.apache.bookkeeper.proto.rpc.metadata.GetLedgerRangesResponse;
import org.apache.bookkeeper.proto.rpc.metadata.GetLedgerRangesResponse.LedgerRangeFormat;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerIdAllocateRequest;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerIdAllocateResponse;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataRequest;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataResponse;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataServiceGrpc.LedgerMetadataServiceImplBase;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;

/**
 * Grpc based ledger metadata service.
 */
@Slf4j
public class LedgerMetadataRpcService extends LedgerMetadataServiceImplBase {

    private final LedgerIdGenerator generator;
    private final LedgerManager lm;
    private final OrderedSafeExecutor scheduler;

    public LedgerMetadataRpcService(LedgerIdGenerator generator,
                                    LedgerManager lm,
                                    OrderedSafeExecutor scheduler) {
        this.generator = generator;
        this.lm = lm;
        this.scheduler = scheduler;
    }

    static void completeErrorResponse(long ledgerId,
                                      StatusCode code,
                                      StreamObserver<LedgerMetadataResponse> respObserver) {
        LedgerMetadataResponse resp = LedgerMetadataResponse.newBuilder()
            .setCode(code)
            .setLedgerId(ledgerId)
            .build();
        respObserver.onNext(resp);
        respObserver.onCompleted();
    }

    static void completeErrorAllocateResponse(long ledgerId,
                                              StatusCode code,
                                              StreamObserver<LedgerIdAllocateResponse> respObserver) {
        LedgerIdAllocateResponse resp = LedgerIdAllocateResponse.newBuilder()
            .setCode(code)
            .setLedgerId(ledgerId)
            .build();
        respObserver.onNext(resp);
        respObserver.onCompleted();
    }

    static void sendSuccessResponse(long ledgerId,
                                    LongVersion version,
                                    LedgerMetadataFormat format,
                                    StreamObserver<LedgerMetadataResponse> respObserver) {
        LedgerMetadataResponse.Builder resp = LedgerMetadataResponse.newBuilder()
            .setLedgerId(ledgerId)
            .setVersion(version.getLongVersion())
            .setCode(StatusCode.SUCCESS);
        if (format != null) {
            resp.setMetadata(format);
        }
        respObserver.onNext(resp.build());
    }

    static void sendSuccessAllocateResponse(long ledgerId,
                                            StreamObserver<LedgerIdAllocateResponse> respObserver) {
        LedgerIdAllocateResponse resp = LedgerIdAllocateResponse.newBuilder()
            .setLedgerId(ledgerId)
            .setCode(StatusCode.SUCCESS)
            .build();
        respObserver.onNext(resp);
        respObserver.onCompleted();
    }

    static void completeSuccessResponse(long ledgerId,
                                        LongVersion version,
                                        LedgerMetadataFormat format,
                                        StreamObserver<LedgerMetadataResponse> respObserver) {
        sendSuccessResponse(ledgerId, version, format, respObserver);
        respObserver.onCompleted();
    }

    @Override
    public void allocate(LedgerIdAllocateRequest request,
                         StreamObserver<LedgerIdAllocateResponse> responseObserver) {
        generator.generateLedgerId((rc, ledgerId) -> {
            if (Code.OK != rc) {
                // failed to generate ledger id
                completeErrorAllocateResponse(ledgerId, StatusCode.LEDGER_METADATA_ERROR, responseObserver);
            } else {
                sendSuccessAllocateResponse(ledgerId, responseObserver);
            }
        });
    }

    @Override
    public void create(LedgerMetadataRequest request,
                       StreamObserver<LedgerMetadataResponse> responseObserver) {
        if (request.getLedgerId() <= 0L) {
            generator.generateLedgerId((rc, ledgerId) -> {
                if (Code.OK != rc) {
                    // failed to generate ledger id
                    completeErrorResponse(ledgerId, StatusCode.LEDGER_METADATA_ERROR, responseObserver);
                } else {
                    create(ledgerId, request.getMetadata(), responseObserver);
                }
            });
        } else {
            create(request.getLedgerId(), request.getMetadata(), responseObserver);
        }
    }

    public void create(long ledgerId,
                       LedgerMetadataFormat format,
                       StreamObserver<LedgerMetadataResponse> respObserver) {
        LedgerMetadata metadata;
        try {
            metadata = LedgerMetadata.fromLedgerMetadataFormat(
                format,
                Version.NEW,
                Optional.absent());
        } catch (IOException e) {
            // invalid ledger format
            completeErrorResponse(
                ledgerId,
                StatusCode.BAD_REQUEST,
                respObserver);
            return;
        }
        lm.createLedgerMetadata(ledgerId, metadata, (rc, result) -> {
            if (Code.LedgerExistException == rc) {
                completeErrorResponse(
                    ledgerId,
                    StatusCode.LEDGER_EXISTS,
                    respObserver);
            } else if (Code.OK != rc) {
                completeErrorResponse(
                    ledgerId,
                    StatusCode.LEDGER_METADATA_ERROR,
                    respObserver);
            } else {
                // the version is set in `metadata`
                completeSuccessResponse(
                    ledgerId,
                    (LongVersion) metadata.getVersion(),
                    format,
                    respObserver);
            }
        });
    }

    @Override
    public void remove(LedgerMetadataRequest request,
                       StreamObserver<LedgerMetadataResponse> responseObserver) {
        Version version;
        long versionValue = request.getExpectedVersion();
        if (versionValue == ANY_LONG_VALUE) {
            version = ANY;
        } else {
            version = new LongVersion(versionValue);
        }

        lm.removeLedgerMetadata(request.getLedgerId(), version, (rc, value) -> {
            if (Code.NoSuchLedgerExistsException == rc) {
                completeErrorResponse(
                    request.getLedgerId(),
                    StatusCode.LEDGER_NOT_FOUND,
                    responseObserver);
            } else if (Code.OK != rc) {
                completeErrorResponse(
                    request.getLedgerId(),
                    StatusCode.LEDGER_METADATA_ERROR,
                    responseObserver);
            } else {
                completeSuccessResponse(
                    request.getLedgerId(),
                    new LongVersion(-1L),
                    null,
                    responseObserver);
            }
        });
    }

    @Override
    public void read(LedgerMetadataRequest request,
                     StreamObserver<LedgerMetadataResponse> responseObserver) {
        lm.readLedgerMetadata(request.getLedgerId(), (rc, metadata) -> {
            if (Code.NoSuchLedgerExistsException == rc) {
                completeErrorResponse(
                    request.getLedgerId(),
                    StatusCode.LEDGER_NOT_FOUND,
                    responseObserver);
            } else if (Code.OK != rc) {
                completeErrorResponse(
                    request.getLedgerId(),
                    StatusCode.LEDGER_METADATA_ERROR,
                    responseObserver);
            } else {
                completeSuccessResponse(
                    request.getLedgerId(),
                    (LongVersion) metadata.getVersion(),
                    metadata.toProtoFormat(),
                    responseObserver);
            }
        });
    }

    @Override
    public void write(LedgerMetadataRequest request,
                      StreamObserver<LedgerMetadataResponse> responseObserver) {
        long ledgerId = request.getLedgerId();
        LedgerMetadata metadata;
        try {
            metadata = LedgerMetadata.fromLedgerMetadataFormat(
                request.getMetadata(),
                new LongVersion(request.getExpectedVersion()),
                Optional.absent());
        } catch (IOException e) {
            // invalid ledger format
            completeErrorResponse(
                ledgerId,
                StatusCode.BAD_REQUEST,
                responseObserver);
            return;
        }
        lm.writeLedgerMetadata(request.getLedgerId(), metadata, (rc, value) -> {
            if (Code.NoSuchLedgerExistsException == rc) {
                completeErrorResponse(
                    ledgerId,
                    StatusCode.LEDGER_NOT_FOUND,
                    responseObserver);
            } else if (Code.MetadataVersionException == rc) {
                completeErrorResponse(
                    ledgerId,
                    StatusCode.BAD_VERSION,
                    responseObserver);
            } else if (Code.OK != rc) {
                completeErrorResponse(
                    ledgerId,
                    StatusCode.LEDGER_METADATA_ERROR,
                    responseObserver);
            } else {
                completeSuccessResponse(
                    ledgerId,
                    (LongVersion) metadata.getVersion(),
                    null,
                    responseObserver);
            }
        });
    }

    static void sendGetLedgerRangesResponse(List<Long> buf,
                                            StreamObserver<GetLedgerRangesResponse> respObserver) {
        GetLedgerRangesResponse resp = GetLedgerRangesResponse.newBuilder()
            .setCode(StatusCode.SUCCESS)
            .setLedgerRange(LedgerRangeFormat.newBuilder().addAllLedgerIds(buf).build())
            .build();
        respObserver.onNext(resp);
    }

    static void sendGetLedgerRangesErrorResponse(StreamObserver<GetLedgerRangesResponse> respObserver) {
        GetLedgerRangesResponse resp = GetLedgerRangesResponse.newBuilder()
            .setCode(StatusCode.LEDGER_METADATA_ERROR)
            .build();
        respObserver.onNext(resp);
    }

    @Override
    public void iterate(GetLedgerRangesRequest request,
                        StreamObserver<GetLedgerRangesResponse> responseObserver) {
        int limit = Math.min(512, request.getLimitPerResponse());

        AtomicReference<List> bufRef = new AtomicReference<>();
        bufRef.set(Lists.newArrayListWithExpectedSize(limit));
        lm.asyncProcessLedgers(
            (ledgerId, iterCb) -> {
                List<Long> buf = bufRef.get();
                buf.add(ledgerId);
                if (buf.size() == limit) {
                    sendGetLedgerRangesResponse(buf, responseObserver);
                    bufRef.set(Lists.newArrayListWithExpectedSize(limit));
                }
                iterCb.processResult(Code.OK, null, null);
            },
            (rc, path, ctx) -> {
                if (Code.OK == rc) {
                    List<Long> buf = bufRef.get();
                    sendGetLedgerRangesResponse(buf, responseObserver);
                } else {
                    sendGetLedgerRangesErrorResponse(responseObserver);
                }
                responseObserver.onCompleted();
            },
            null,
            Code.OK,
            Code.MetaStoreException);
    }

    @Override
    public StreamObserver<LedgerMetadataRequest> watchOne(StreamObserver<LedgerMetadataResponse> responseObserver) {
        LedgerMetadataWatcher watcher = new LedgerMetadataWatcher(responseObserver, lm, scheduler);
        return watcher;
    }

    static class LedgerMetadataWatcher
            implements StreamObserver<LedgerMetadataRequest>, LedgerMetadataListener {

        private final StreamObserver<LedgerMetadataResponse> responseObserver;
        private final LedgerManager underlying;
        private final OrderedSafeExecutor scheduler;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private long lid = INVALID_LEDGER_ID;
        private long expectedVersion;

        LedgerMetadataWatcher(StreamObserver<LedgerMetadataResponse> responseObserver,
                              LedgerManager underlying,
                              OrderedSafeExecutor scheduler) {
            this.responseObserver = responseObserver;
            this.underlying = underlying;
            this.scheduler = scheduler;
        }

        private void unsafeFailSession(Status status) {
            if (closed.compareAndSet(false, true)) {
                responseObserver.onError(new StatusRuntimeException(status));
            }
        }

        //
        // Request stream
        //

        @Override
        public void onNext(LedgerMetadataRequest request) {
            this.scheduler.submitOrdered(responseObserver, new SafeRunnable() {
                @Override
                public void safeRun() {
                    unsafeProcessLedgerMetadataRequest(request);
                }
            });
        }

        private void unsafeProcessLedgerMetadataRequest(LedgerMetadataRequest request) {
            if (INVALID_LEDGER_ID != lid) {
                // unexpected situation: two watch requests from same session
                unsafeUnwatchLedger();
                unsafeFailSession(Status.ALREADY_EXISTS);
                return;
            }

            if (request.getLedgerId() <= 0) {
                // the request is bad
                unsafeFailSession(Status.INVALID_ARGUMENT);
                return;
            }

            if (closed.get()) {
                // the request is cancelled due to the watcher is already closed
                unsafeFailSession(Status.CANCELLED);
                return;
            }

            this.lid = request.getLedgerId();
            this.expectedVersion = request.getExpectedVersion();

            // register metadata listener
            underlying.registerLedgerMetadataListener(lid, this);
        }

        @Override
        public void onError(Throwable throwable) {
            // the connection is broken
            unwatchLedger();
        }

        @Override
        public void onCompleted() {
            // the connection is completed
            unwatchLedger();
        }

        private void unwatchLedger() {
            scheduler.submitOrdered(responseObserver, new SafeRunnable() {
                @Override
                public void safeRun() {
                    unsafeUnwatchLedger();
                }
            });
        }

        private void unsafeUnwatchLedger() {
            if (INVALID_LEDGER_ID != lid) {
                underlying.unregisterLedgerMetadataListener(lid, LedgerMetadataWatcher.this);
            }
        }

        //
        // ledger metadata listener
        //

        // when changed will readmetadata and return metadata, while delete, will return null
        @Override
        public void onChanged(long ledgerId, LedgerMetadata metadata) {
            scheduler.submitOrdered(responseObserver, new SafeRunnable() {
                @Override
                public void safeRun() {
                    unsafeProcessLedgerMetadata(metadata);
               }
            });
        }

        private void unsafeProcessLedgerMetadata(LedgerMetadata metadata) {
            LongVersion version = (metadata != null) ? (LongVersion) metadata.getVersion() : new LongVersion(-1L);
            long longVersion = version.getLongVersion();

            if (metadata == null || longVersion >= expectedVersion) {
                // send the updated metadata to client
                sendSuccessResponse(
                    lid,
                    version,
                    metadata != null ? metadata.toProtoFormat() : null,
                    responseObserver);
                this.expectedVersion = longVersion;
            } else {
                // we received a smaller version of ledger metadata
                // that means that the bookie connect to a lagging metadata storage node
                unsafeFailSession(Status.CANCELLED);
            }
        }
    }
}
