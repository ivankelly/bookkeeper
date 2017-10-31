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
package org.apache.bookkeeper.client.meta;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.util.MutableHandlerRegistry;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.client.utils.RpcUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRange;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRangeIterator;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.rpc.common.StatusCode;
import org.apache.bookkeeper.proto.rpc.metadata.GetLedgerRangesResponse;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataResponse;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataServiceGrpc;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataServiceGrpc.LedgerMetadataServiceFutureStub;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataServiceGrpc.LedgerMetadataServiceImplBase;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataServiceGrpc.LedgerMetadataServiceStub;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRpcLedgerManager {
    private final static Logger LOG = LoggerFactory.getLogger(TestRpcLedgerManager.class);

    private final String serverName = "fake server for " + getClass();
    private MutableHandlerRegistry serviceRegistry;
    private Server fakeServer;
    private RpcLedgerManager rpcLedgerManager;
    private ManagedChannel channel;
    LedgerMetadataServiceStub lmService;
    LedgerMetadataServiceFutureStub lmFutureService;
    Resource<OrderedScheduler> schedulerResource;

    LedgerMetadata createLedgerMetadata() {
        // set version to 1s ago
        long version = System.currentTimeMillis() - 1000;
        Map<String, byte[]> customMetadata = Maps.newHashMap();
        customMetadata.put("test_key", "test_value".getBytes(UTF_8));
        LedgerMetadata metadata = new LedgerMetadata(5,
          3,
          2,
          BookKeeper.DigestType.CRC32,
          "password".getBytes(UTF_8),
          customMetadata);
        metadata.setVersion(new LongVersion(version));

        return metadata;
    }

    @Before
    public void setUp() throws Exception {
        serviceRegistry = new MutableHandlerRegistry();
        fakeServer = InProcessServerBuilder
          .forName(serverName)
          .fallbackHandlerRegistry(serviceRegistry)
          .directExecutor()
          .build()
          .start();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        Optional<String> token =  Optional.empty();

        lmService = RpcUtils.configureRpcStub(
            LedgerMetadataServiceGrpc.newStub(channel),
            token);
        lmFutureService = RpcUtils.configureRpcStub(
            LedgerMetadataServiceGrpc.newFutureStub(channel),
            token);
        schedulerResource = new Resource<OrderedScheduler>() {
            private static final String name = "test-rpc-scheduler";

            @Override
            public OrderedScheduler create() {
                return OrderedScheduler.newSchedulerBuilder()
                  .numThreads(Runtime.getRuntime().availableProcessors() * 2)
                  .name(name)
                  .build();
            }

            @Override
            public void close(OrderedScheduler instance) {
                instance.shutdown();
            }

            @Override
            public String toString() {
                return name;
            }
        };

        rpcLedgerManager = new RpcLedgerManager(lmService, lmFutureService, schedulerResource);
    }

    @After
    public void tearDown() throws Exception {
        if (null != rpcLedgerManager) {
            rpcLedgerManager.close();
        }
        if (null != fakeServer) {
            fakeServer.shutdown();
        }
    }

    /**
     * Test createLedgerMetadata SUCCESS, expect rc == OK and metadata.version set
     */
    @Test
    public void testCreateLedgerMetadataSuccess() throws Exception {
        long ledgerId = System.currentTimeMillis();
        long version = System.currentTimeMillis();
        LedgerMetadata metadata = createLedgerMetadata();
        List<Integer> resultCode = new ArrayList<Integer>(1);
        GenericCallback<Void> callback = new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                resultCode.add(rc);
            }
        };

        LedgerMetadataResponse response = LedgerMetadataResponse.newBuilder()
          .setCode(StatusCode.SUCCESS)
          .setVersion(version)
          .build();

        LedgerMetadataServiceImplBase ledgerMetadataService = new LedgerMetadataServiceImplBase() {
            @Override
            public void create(org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataRequest request,
                               StreamObserver<LedgerMetadataResponse> responseObserver) {
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };

        serviceRegistry.addService(ledgerMetadataService.bindService());

        rpcLedgerManager.createLedgerMetadata(ledgerId, metadata, callback);
        Thread.sleep(100);

        // verify rc returned in callback
        assertEquals(Integer.valueOf(BKException.Code.OK), resultCode.get(0));
        // verify metadata version is set to above "version"
        assertEquals(Version.Occurred.CONCURRENTLY, metadata.getVersion().compare(new LongVersion(version)));
    }

    /**
     * Test createLedgerMetadata LEDGER_EXISTS, expect rc == LedgerExistException
     */
    @Test
    public void testCreateLedgerMetadataFail() throws Exception {
        long ledgerId = System.currentTimeMillis();
        long version = System.currentTimeMillis();
        LedgerMetadata metadata = createLedgerMetadata();
        List<Integer> resultCode = new ArrayList<Integer>(1);
        GenericCallback<Void> callback = new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                resultCode.add(rc);
            }
        };

        LedgerMetadataResponse response = LedgerMetadataResponse.newBuilder()
          .setCode(StatusCode.LEDGER_EXISTS)
          .setVersion(version)
          .build();

        LedgerMetadataServiceImplBase ledgerMetadataService = new LedgerMetadataServiceImplBase() {
            @Override
            public void create(org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataRequest request,
                               StreamObserver<LedgerMetadataResponse> responseObserver) {
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };

        serviceRegistry.addService(ledgerMetadataService.bindService());

        rpcLedgerManager.createLedgerMetadata(ledgerId, metadata, callback);
        Thread.sleep(100);

        // verify rc returned right in callback
        assertEquals(Integer.valueOf(BKException.Code.LedgerExistException), resultCode.get(0));
    }

    /**
     * Test removeLedgerMetadata SUCCESS, expect rc == OK and metadata.version set
     */
    @Test
    public void testRemoveLedgerMetadataSuccess() throws Exception {
        long ledgerId = System.currentTimeMillis();
        long version = System.currentTimeMillis();
        List<Integer> resultCode = new ArrayList<Integer>(1);
        GenericCallback<Void> callback = new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                resultCode.add(rc);
            }
        };

        LedgerMetadataResponse response = LedgerMetadataResponse.newBuilder()
          .setCode(StatusCode.SUCCESS)
          .setVersion(version)
          .build();

        LedgerMetadataServiceImplBase ledgerMetadataService = new LedgerMetadataServiceImplBase() {
            @Override
            public void remove(org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataRequest request,
                               StreamObserver<LedgerMetadataResponse> responseObserver) {
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };

        serviceRegistry.addService(ledgerMetadataService.bindService());

        rpcLedgerManager.removeLedgerMetadata(ledgerId, new LongVersion(version), callback);
        Thread.sleep(100);

        // verify rc returned in callback
        assertEquals(Integer.valueOf(BKException.Code.OK), resultCode.get(0));
    }

    /**
     * Test removeLedgerMetadata LEDGER_NOT_FOUND, expect rc == NoSuchLedgerExistsException
     */
    @Test
    public void testRemoveLedgerMetadataFail() throws Exception {
        long ledgerId = System.currentTimeMillis();
        long version = System.currentTimeMillis();
        List<Integer> resultCode = new ArrayList<Integer>(1);
        GenericCallback<Void> callback = new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                resultCode.add(rc);
            }
        };

        LedgerMetadataResponse response = LedgerMetadataResponse.newBuilder()
          .setCode(StatusCode.LEDGER_NOT_FOUND)
          .setVersion(version)
          .build();

        LedgerMetadataServiceImplBase ledgerMetadataService = new LedgerMetadataServiceImplBase() {
            @Override
            public void remove(org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataRequest request,
                               StreamObserver<LedgerMetadataResponse> responseObserver) {
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };

        serviceRegistry.addService(ledgerMetadataService.bindService());

        rpcLedgerManager.removeLedgerMetadata(ledgerId, new LongVersion(version), callback);
        Thread.sleep(100);

        // verify rc returned right in callback
        assertEquals(Integer.valueOf(BKException.Code.NoSuchLedgerExistsException), resultCode.get(0));
    }

    /**
     * Test readLedgerMetadata SUCCESS, expect rc == OK and metadata.version set
     */
    @Test
    public void testReadLedgerMetadataSuccess() throws Exception {
        long ledgerId = System.currentTimeMillis();
        long version = System.currentTimeMillis();
        LedgerMetadata metadata = createLedgerMetadata();

        List<Integer> resultCode = new ArrayList<Integer>(1);
        List<LedgerMetadata> resultMetadata = new ArrayList<LedgerMetadata>(1);

        GenericCallback<LedgerMetadata> callback = new GenericCallback<LedgerMetadata>() {
            @Override
            public void operationComplete(int rc, LedgerMetadata m) {
                resultCode.add(rc);
                resultMetadata.add(m);
            }
        };

        LedgerMetadataResponse response = LedgerMetadataResponse.newBuilder()
          .setCode(StatusCode.SUCCESS)
          .setMetadata(metadata.toProtoFormat())
          .setVersion(version)
          .build();

        LedgerMetadataServiceImplBase ledgerMetadataService = new LedgerMetadataServiceImplBase() {
            @Override
            public void read(org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataRequest request,
                               StreamObserver<LedgerMetadataResponse> responseObserver) {
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };

        serviceRegistry.addService(ledgerMetadataService.bindService());

        rpcLedgerManager.readLedgerMetadata(ledgerId, callback);
        Thread.sleep(100);

        // verify rc returned in callback
        assertEquals(Integer.valueOf(BKException.Code.OK), resultCode.get(0));

        // verify returned Metadata
        assertNotNull(resultMetadata.get(0));
        assertEquals(false, metadata.isConflictWith(resultMetadata.get(0)));
    }

    /**
     * Test readLedgerMetadata LEDGER_NOT_FOUND, expect rc == NoSuchLedgerExistsException
     */
    @Test
    public void testReadLedgerMetadataFail() throws Exception {
        long ledgerId = System.currentTimeMillis();
        long version = System.currentTimeMillis();

        List<Integer> resultCode = new ArrayList<Integer>(1);
        List<LedgerMetadata> resultMetadata = new ArrayList<LedgerMetadata>(1);

        GenericCallback<LedgerMetadata> callback = new GenericCallback<LedgerMetadata>() {
            @Override
            public void operationComplete(int rc, LedgerMetadata m) {
                resultCode.add(rc);
                resultMetadata.add(m);
            }
        };

        LedgerMetadataResponse response = LedgerMetadataResponse.newBuilder()
            .setCode(StatusCode.LEDGER_NOT_FOUND)
            .setVersion(version)
            .build();

        LedgerMetadataServiceImplBase ledgerMetadataService = new LedgerMetadataServiceImplBase() {
            @Override
            public void read(org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataRequest request,
                             StreamObserver<LedgerMetadataResponse> responseObserver) {
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };

        serviceRegistry.addService(ledgerMetadataService.bindService());

        rpcLedgerManager.readLedgerMetadata(ledgerId, callback);
        Thread.sleep(100);

        // verify rc returned in callback
        assertEquals(Integer.valueOf(BKException.Code.NoSuchLedgerExistsException), resultCode.get(0));
    }

    /**
     * Test writeLedgerMetadata SUCCESS, expect rc == OK and metadata.version set
     */
    @Test
    public void testWriteLedgerMetadataSuccess() throws Exception {
        long ledgerId = System.currentTimeMillis();
        long version = System.currentTimeMillis();
        LedgerMetadata metadata = createLedgerMetadata();

        List<Integer> resultCode = new ArrayList<Integer>(1);
        GenericCallback<Void> callback = new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void m) {
                resultCode.add(rc);
            }
        };

        LedgerMetadataResponse response = LedgerMetadataResponse.newBuilder()
            .setCode(StatusCode.SUCCESS)
            .setVersion(version)
            .build();

        LedgerMetadataServiceImplBase ledgerMetadataService = new LedgerMetadataServiceImplBase() {
            @Override
            public void write(org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataRequest request,
                             StreamObserver<LedgerMetadataResponse> responseObserver) {
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };

        serviceRegistry.addService(ledgerMetadataService.bindService());

        rpcLedgerManager.writeLedgerMetadata(ledgerId, metadata, callback);
        Thread.sleep(100);

        // verify rc returned in callback
        assertEquals(Integer.valueOf(BKException.Code.OK), resultCode.get(0));

        // verify returned Metadata version
        assertEquals(Version.Occurred.CONCURRENTLY, metadata.getVersion().compare(new LongVersion(version)));
    }

    /**
     * Test readLedgerMetadata LEDGER_NOT_FOUND, expect rc == NoSuchLedgerExistsException
     */
    @Test
    public void testWriteLedgerMetadataFail() throws Exception {
        long ledgerId = System.currentTimeMillis();
        long version = System.currentTimeMillis();
        LedgerMetadata metadata = createLedgerMetadata();

        List<Integer> resultCode = new ArrayList<Integer>(1);
        GenericCallback<Void> callback = new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void m) {
                resultCode.add(rc);
            }
        };

        LedgerMetadataResponse response = LedgerMetadataResponse.newBuilder()
            .setCode(StatusCode.LEDGER_NOT_FOUND)
            .setVersion(version)
            .build();

        LedgerMetadataServiceImplBase ledgerMetadataService = new LedgerMetadataServiceImplBase() {
            @Override
            public void write(org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataRequest request,
                              StreamObserver<LedgerMetadataResponse> responseObserver) {
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };

        serviceRegistry.addService(ledgerMetadataService.bindService());

        rpcLedgerManager.writeLedgerMetadata(ledgerId, metadata, callback);
        Thread.sleep(100);

        // verify rc returned in callback
        assertEquals(Integer.valueOf(BKException.Code.NoSuchLedgerExistsException), resultCode.get(0));
    }

    /**
     * Test getLedgerRanges SUCCESS, expect success and get one LedgerRange
     */
    @Test
    public void testGetLedgerRangesSuccess() throws Exception {
        List<Long> ledgers = Lists.newArrayList(1L, 2L, 3L);
        GetLedgerRangesResponse response = GetLedgerRangesResponse.newBuilder()
          .setCode(StatusCode.SUCCESS)
          .setLedgerRange(GetLedgerRangesResponse.LedgerRangeFormat.newBuilder()
            .addAllLedgerIds(ledgers)
            .build())
          .build();

        LedgerMetadataServiceImplBase ledgerMetadataService = new LedgerMetadataServiceImplBase() {
            @Override
            public void iterate(org.apache.bookkeeper.proto.rpc.metadata.GetLedgerRangesRequest request,
                              StreamObserver<GetLedgerRangesResponse> responseObserver) {
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };

        serviceRegistry.addService(ledgerMetadataService.bindService());
        LedgerRangeIterator iterator = rpcLedgerManager.getLedgerRanges();

        // expect has a LedgerRange got.
        assertTrue(iterator.hasNext());
        LedgerRange ledgerRange = iterator.next();
        // expect the LedgerRange contains all the Long that passed in
        assertEquals(ledgers.size(), ledgerRange.size());

        for (Long ledger: ledgers) {
            assertTrue(ledgerRange.getLedgers().contains(ledger));
        }
        // only one ledgerRange, expect false to get another more.
        assertFalse(iterator.hasNext());
    }

    /**
     * Test getLedgerRanges ERROR, expect get no LedgerRange
     */
    @Test
    public void testGetLedgerRangesFailed() throws Exception {
        GetLedgerRangesResponse response = GetLedgerRangesResponse.newBuilder()
          .setCode(StatusCode.LEDGER_METADATA_ERROR)
          .build();

        LedgerMetadataServiceImplBase ledgerMetadataService = new LedgerMetadataServiceImplBase() {
            @Override
            public void iterate(org.apache.bookkeeper.proto.rpc.metadata.GetLedgerRangesRequest request,
                                StreamObserver<GetLedgerRangesResponse> responseObserver) {
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }
        };

        serviceRegistry.addService(ledgerMetadataService.bindService());
        LedgerRangeIterator iterator = rpcLedgerManager.getLedgerRanges();

        // expect false to get ledgerRange.
        assertFalse(iterator.hasNext());
    }
}
