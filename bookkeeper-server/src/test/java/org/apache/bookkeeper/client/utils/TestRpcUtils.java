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

package org.apache.bookkeeper.client.utils;


import static com.google.common.base.Charsets.UTF_8;
import static io.grpc.Status.ABORTED;
import static io.grpc.Status.ALREADY_EXISTS;
import static io.grpc.Status.INVALID_ARGUMENT;
import static io.grpc.Status.NOT_FOUND;
import static io.grpc.Status.PERMISSION_DENIED;
import static io.grpc.Status.UNAUTHENTICATED;
import static org.apache.bookkeeper.client.utils.RpcUtils.createLedgerMetadataRequest;
import static org.apache.bookkeeper.client.utils.RpcUtils.getLedgerRangesRequest;
import static org.apache.bookkeeper.client.utils.RpcUtils.readLedgerMetadataRequest;
import static org.apache.bookkeeper.client.utils.RpcUtils.removeLedgerMetadataRequest;
import static org.apache.bookkeeper.client.utils.RpcUtils.watchLedgerMetadataRequest;
import static org.apache.bookkeeper.client.utils.RpcUtils.writeLedgerMetadataRequest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Maps;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.util.Map;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.proto.rpc.metadata.GetLedgerRangesRequest;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataRequest;
import org.apache.bookkeeper.versioning.LongVersion;
import org.junit.Test;

/**
 * Unit test of {@link RpcUtils}.
 */
public class TestRpcUtils {

    @Test
    public void testShouldRetryOnRpcException() {
        assertFalse(RpcUtils.shouldRetryOnRpcException(new StatusRuntimeException(INVALID_ARGUMENT)));
        assertFalse(RpcUtils.shouldRetryOnRpcException(new StatusRuntimeException(NOT_FOUND)));
        assertFalse(RpcUtils.shouldRetryOnRpcException(new StatusRuntimeException(ALREADY_EXISTS)));
        assertFalse(RpcUtils.shouldRetryOnRpcException(new StatusRuntimeException(PERMISSION_DENIED)));
        assertFalse(RpcUtils.shouldRetryOnRpcException(new StatusRuntimeException(UNAUTHENTICATED)));

        assertFalse(RpcUtils.shouldRetryOnRpcException(new StatusException(INVALID_ARGUMENT)));
        assertFalse(RpcUtils.shouldRetryOnRpcException(new StatusException(NOT_FOUND)));
        assertFalse(RpcUtils.shouldRetryOnRpcException(new StatusException(ALREADY_EXISTS)));
        assertFalse(RpcUtils.shouldRetryOnRpcException(new StatusException(PERMISSION_DENIED)));
        assertFalse(RpcUtils.shouldRetryOnRpcException(new StatusException(UNAUTHENTICATED)));

        assertFalse(RpcUtils.shouldRetryOnRpcException(new RuntimeException()));

        assertTrue(RpcUtils.shouldRetryOnRpcException(new StatusRuntimeException(ABORTED)));
        assertTrue(RpcUtils.shouldRetryOnRpcException(new StatusException(ABORTED)));
    }

    @Test
    public void testCreateLedgerMetadataRequest() {
        long time = System.currentTimeMillis();
        Map<String, byte[]> customMetadata = Maps.newHashMap();
        customMetadata.put("test_key", "test_value".getBytes(UTF_8));
        LedgerMetadata metadata = new LedgerMetadata(5,
            3,
            2,
            BookKeeper.DigestType.CRC32,
            "password".getBytes(UTF_8),
            customMetadata);
        metadata.setVersion(new LongVersion(time));

        LedgerMetadataRequest request = createLedgerMetadataRequest(time, metadata);
        assertEquals(time, request.getLedgerId());
        assertEquals(metadata.toProtoFormat(), request.getMetadata());
    }

    @Test
    public void testRemoveLedgerMetadataRequest() {
        long time = System.currentTimeMillis();
        LedgerMetadataRequest request = removeLedgerMetadataRequest(time, new LongVersion(time));
        assertEquals(time, request.getLedgerId());
        assertEquals(time, request.getExpectedVersion());
    }

    @Test
    public void testReadLedgerMetadataRequest() {
        long time = System.currentTimeMillis();
        LedgerMetadataRequest request = readLedgerMetadataRequest(time);
        assertEquals(time, request.getLedgerId());
    }

    @Test
    public void testWriteLedgerMetadataRequest() {
        long time = System.currentTimeMillis();
        Map<String, byte[]> customMetadata = Maps.newHashMap();
        customMetadata.put("test_key", "test_value".getBytes(UTF_8));
        LedgerMetadata metadata = new LedgerMetadata(5,
          3,
          2,
          BookKeeper.DigestType.CRC32,
          "password".getBytes(UTF_8),
          customMetadata);
        metadata.setVersion(new LongVersion(time));

        LedgerMetadataRequest request = writeLedgerMetadataRequest(time, metadata);
        assertEquals(time, request.getLedgerId());
        assertEquals(metadata.toProtoFormat(), request.getMetadata());
        assertEquals(time, request.getExpectedVersion());
    }

    @Test
    public void testWatchLedgerMetadataRequest() {
        long time = System.currentTimeMillis();
        LedgerMetadataRequest request = watchLedgerMetadataRequest(time);
        assertEquals(time, request.getLedgerId());
    }

    @Test
    public void testGetLedgerRangesRequest() {
        int limit = 17;
        GetLedgerRangesRequest request = getLedgerRangesRequest(limit);
        assertEquals(limit, request.getLimitPerResponse());
    }
}
