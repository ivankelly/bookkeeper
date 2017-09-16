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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import java.io.IOException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.rpc.common.StatusCode;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerIdAllocateRequest;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerIdAllocateResponse;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataServiceGrpc.LedgerMetadataServiceFutureStub;

/**
 * A RPC based {@link RpcLedgerIdGenerator}.
 */
class RpcLedgerIdGenerator implements LedgerIdGenerator {

    private static final LedgerIdAllocateRequest ALLOCATE_REQUEST = LedgerIdAllocateRequest.newBuilder().build();

    private final LedgerMetadataServiceFutureStub lmService;

    RpcLedgerIdGenerator(LedgerMetadataServiceFutureStub lmService) {
        this.lmService = lmService;
    }

    @Override
    public void generateLedgerId(GenericCallback<Long> cb) {
        Futures.addCallback(
            lmService.allocate(ALLOCATE_REQUEST),
            new FutureCallback<LedgerIdAllocateResponse>() {

                @Override
                public void onSuccess(LedgerIdAllocateResponse resp) {
                    if (StatusCode.SUCCESS == resp.getCode()) {
                        cb.operationComplete(Code.OK, resp.getLedgerId());
                    } else {
                        cb.operationComplete(Code.MetaStoreException, null);
                    }
                }

                @Override
                public void onFailure(Throwable cause) {
                    cb.operationComplete(Code.MetaStoreException, null);
                }
            });
    }

    @Override
    public void close() throws IOException {
        // no-op
    }

}
