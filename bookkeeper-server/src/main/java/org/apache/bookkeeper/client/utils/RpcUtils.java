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

import static org.apache.bookkeeper.common.concurrent.FutureUtils.fromListenableFuture;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractStub;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.common.util.Backoff;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.Retries;
import org.apache.bookkeeper.proto.rpc.metadata.GetLedgerRangesRequest;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataRequest;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;

/**
 * RPC utility functions
 */
public final class RpcUtils {

    private static final String TOKEN = "token";

    // backoff parameters

    public static final int DEFAULT_BACKOFF_START_MS = 200;
    public static final int DEFAULT_BACKOFF_MAX_MS = 1000;
    public static final int DEFAULT_BACKOFF_MULTIPLIER = 2;

    // default retry predicate
    private static final Predicate<Throwable> DEFAULT_RPC_RETRY_PREDICATE =
        cause -> shouldRetryOnRpcException(cause);

    private RpcUtils() {}

    public static <T extends AbstractStub<T>> T configureRpcStub(T stub, Optional<String> token) {
        return token.map(t -> {
            Metadata metadata = new Metadata();
            Metadata.Key<String> tokenKey = Metadata.Key.of(TOKEN, Metadata.ASCII_STRING_MARSHALLER);
            metadata.put(tokenKey, t);
            CallCredentials callCredentials = (method, attrs, appExecutor, applier) -> {
                applier.apply(metadata);
            };
            return stub.withCallCredentials(callCredentials);
        }).orElse(stub);
    }

    public static Stream<Long> defaultBackoffs() {
        return Backoff.exponential(
            DEFAULT_BACKOFF_START_MS,
            DEFAULT_BACKOFF_MULTIPLIER,
            DEFAULT_BACKOFF_MAX_MS);
    }

    public static Predicate<Throwable> defaultRpcRetryPredicate() {
        return DEFAULT_RPC_RETRY_PREDICATE;
    }

    public static LedgerMetadataRequest createLedgerMetadataRequest(long ledgerId,
                                                                    LedgerMetadata metadata) {
        return LedgerMetadataRequest.newBuilder()
            .setLedgerId(ledgerId)
            .setMetadata(metadata.toProtoFormat())
            .build();
    }

    public static LedgerMetadataRequest removeLedgerMetadataRequest(long ledgerId,
                                                                    Version version) {
        LongVersion longVersion;
        if(version == Version.ANY) {
            longVersion = LongVersion.ANY_LONG;
        } else {
            longVersion = (LongVersion) version;
        }

        return LedgerMetadataRequest.newBuilder()
            .setLedgerId(ledgerId)
            .setExpectedVersion(longVersion.getLongVersion())
            .build();
    }

    public static LedgerMetadataRequest readLedgerMetadataRequest(long ledgerId) {
        return LedgerMetadataRequest.newBuilder().setLedgerId(ledgerId).build();
    }

    public static LedgerMetadataRequest writeLedgerMetadataRequest(long ledgerId, LedgerMetadata metadata) {
        return LedgerMetadataRequest.newBuilder()
            .setLedgerId(ledgerId)
            .setMetadata(metadata.toProtoFormat())
            .setExpectedVersion(((LongVersion) metadata.getVersion()).getLongVersion())
            .buildPartial();
    }

    public static LedgerMetadataRequest watchLedgerMetadataRequest(long ledgerId) {
        return LedgerMetadataRequest.newBuilder().setLedgerId(ledgerId).build();
    }

    public static GetLedgerRangesRequest getLedgerRangesRequest(int limit) {
        return GetLedgerRangesRequest.newBuilder()
            .setLimitPerResponse(limit)
            .build();
    }

    /**
     * Check if we should retry on a RPC exception <i>cause</i>.
     *
     * @param cause the rpc exception
     * @return true if we should retry on a RPC exception, otherwise false.
     */
    public static boolean shouldRetryOnRpcException(Throwable cause) {
        if (cause instanceof StatusRuntimeException || cause instanceof StatusException) {
            Status rpcStatus;
            if (cause instanceof StatusRuntimeException) {
                rpcStatus = ((StatusRuntimeException) cause).getStatus();
            } else {
                rpcStatus = ((StatusException) cause).getStatus();
            }
            switch (rpcStatus.getCode()) {
                case INVALID_ARGUMENT:
                case NOT_FOUND:
                case ALREADY_EXISTS:
                case PERMISSION_DENIED:
                case UNAUTHENTICATED:
                    return false;
                default:
                    return true;
            }
        } else if (cause instanceof RuntimeException) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Execute an operation with the default backoffs and default retry predicate.
     *
     * @param opSupplier the supplier that supplies operation to run
     * @param scheduler scheduler to execute the callback
     * @param maxNumRetries max num of retries
     */
    public static <T> CompletableFuture<T> retry(Supplier<ListenableFuture<T>> opSupplier,
                                                 OrderedScheduler scheduler,
                                                 int maxNumRetries) {
        return Retries.run(
            defaultBackoffs().limit(maxNumRetries),
            defaultRpcRetryPredicate(),
            () -> fromListenableFuture(opSupplier.get(), Function.identity()),
            scheduler);
    }

}
