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

import com.google.common.annotations.VisibleForTesting;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.util.Optional;
import org.apache.bookkeeper.client.ClientResources;
import org.apache.bookkeeper.client.resolver.SimpleNameResolverFactory;
import org.apache.bookkeeper.client.utils.RpcUtils;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataServiceGrpc;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataServiceGrpc.LedgerMetadataServiceFutureStub;
import org.apache.bookkeeper.proto.rpc.metadata.LedgerMetadataServiceGrpc.LedgerMetadataServiceStub;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class RpcLedgerManagerFactory extends LedgerManagerFactory {

    private static final int CUR_VERSION = 1;

    private final Optional<ManagedChannelBuilder> channelBuilder;

    // variables initialized by {@link #initialize()}
    ClientConfiguration conf;
    ManagedChannel channel;
    LedgerMetadataServiceStub lmService;
    LedgerMetadataServiceFutureStub lmFutureService;

    RpcLedgerManagerFactory() {
        this(Optional.empty());
    }

    @VisibleForTesting
    RpcLedgerManagerFactory(Optional<ManagedChannelBuilder> channelBuilder) {
        this.channelBuilder = channelBuilder;
    }

    @Override
    public int getCurrentVersion() {
        return CUR_VERSION;
    }

    @Override
    public LedgerManagerFactory initialize(AbstractConfiguration conf, ZooKeeper zk, int factoryVersion)
            throws IOException {
        if (CUR_VERSION != factoryVersion) {
            throw new IOException("Incompatible layout version found : " + factoryVersion);
        }
        this.conf = new ClientConfiguration();
        this.conf.loadConf(conf);

        // initialize the channel
        // TODO: initialize TLS channel
        ManagedChannelBuilder builder = channelBuilder.orElse(
            NettyChannelBuilder
                .forTarget("bookie")
                .nameResolverFactory(SimpleNameResolverFactory.of(this.conf.getClientBootstrapBookies()))
                .usePlaintext(true));
        this.channel = builder.build();

        // configure the ledger metadata rpc service
        this.lmService = RpcUtils.configureRpcStub(
            LedgerMetadataServiceGrpc.newStub(channel),
            Optional.empty());
        this.lmFutureService = RpcUtils.configureRpcStub(
            LedgerMetadataServiceGrpc.newFutureStub(channel),
            Optional.empty());
        return this;
    }

    @Override
    public void uninitialize() throws IOException {
        if (null != channel) {
            channel.shutdown();
        }
    }

    @Override
    public LedgerIdGenerator newLedgerIdGenerator() {
        return new RpcLedgerIdGenerator(lmFutureService);
    }

    @Override
    public LedgerManager newLedgerManager() {
        return new RpcLedgerManager(lmService,
            lmFutureService,
            ClientResources.create(conf, NullStatsLogger.INSTANCE).workerPool());
    }

    @Override
    public LedgerUnderreplicationManager newLedgerUnderreplicationManager()
            throws KeeperException, InterruptedException, CompatibilityException {
        throw new UnsupportedOperationException("Ledger Underreplicated Manager is not supported yet");
    }

}
