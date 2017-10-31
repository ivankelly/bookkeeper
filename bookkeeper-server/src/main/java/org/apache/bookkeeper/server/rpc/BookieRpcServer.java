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

import io.grpc.HandlerRegistry;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.common.util.SharedResourceManager;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.server.conf.RpcConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.OrderedSafeExecutor;

/**
 * A {@link org.apache.bookkeeper.common.component.LifecycleComponent} that starts rpc services.
 */
@Slf4j
public class BookieRpcServer extends AbstractLifecycleComponent<RpcConfiguration> {

    public static final String NAME = "bookie-rpc";

    /**
     * Build a {@link BookieRpcServer} from a {@link BookieRpcServerSpec} spec.
     *
     * @param builder builder spec to build {@link BookieRpcServer}.
     * @return a built bookie rpc server instance.
     */
    public static BookieRpcServer build(BookieRpcServerSpec builder) {
        return new BookieRpcServer(
            builder.bookieSupplier().get(),
            builder.rpcConf(),
            builder.endpoint(),
            builder.localServerName(),
            builder.localHandlerRegistry(),
            builder.statsLogger(),
            builder.schedulerResource());
    }

    private final BookieSocketAddress myEndpoint;
    private final Server rpcServer;
    private final Resource<OrderedSafeExecutor> schedulerResource;
    private final OrderedSafeExecutor scheduler;

    BookieRpcServer(Bookie bookie,
                    RpcConfiguration conf,
                    BookieSocketAddress myEndpoint,
                    StatsLogger statsLogger,
                    Resource<OrderedSafeExecutor> schedulerResource) {
        this(
            bookie,
            conf,
            Optional.of(myEndpoint),
            Optional.empty(),
            Optional.empty(),
            statsLogger,
            schedulerResource);
    }

    private BookieRpcServer(Bookie bookie,
                            RpcConfiguration conf,
                            Optional<BookieSocketAddress> myEndpoint,
                            Optional<String> localServerName,
                            Optional<HandlerRegistry> localHandlerRegistry,
                            StatsLogger statsLogger,
                            Resource<OrderedSafeExecutor> schedulerResource) {
        super("bookie-rpc-server", conf, statsLogger);
        this.myEndpoint = myEndpoint.orElse(null);
        this.schedulerResource = schedulerResource;
        this.scheduler = SharedResourceManager.shared().get(schedulerResource);
        if (localServerName.isPresent()) {
            InProcessServerBuilder serverBuilder = InProcessServerBuilder
                .forName(localServerName.get())
                .directExecutor();
            if (localHandlerRegistry.isPresent()) {
                serverBuilder = serverBuilder.fallbackHandlerRegistry(localHandlerRegistry.get());
            }
            this.rpcServer = serverBuilder.build();
        } else {
            this.rpcServer = ServerBuilder
                .forPort(this.myEndpoint.getPort())
                .addService(new LedgerMetadataRpcService(
                    bookie.getLedgerManagerFactory().newLedgerIdGenerator(),
                    bookie.getLedgerManager(),
                    scheduler
                ))
                .build();
        }
    }


    @Override
    protected void doStart() {
        try {
            rpcServer.start();
        } catch (IOException ioe) {
            log.error("Failed to start bookie rpc service", ioe);
            throw new RuntimeException("Failed to start bookie rpc service", ioe);
        }
    }

    @Override
    protected void doStop() {
        rpcServer.shutdown();
    }

    @Override
    protected void doClose() throws IOException {
        SharedResourceManager.shared()
            .release(schedulerResource, scheduler);
    }
}
