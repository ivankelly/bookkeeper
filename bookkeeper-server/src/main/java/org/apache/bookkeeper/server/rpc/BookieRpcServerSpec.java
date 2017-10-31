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
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.server.conf.RpcConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.inferred.freebuilder.FreeBuilder;

/**
 * Builder to build a rpc server.
 */
@FreeBuilder
public interface BookieRpcServerSpec {

    /**
     * A bookie supplier that supplies a bookie object for building rpc server.
     *
     * @return bookie supplier
     */
    Supplier<Bookie> bookieSupplier();

    /**
     * Get a rpc related configuration object for building rpc server.
     *
     * @return rpc related configuration object.
     */
    RpcConfiguration rpcConf();

    /**
     * Get the scheduler used for building rpc server.
     *
     * @return scheduler used for building rpc server.
     */
    Resource<OrderedSafeExecutor> schedulerResource();

    /**
     * Get the rpc endpoint.
     *
     * @return rpc endpoint.
     */
    Optional<BookieSocketAddress> endpoint();

    /**
     * Stats logger used by the rpc server.
     *
     * @return stats logger used by the rpc server
     */
    StatsLogger statsLogger();

    /**
     * Get the local server name for building in-process rpc server.
     *
     * <p>This setting is used for in-process rpc server. It only takes effect when
     * {@link #endpoint()} is empty.
     *
     * @return local server name.
     */
    Optional<String> localServerName();

    /**
     * Get the local handler registry for building in-process rpc server.
     *
     * <p>This setting is used for in-process rpc server. It only takes effect when
     * {@link #endpoint()} is empty.
     *
     * @return local handler registry
     */
    Optional<HandlerRegistry> localHandlerRegistry();

    /**
     * Builder to build rpc server.
     */
    class Builder extends BookieRpcServerSpec_Builder {}

    static Builder newBuilder() {
        return new Builder();
    }

}
