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

package org.apache.bookkeeper.server.service;

import java.io.IOException;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.server.conf.RpcConfiguration;
import org.apache.bookkeeper.server.rpc.BookieRpcServer;
import org.apache.bookkeeper.server.rpc.BookieRpcServerSpec;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Service component that runs {@link org.apache.bookkeeper.server.rpc.BookieRpcServer}.
 */
public class BookieRpcService extends AbstractLifecycleComponent<RpcConfiguration> {

    private final BookieRpcServerSpec builder;
    private BookieRpcServer rpcServer;

    public BookieRpcService(RpcConfiguration conf,
                            BookieRpcServerSpec builder,
                            StatsLogger statsLogger) {
        super("bookie-rpc-service", conf, statsLogger);
        this.builder = builder;
    }

    public BookieRpcServerSpec getBuilder() {
        return builder;
    }

    @Override
    protected void doStart() {
        this.rpcServer = BookieRpcServer.build(builder);
        this.rpcServer.start();
    }

    @Override
    protected void doStop() {
        this.rpcServer.stop();
    }

    @Override
    protected void doClose() throws IOException {
        this.rpcServer.close();
    }
}
