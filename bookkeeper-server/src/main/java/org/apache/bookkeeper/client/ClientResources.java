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

package org.apache.bookkeeper.client;

import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Define a set of resources used by a client.
 */
public final class ClientResources {

    public static ClientResources create(ClientConfiguration conf,
                                         StatsLogger statsLogger) {
        return new ClientResources(conf, statsLogger);
    }

    private final Resource<OrderedScheduler> workerPoolResource;

    private ClientResources(ClientConfiguration conf,
                            StatsLogger statsLogger) {
        this.workerPoolResource = new Resource<OrderedScheduler>() {

            private static final String name = "BookKeeperClientWorker";

            @Override
            public OrderedScheduler create() {
                return OrderedScheduler.newSchedulerBuilder()
                    .name(name)
                    .numThreads(conf.getNumWorkerThreads())
                    .statsLogger(statsLogger)
                    .traceTaskExecution(conf.getEnableTaskExecutionStats())
                    .traceTaskWarnTimeMicroSec(conf.getTaskExecutionWarnTimeMicros())
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
    }

    public Resource<OrderedScheduler> workerPool() {
        return workerPoolResource;
    }
}
