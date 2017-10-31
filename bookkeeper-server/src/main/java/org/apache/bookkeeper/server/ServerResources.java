/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.server;

import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.OrderedSafeExecutor;

/**
 * Define a set of resources used for a server.
 */
public class ServerResources {

    public static ServerResources create(StatsLogger statsLogger) {
        return new ServerResources(statsLogger);
    }

    private final Resource<OrderedSafeExecutor> scheduler;

    private ServerResources(StatsLogger statsLogger) {
        this.scheduler =
            new Resource<OrderedSafeExecutor>() {

                private static final String name = "BookieScheduler";

                @Override
                public OrderedSafeExecutor create() {
                    return OrderedSafeExecutor.newBuilder()
                        .name(name)
                        .numThreads(Runtime.getRuntime().availableProcessors() * 2)
                        .statsLogger(statsLogger)
                        .build();
                }

                @Override
                public void close(OrderedSafeExecutor instance) {
                    instance.shutdown();
                }

                @Override
                public String toString() {
                    return name;
                }
            };
    }

    public Resource<OrderedSafeExecutor> scheduler() {
        return scheduler;
    }

}
