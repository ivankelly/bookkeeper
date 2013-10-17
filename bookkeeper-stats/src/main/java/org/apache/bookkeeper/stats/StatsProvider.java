/**
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
package org.apache.bookkeeper.stats;

import org.apache.commons.configuration.Configuration;

/**
 * A statistics provider. This collects the statistics for an
 * application. Reporting of the stats can also be initialized in the
 * #initialize(Configuration) method. Implementations of this class must
 * have a default constructor.
 *
 * Any initialization can be done in the #initialize(Configuration) method.
 */
public interface StatsProvider {
    /**
     * Initialize the provider. If this fails it should log an error
     * message rather than throwing any exceptions.
     */
    void initialize(Configuration conf);

    /**
     * Get an operation timer
     * @return a OpTimer object to be completed when the operation is completed
     */
    OpTimer getOpTimer(Class group, String... names);

    /**
     * Get a histogram
     * @return a histogram
     */
    Histogram getHistogram(Class group, String... names);

    /**
     * Register a gauge with the provider.
     * @see Gauge
     */
    <T extends Number> void registerGauge(Gauge<T> gauge,
            Class group, String... names);

    Meter getMeter(Class group, String... names);

    /**
     * Get a named counter.
     * @return the counter specified by group and name
     */
    Counter getCounter(Class group, String... names);

    /**
     * Free any resources held by the stats provider
     */
    void close();
}
