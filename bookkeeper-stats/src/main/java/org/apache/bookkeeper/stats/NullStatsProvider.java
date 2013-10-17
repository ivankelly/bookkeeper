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
 * Default stats provider. Does nothing.
 */
public class NullStatsProvider implements StatsProvider {
    @Override
    public void initialize(Configuration conf) { /* noop */ }

    @Override
    public void close() { /* noop */ }

    private OpTimer opTimerInstance = new OpTimer() {
            Ctx ctx = new Ctx() {
                    @Override
                    public void success() { /* noop */ }

                    @Override
                    public void failure() { /* noop */ }
                };

            @Override
            public Ctx create() {
                return ctx;
            }
        };

    @Override
    public OpTimer getOpTimer(Class group, String... names) {
        return opTimerInstance;
    }

    private Histogram histInstance = new Histogram() {
            @Override
            public void add(long value) { /* noop */ }
        };

    @Override
    public Histogram getHistogram(Class group, String... names) {
        return histInstance;
    }

    @Override
    public <T extends Number> void registerGauge(Gauge<T> gauge, Class group, String... names) {
        // noop
    }

    private Meter meterInstance = new Meter() {
            @Override
            public void mark() {}
            @Override
            public void mark(int value) {}
        };

    @Override
    public Meter getMeter(Class group, String... names) {
        return meterInstance;
    }

    private Counter counterInstance = new Counter() {
            @Override
            public void clear() { /* noop */ }
            @Override
            public void inc() { /* noop */ }
            @Override
            public void dec() { /* noop */ }
            @Override
            public void inc(long delta) { /* noop */ }
            @Override
            public Long get()  { return 0L; }
        };

    @Override
    public Counter getCounter(Class group, String... names) {
        return counterInstance;
    }
}
