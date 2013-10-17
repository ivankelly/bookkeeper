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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.BaseConfiguration;

import org.junit.Test;
import org.junit.Assert;

public class TestStatsProvider {
    static final Logger LOG = LoggerFactory.getLogger(TestStatsProvider.class);

    /**
     * Test that when we configure a provider, it will be constructed.
     * And that if it doesn't exist, no exception will be thrown and
     * NullStatsProvider will be configured instead.
     */
    @Test
    public void testConstruction() {
        Configuration conf = new BaseConfiguration();
        conf.setProperty(Stats.STATS_PROVIDER_CLASS, "Class does not exist");
        Stats.init(conf);
        Assert.assertEquals("Should be the null provider",
                            Stats.get().getClass(), NullStatsProvider.class);

        conf.setProperty(Stats.STATS_PROVIDER_CLASS, DummyStatsProvider.class.getName());
        Stats.init(conf);
        Assert.assertEquals("Should be the dummy provider",
                            Stats.get().getClass(), DummyStatsProvider.class);
    }

    public static class DummyStatsProvider implements StatsProvider {
        @Override
        public void initialize(Configuration conf) { /* noop */ }

        @Override
        public void close() {
        }

        @Override
        public OpTimer getOpTimer(Class group, String... names) {
            return new OpTimer() {
                @Override
                public Ctx create() {
                    return new Ctx() {
                        @Override
                        public void success() { }

                        @Override
                        public void failure() { }
                    };
                }
            };
        }

        @Override
        public Histogram getHistogram(Class group, String... names) {
            return new Histogram() {
                @Override
                public void add(long value) { /* noop */ }
            };
        }

        @Override
        public Meter getMeter(Class meter, String... names) {
            return new Meter() {
                @Override
                public void mark() {}
                @Override
                public void mark(int value) {}
            };
        }

        @Override
        public <T extends Number> void registerGauge(Gauge<T> gauge,
                Class group, String... names) {
            // noop
        }

        @Override
        public Counter getCounter(Class group, String... names) {
            return new Counter() {
                @Override
                public void clear() { /* noop */ }
                @Override
                public void inc() { /* noop */ }
                @Override
                public void dec() { /* noop */ }
                @Override
                public void inc(long delta) { /* noop */ }
                @Override
                public Long get() { return 0L; }
            };
        }
    }
}
