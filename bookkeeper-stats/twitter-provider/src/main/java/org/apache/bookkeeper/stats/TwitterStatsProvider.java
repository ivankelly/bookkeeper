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

import com.twitter.common.stats.Rate;
import com.twitter.common.stats.Stats;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.common.stats.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.configuration.Configuration;

public class TwitterStatsProvider implements StatsProvider {
    static final Logger LOG = LoggerFactory.getLogger(TwitterStatsProvider.class);

    static HTTPStatsExporter http = new HTTPStatsExporter(8080);
    static AtomicBoolean initialized = new AtomicBoolean(false);

    @Override
    public void initialize(Configuration conf) {
        if (initialized.compareAndSet(false,true)) {
            try {
                http.start();
            } catch (Exception e) {
                LOG.error("Exception starting stat reporter", e);
            }
        }
    }

    ConcurrentHashMap<String,RequestStats> requestStats
        = new ConcurrentHashMap<String,RequestStats>();
    ConcurrentHashMap<String,Counter> counters
        = new ConcurrentHashMap<String,Counter>();

    @Override
    public TimedOp getTimedOp(final String group, final String namePart) {
        final long start = System.currentTimeMillis();
        final String name = group + "_" + namePart;
        return new TimedOp() {
            private RequestStats getStats() {
                if (requestStats.containsKey(name)) {
                    return requestStats.get(name);
                }
                RequestStats stats = new RequestStats(name);
                RequestStats oldstats = requestStats.putIfAbsent(name, stats);
                if (oldstats != null) {
                    return oldstats;
                } else {
                    return stats;
                }
            }

            public void success() {
                getStats().requestComplete(
                        TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()-start));
            }

            public void failure() {
                getStats().incErrors(
                        TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()-start));
            }
        };
    }

    @Override
    public <T extends Number> void registerGauge(final String group, String name,
                                                 final Gauge<T> gauge) {
        Stats.export(new SampledStat<T>(group + "_" + name, gauge.getSample()) {
                @Override
                public T doSample() {
                    return gauge.getSample();
                }
            });
    }

    @Override
    public Counter getCounter(final String group, final String namePart) {
        final String name = group + "_" + namePart;
        if (counters.containsKey(name)) {
            return counters.get(name);
        }
        final AtomicLong value = new AtomicLong(0);
        Counter c = new Counter() {
                @Override
                public synchronized void clear() {
                    value.getAndSet(0);
                }

                @Override
                public Long get() {
                    return value.get();
                }

                @Override
                public void inc() {
                    value.incrementAndGet();
                }

                @Override
                public void dec() {
                    value.decrementAndGet();
                }

                @Override
                public void add(long delta) {
                    value.addAndGet(delta);
                }
            };

        Counter oldc = counters.putIfAbsent(name, c);
        if (oldc != null) {
            return oldc;
        } else {
            // Export the value.
            Stats.export(name, value);
            // Export the rate of this value.
            Stats.export(Rate.of(name + "_per_sec", value).build());
            return c;
        }
    }
}
