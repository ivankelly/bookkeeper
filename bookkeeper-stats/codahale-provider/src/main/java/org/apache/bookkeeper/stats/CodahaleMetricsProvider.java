/**
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*
*/
package org.apache.bookkeeper.stats;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;


import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;
import static com.codahale.metrics.MetricRegistry.name;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.configuration.Configuration;

public class CodahaleMetricsProvider implements StatsProvider {
    static final Logger LOG = LoggerFactory.getLogger(CodahaleMetricsProvider.class);

    final static String CODAHALE_STATS_COMPAT_MODE = "codahaleStatsCompatMode";
    static boolean initialized = false;
    static final MetricRegistry metrics = new MetricRegistry();
    static ScheduledReporter reporter = null;

    @Override
    public synchronized void initialize(Configuration conf) {
        if (initialized) {
            return;
        }
        initialized = true;
        metrics.registerAll(new MemoryUsageGaugeSet());
        metrics.registerAll(new GarbageCollectorMetricSet());

        try {
            String registryName = conf.getString("codahaleStatsPrefix");
            boolean compatMode = conf.getBoolean(CODAHALE_STATS_COMPAT_MODE);
            String graphiteHost = conf.getString("codahale_stats_graphite_host");
            int metricsOutputFrequency = conf.getInt("codahale_stats_frequency_s", 60);
            String metricsDir = conf.getString("codahale_stats_output_directory");

            if (!Strings.isNullOrEmpty(graphiteHost)) {
                LOG.info("Configuring stats with graphite");
                HostAndPort addr = HostAndPort.fromString(graphiteHost);
                final Graphite graphite = new Graphite(
                        new InetSocketAddress(addr.getHostText(), addr.getPort()));
                reporter = GraphiteReporter.forRegistry(metrics)
                    .prefixedWith(registryName)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .filter(MetricFilter.ALL)
                    .build(graphite);
            } else if (!Strings.isNullOrEmpty(metricsDir)) {
                // NOTE: 1/ metrics output files are exclusive to a given process
                // 2/ the output directory must exist
                // 3/ if output files already exist they are not overwritten and there is no metrics output
                LOG.info("Configuring stats with csv output to directory [{}]",
                        new File(metricsDir+File.separator+registryName).getAbsolutePath());
                reporter = CsvReporter.forRegistry(metrics)
                        .convertRatesTo(TimeUnit.SECONDS)
                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                        .build(new File(metricsDir+File.separator+registryName));
            } else {
                LOG.info("Configuring stats with slf4j");
                reporter = Slf4jReporter.forRegistry(metrics)
                        .outputTo(LoggerFactory.getLogger(registryName))
                        .convertRatesTo(TimeUnit.SECONDS)
                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                        .build();
            }
            LOG.info("Result are output every {} seconds", metricsOutputFrequency);
            reporter.start(metricsOutputFrequency, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error("Exception starting stat reporter", e);
        }
    }

    @Override
    public synchronized void close() {
        if (reporter != null) {
            reporter.report();
            reporter.stop();
        }
        reporter = null;
        initialized = false;
    }

    @Override
    public OpTimer getOpTimer(final Class group, final String... names) {
        final String[] errorNames = Arrays.copyOf(names, names.length+1);
        errorNames[names.length] = "errors";

        return new OpTimer() {
            Timer timer = metrics.timer(name(group, names));
            com.codahale.metrics.Meter errors = metrics.meter(name(group, errorNames));

            @Override
            public Ctx create() {
                final Timer.Context ctx = timer.time();
                return new Ctx() {
                    public void success() {
                        ctx.stop();
                    }

                    public void failure() {
                        errors.mark();
                    }
                };
            }
        };
    }

    @Override
    public Meter getMeter(final Class group, final String... names) {
        final com.codahale.metrics.Meter m1 = metrics.meter(name(group, names));
        return new Meter() {
            @Override
            public void mark() { m1.mark(); }

            @Override
            public void mark(int value) { m1.mark(value); }
        };
    }

    @Override
    public Histogram getHistogram(final Class group, final String... names) {
        return new Histogram() {
            com.codahale.metrics.Histogram h = metrics.histogram(name(group, names));
            @Override
            public void add(long value) {
                h.update(value);
            }
        };
    }

    @Override
    public <T extends Number> void registerGauge(final Gauge<T> gauge,
                                                 final Class group, String... names) {
        String metricName = name(group, names);
        metrics.remove(metricName);

        metrics.register(metricName, new com.codahale.metrics.Gauge<T>() {
                    @Override
                    public T getValue() {
                        return gauge.getSample();
                    }
            });
    }

    @Override
    public Counter getCounter(final Class group, final String... names) {
        final com.codahale.metrics.Counter c1 = metrics.counter(name(group, names));

        return new Counter() {
                @Override
                public synchronized void clear() {
                    c1.inc(-1 * c1.getCount());
                }

                @Override
                public Long get() {
                    return c1.getCount();
                }

                @Override
                public void inc() {
                    c1.inc();
                }

                @Override
                public void dec() {
                    c1.dec();
                }

                @Override
                public void inc(long delta) {
                    c1.inc(delta);
                }
            };
    }
}
