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

public class TwitterStatsProvider implements StatsProvider {
    static final Logger LOG = LoggerFactory.getLogger(TwitterStatsProvider.class);

    static HTTPStatsExporter http = new HTTPStatsExporter(8080);
    static AtomicBoolean initialized = new AtomicBoolean(false);

    void init() {
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
    public TimedOp getTimedOp(final String name) {
        final long start = System.currentTimeMillis();
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

            public void fail() {
                getStats().incErrors(
                        TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()-start));
            }
        };
    }

    @Override
    public <T extends Number> void registerGauge(String name, final Gauge<T> gauge) {
        Stats.export(new SampledStat<T>(name, gauge.getSample()) {
                @Override
                public T doSample() {
                    return gauge.getSample();
                }
            });
    }

    @Override
    public Counter getCounter(final String name) {
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
