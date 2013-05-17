package org.apache.bookkeeper.stats;

public interface StatsProvider {
    TimedOp getTimedOp(String name);

    <T extends Number> void registerGauge(String name, Gauge<T> gauge);

    Counter getCounter(String name);
}
