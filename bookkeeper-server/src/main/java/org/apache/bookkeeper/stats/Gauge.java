package org.apache.bookkeeper.stats;

public interface Gauge<T> {
    public T getSample();
}
