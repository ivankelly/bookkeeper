package org.apache.bookkeeper.stats;

public interface TimedOp {
    void success();
    void fail();
}
