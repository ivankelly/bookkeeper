package org.apache.bookkeeper.bookie.lsmindex;

import org.apache.bookkeeper.proto.DataFormats.KeyValue;
import java.io.IOException;

public interface KeyValueIterator {
    public boolean hasNext() throws IOException;
    public KeyValue peek() throws IOException;
    public KeyValue next() throws IOException;
    public void close() throws IOException;
}
