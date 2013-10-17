package org.apache.bookkeeper.bookie.lsmindex;

import org.apache.bookkeeper.proto.DataFormats.KeyValue;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;

import org.apache.bookkeeper.stats.Meter;
import org.apache.bookkeeper.stats.Stats;
import org.apache.bookkeeper.conf.ClientConfiguration;

import java.util.Comparator;
import java.util.Random;
import java.io.IOException;
import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchSSTable {
    private final static Logger LOG
        = LoggerFactory.getLogger(BenchSSTable.class);

    static class StatCollectorIterator implements KeyValueIterator {
        final KeyValueIterator iter;

        final Meter bytesIterated;
        final Meter entriesIterated;

        StatCollectorIterator(String statname, KeyValueIterator iter) {
            this.iter = iter;
            bytesIterated = Stats.get().getMeter(BenchSSTable.class, statname + "-bytesIterated");
            entriesIterated = Stats.get().getMeter(BenchSSTable.class, statname + "-entriesIterated");
        }

        @Override
        public synchronized boolean hasNext() throws IOException {
            return iter.hasNext();
        }

        @Override
        public synchronized KeyValue peek() throws IOException {
            return iter.peek();
        }

        @Override
        public synchronized KeyValue next() throws IOException {
            KeyValue kv = iter.next();
            bytesIterated.mark(kv.getSerializedSize());
            entriesIterated.mark();
            return kv;
        }

        @Override
        public void close() throws IOException {
            iter.close();
        }
    }

    static class RandomDataIterator implements KeyValueIterator {
        int iterateCount = 0;
        int lastKey = 0;
        final int count;
        KeyValue next = null;
        final Random r;
        final byte[] data;

        RandomDataIterator(int seed, int count) {
            r = new Random(seed);
            data = new byte[30];
            this.count = count;
        }

        @Override
        public synchronized boolean hasNext() throws IOException {
            if (next == null) {
                int key = lastKey + 1;
                lastKey = key;                
                next = KeyValue.newBuilder()
                    .setKey(ByteString.copyFrom(Ints.toByteArray(key)))
                    .setValue(ByteString.copyFrom(data)).build();
            }
            if (iterateCount < count) {
                return true;
            }
            return false;
        }

        @Override
        public synchronized KeyValue peek() throws IOException {
            if (!hasNext()) {
                throw new IOException("No next entry");
            }
            return next;
        }

        @Override
        public synchronized KeyValue next() throws IOException {
            try {
                return peek();
            } finally {
                next = null;
            }
        }

        @Override
        public void close() throws IOException {
        }
    }

    public static void main(String[] args) throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setProperty("statsProviderClass","org.apache.bookkeeper.stats.CodahaleMetricsProvider");
        conf.setProperty("codahale_stats_name", "lsmstats");
        conf.setProperty("codahale_stats_frequency_s", 5);
        //codahale_stats_graphite_host={{ graphite }}
        Stats.init(conf);

        KeyValueIterator iter = new StatCollectorIterator("writes", new RandomDataIterator(0xdeadbeef, 1000000));
        Comparator<ByteString> comp = KeyComparators.unsignedLexicographical();
        
        LOG.info("Starting writing");
        SSTableImpl.store(new File("1.sst"), comp, iter);
        LOG.info("Finished writing");
    }
}
