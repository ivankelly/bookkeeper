package org.apache.bookkeeper.bookie.lsmindex;

import org.apache.bookkeeper.proto.DataFormats.KeyValue;
import com.google.common.primitives.Ints;
import com.google.protobuf.ByteString;

import org.apache.bookkeeper.stats.Meter;
import org.apache.bookkeeper.stats.OpTimer;
import org.apache.bookkeeper.stats.Stats;
import org.apache.bookkeeper.conf.ClientConfiguration;

import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;
import java.io.IOException;
import java.io.File;
import java.io.ByteArrayOutputStream;
import com.google.common.io.Closeables;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BenchSSTable {
    private final static Logger LOG
        = LoggerFactory.getLogger(BenchSSTable.class);

    static class RandomDataIterator implements KeyValueIterator {
        int iterateCount = 0;
        int lastKey = 0;
        final int count;
        KeyValue next = null;
        final Random r;
        final byte[] data;

        RandomDataIterator(int seed, int count, int entrySize) {
            r = new Random(seed);
            data = new byte[entrySize];
            this.count = count;
        }

        @Override
        public synchronized boolean hasNext() throws IOException {
            if (next == null) {
                int key = lastKey + r.nextInt(100);
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
                iterateCount++;
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

        Options options = new Options();
        options.addOption("file", true, "File to write to");
        options.addOption("numEntries", true, "Num entries to write");
        options.addOption("entrySize", true, "Entry size");
        options.addOption("mode", true, "Mode to run (serialize, write");
        options.addOption("seed", true, "Seed to use for randomness");
        options.addOption("help", false, "This message");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help") || !cmd.hasOption("mode")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("BenchSSTable <options>", options);
            System.exit(-1);
        }

        Stats.init(conf);

        int numEntries = Integer.valueOf(cmd.getOptionValue("numEntries", "1000000"));
        int entrySize = Integer.valueOf(cmd.getOptionValue("entrySize", "100"));

        String mode = cmd.getOptionValue("mode");
        int seed = Integer.valueOf(cmd.getOptionValue("seed", "0xdeadbeef"));
        if (mode.equals("serialize")) {
            OpTimer timer = Stats.get().getOpTimer(BenchSSTable.class, "serialization");
            Meter bytesSerialized = Stats.get().getMeter(BenchSSTable.class, "bytes");
            KeyValueIterator iter = new RandomDataIterator(seed, numEntries, entrySize);
            byte[] buffer = new byte[entrySize*2];
            ByteArrayOutputStream bos = new ByteArrayOutputStream(entrySize*2);
            while (iter.hasNext()) {
                bos.reset();
                OpTimer.Ctx ctx = timer.create();
                KeyValue kv = iter.next();
                kv.writeDelimitedTo(bos);
                bytesSerialized.mark(kv.getSerializedSize());
                ctx.success();
            }
        } else if (mode.equals("read")) {
            Comparator<ByteString> comp = KeyComparators.unsignedLexicographical();
            String[] files = cmd.getOptionValues("file");
            final List<KeyValueIterator> iterators = new ArrayList<KeyValueIterator>();
            try {
                for (String f : files) {
                    SSTableImpl sst = SSTableImpl.open(new File(cmd.getOptionValue("file")), comp);
                    iterators.add(sst.iterator());
                }
                KeyValueIterator iter = new DedupeIterator(comp,
                        new MergingIterator(comp, iterators));

                LOG.info("Starting reading");
                while (iter.hasNext()) {
                    iter.next();
                }
                LOG.info("Finished reading");
            } finally {
                for (KeyValueIterator i : iterators) {
                    Closeables.closeQuietly(i);
                }
            }
        } else {
            KeyValueIterator iter = new RandomDataIterator(seed, numEntries, entrySize);
            Comparator<ByteString> comp = KeyComparators.unsignedLexicographical();

            LOG.info("Starting writing");
            SSTableImpl.store(new File(cmd.getOptionValue("file")), comp, iter);
            LOG.info("Finished writing");
        }
    }
}
