package org.apache.bookkeeper.bookie.lsmindex;

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Comparator;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import java.io.IOException;
import java.io.File;
import java.io.OutputStream;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.FileChannel;

import com.google.common.io.LimitInputStream;
import com.google.common.primitives.Ints;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ByteString;

import org.apache.bookkeeper.stats.Stats;
import org.apache.bookkeeper.stats.Meter;
import org.apache.bookkeeper.stats.OpTimer;
import org.apache.bookkeeper.proto.DataFormats.Metadata;
import org.apache.bookkeeper.proto.DataFormats.KeyValue;
import org.apache.bookkeeper.proto.DataFormats.IndexEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSTableImpl {
    private final static Logger LOG = LoggerFactory.getLogger(SSTableImpl.class);

    private static final long SSTABLE_CANARY = 0xDEADBEEFCAFEBABEL;
    private static final int SSTABLE_FORMAT_CURRENT_VERSION = 1;
    private static final int BLOCKSIZE = 128*1024; // think about this more
    private static final int FOOTERSIZE = 128*1024; // think about this more

    static SSTableImpl store(File fn, Comparator<ByteString> comparator,
                             KeyValueIterator kvs) throws IOException {
        Meter byteMeter = Stats.get().getMeter(SSTableImpl.class, "bytes-written");
        Meter entryMeter = Stats.get().getMeter(SSTableImpl.class, "entries-written");
        FileOutputStream os = new FileOutputStream(fn);

        FileChannel fc = os.getChannel();
        NavigableMap<ByteString,Long> index = new TreeMap<ByteString,Long>(comparator);

        try {
            ByteString lastKey = ByteString.EMPTY;
            long lastBlockStart = -1;

            ByteArrayOutputStream bos = new ByteArrayOutputStream(BLOCKSIZE);
            int entryCount = 0;
            while (kvs.hasNext()) {
                KeyValue kv = kvs.next();
                if (lastBlockStart == -1
                    || (bos.size() + kv.getSerializedSize() + 4) >  BLOCKSIZE) {
                    bos.writeTo(os);

                    byteMeter.mark(bos.size());
                    bos.reset();

                    entryMeter.mark(entryCount);
                    entryCount = 0;

                    lastBlockStart = fc.position();
                    index.put(kv.getKey(), lastBlockStart);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Adding {} ({}) {} to index",
                                  new Object[] { kv.getKey().toByteArray(),
                                                 Ints.fromByteArray(kv.getKey().toByteArray()),
                                                 lastBlockStart });
                    }
                }
                lastKey = kv.getKey();

                kv.writeDelimitedTo(bos);
                entryCount++;
            }
            bos.writeTo(os);

            long indexOffset = fc.position();
            writeIndex(os, index);
            Metadata md = Metadata.newBuilder()
                .setVersion(SSTABLE_FORMAT_CURRENT_VERSION)
                .setCanary(SSTABLE_CANARY)
                .setLastKey(lastKey)
                .setIndexOffset(indexOffset).build();
            writeFooter(os, md);
            bos.flush();
            return new SSTableImpl(fn, md, index, comparator);
        } finally {
            os.close();
        }
    }

    static void writeIndex(OutputStream os, Map<ByteString,Long> index)
            throws IOException {
        for (Map.Entry<ByteString,Long> i : index.entrySet()) {
            IndexEntry e = IndexEntry.newBuilder().setKey(i.getKey())
                .setOffset(i.getValue()).build();
            LOG.debug("Writing entry {} {}", e.getKey().toByteArray(), e.getOffset());
            e.writeDelimitedTo(os);
        }
    }

    static void writeFooter(OutputStream os, Metadata md)
            throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        md.writeDelimitedTo(bos);
        os.write(Arrays.copyOf(bos.toByteArray(), FOOTERSIZE));
    }

    final File file;
    final Metadata meta;
    final NavigableMap<ByteString,Long> index;
    final Comparator<ByteString> comparator;

    private SSTableImpl(File file, Metadata meta,
                        NavigableMap<ByteString,Long> index,
                        Comparator<ByteString> comparator) {
        this.file = file;
        this.meta = meta;
        this.index = index;
        this.comparator = comparator;
    }

    static SSTableImpl open(File file, Comparator<ByteString> comparator) throws IOException {
        FileInputStream s = new FileInputStream(file);
        Metadata meta;
        NavigableMap<ByteString,Long> index;

        OpTimer openTimer = Stats.get().getOpTimer(SSTableImpl.class, "open");
        OpTimer.Ctx timer = openTimer.create();
        try {
            long metaPosition = file.length() - FOOTERSIZE;
            s.getChannel().position(metaPosition);

            Metadata.Builder builder = Metadata.newBuilder();
            builder.mergeDelimitedFrom(s);
            meta = builder.build();

            if (meta.getCanary() != SSTABLE_CANARY) {
                throw new IOException("SSTable corrupt");
            }

            s.getChannel().position(meta.getIndexOffset());
            index = new ConcurrentSkipListMap<ByteString,Long>(comparator);
            while (s.getChannel().position() < metaPosition) {
                IndexEntry.Builder builder2 = IndexEntry.newBuilder();
                builder2.mergeDelimitedFrom(s);
                IndexEntry e = builder2.build();
                index.put(e.getKey(), e.getOffset());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Reading index entry {} {}", e.getKey().toByteArray(), e.getOffset());
                }
            }
            timer.success();
        } finally {
            s.close();
        }
        return new SSTableImpl(file, meta, index, comparator);
    }

    KeyValueIterator iterator() throws IOException {
        return iterator(getFirstKey(), getLastKey());
    }

    KeyValueIterator iterator(final ByteString firstKey,
                              final ByteString lastKey) throws IOException {
        if (!mayContain(firstKey)) {
            return nullIterator;
        }
        Map.Entry<ByteString,Long> offset = index.floorEntry(firstKey);
        if (offset == null) {
            throw new IllegalStateException("offset must exist in the index."
                                            + "If not, mayContain should have failed");
        }
        LOG.debug("Found index entry {} reading from {}",
                  offset.getKey().toByteArray(), offset.getValue());

        final FileInputStream s = new FileInputStream(file);
        s.getChannel().position(offset.getValue());
        long limit = meta.getIndexOffset() - offset.getValue();
        final LimitInputStream is = new LimitInputStream(new BufferedInputStream(s, BLOCKSIZE), limit);

        return new KeyValueIterator() {
            KeyValue curKV;

            @Override
            public synchronized boolean hasNext() throws IOException {
                while (curKV == null) {
                    try {
                        KeyValue.Builder builder = KeyValue.newBuilder();
                        if (!builder.mergeDelimitedFrom(is)) {
                            // false means end of stream
                            return false;
                        }
                        curKV = builder.build();
                    } catch (IOException ioe) {
                        LOG.error("Exception reading from sstable", ioe);
                        return false;
                    }
                    if (comparator.compare(curKV.getKey(), firstKey) < 0) {
                        curKV = null;
                        continue;
                    }
                    if (comparator.compare(curKV.getKey(), lastKey) > 0) {
                        curKV = null;
                        return false;
                    }
                }
                return true;
            }

            @Override
            public synchronized KeyValue peek() throws IOException {
                KeyValue kv = curKV;
                if (kv == null) {
                    throw new IOException("No next entry");
                }
                return kv;
            }

            @Override
            public synchronized KeyValue next() throws IOException {
                KeyValue kv = curKV;
                if (kv == null) {
                    throw new IOException("No next entry");
                }
                curKV = null;
                return kv;
            }

            @Override
            public void close() throws IOException {
                s.close();
            }
        };
    }

    boolean mayContain(ByteString key) {
        return comparator.compare(key, getFirstKey()) >= 0
            && comparator.compare(key, getLastKey()) <= 0;
    }

    ByteString getFirstKey() {
        if (index.size() > 0) {
            return index.firstKey();
        } else {
            return getLastKey(); // sstable is empty
        }
    }

    ByteString getLastKey() {
        return meta.getLastKey();
    }

    private static final KeyValueIterator nullIterator = new KeyValueIterator() {
            @Override
            public boolean hasNext() throws IOException {
                return false;
            }

            @Override
            public KeyValue peek() throws IOException {
                return next();
            }

            @Override
            public KeyValue next() throws IOException {
                throw new IOException("Null iterator, there are no entries");
            }

            @Override
            public void close() throws IOException {
            }
        };
}
