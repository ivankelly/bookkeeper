package org.apache.bookkeeper.bookie.lsmindex;

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Comparator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.FileChannel;

import com.google.common.primitives.Ints;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ByteString;

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

    static void store(File fn, KeyValueIterator kvs) throws IOException {
        FileOutputStream os = new FileOutputStream(fn);
        FileChannel fc = os.getChannel();
        Map<ByteString,Long> index = new HashMap<ByteString,Long>();
        try {
            ByteString lastKey = ByteString.EMPTY;
            long lastBlockStart = -1;
            while (kvs.hasNext()) {
                KeyValue kv = kvs.next();
                if (lastBlockStart == -1
                    || (fc.position() - lastBlockStart) > BLOCKSIZE) {
                    lastBlockStart = fc.position();
                    index.put(kv.getKey(), lastBlockStart);
                    LOG.debug("Adding {} ({}) {} to index",
                            new Object[] { kv.getKey().toByteArray(),
                                           Ints.fromByteArray(kv.getKey().toByteArray()),
                                           lastBlockStart });
                }
                lastKey = kv.getKey();

                kv.writeDelimitedTo(os);
            }
            long indexOffset = fc.position();
            writeIndex(os, index);
            Metadata md = Metadata.newBuilder()
                .setVersion(SSTABLE_FORMAT_CURRENT_VERSION)
                .setCanary(SSTABLE_CANARY)
                .setLastKey(lastKey)
                .setIndexOffset(indexOffset).build();
            writeFooter(os, md);
        } finally {
            os.close();
        }
    }

    static void writeIndex(FileOutputStream os, Map<ByteString,Long> index)
            throws IOException {
        for (Map.Entry<ByteString,Long> i : index.entrySet()) {
            IndexEntry e = IndexEntry.newBuilder().setKey(i.getKey())
                .setOffset(i.getValue()).build();
            LOG.info("Writing entry {} {}", e.getKey().toByteArray(), e.getOffset());
            e.writeDelimitedTo(os);
        }
    }

    static void writeFooter(FileOutputStream os, Metadata md)
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
                LOG.debug("Reading index entry {} {}", e.getKey().toByteArray(), e.getOffset());
            }
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

        return new KeyValueIterator() {
            KeyValue curKV;

            @Override
            public synchronized boolean hasNext() throws IOException {
                while (curKV == null) {
                    if (s.getChannel().position() >= meta.getIndexOffset()) {
                        curKV = null;
                        return false;
                    }

                    try {
                        KeyValue.Builder builder = KeyValue.newBuilder();
                        builder.mergeDelimitedFrom(s);
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

    private ByteString getFirstKey() {
        if (index.size() > 0) {
            return index.firstKey();
        } else {
            return getLastKey(); // sstable is empty
        }
    }

    private ByteString getLastKey() {
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
