package org.apache.bookkeeper.bookie.lsmindex;

import java.util.SortedSet;
import java.io.IOException;
import java.io.Closeable;
import java.io.File;
import java.util.Comparator;

import java.io.EOFException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.FileOutputStream;

import java.util.TreeSet;

import org.apache.bookkeeper.bookie.lsmindex.Manifest.Entry;
import org.apache.bookkeeper.proto.DataFormats.ManifestMutation;
import org.apache.bookkeeper.proto.DataFormats.ManifestEntry;
import org.apache.bookkeeper.proto.DataFormats.ManifestHeader;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.common.io.Closeables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManifestLog implements Closeable {
    private final static Logger LOG
        = LoggerFactory.getLogger(ManifestLog.class);

    interface LogScanner {
        void apply(Integer level, SortedSet<Entry> oldSet, SortedSet<Entry> newSet)
                throws IOException;
    }

    static void replayLog(Comparator<Entry> entryComparator,
                         File fn, LogScanner scanner) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(fn));
        CodedInputStream cis = CodedInputStream.newInstance(is);
        ManifestHeader.Builder header = ManifestHeader.newBuilder();
        header.mergeDelimitedFrom(is);

        try {
            while (!cis.isAtEnd()) {
                ManifestMutation.Builder mutationBuilder = ManifestMutation.newBuilder();
                ManifestMutation mutation;

                try {
                    int size = cis.readRawVarint32();
                    int oldlimit = cis.pushLimit(size);
                    mutationBuilder.mergeFrom(cis);
                    cis.popLimit(oldlimit);
                    mutation = mutationBuilder.build();
                } catch (InvalidProtocolBufferException ipbe) { 
                    if (!cis.isAtEnd()) {
                        // corrupt, throw an exception
                        throw new IOException("Found corrupt data in log", ipbe);
                    }
                    break;
                }
                TreeSet<Entry> oldSet = new TreeSet<Entry>(entryComparator);
                TreeSet<Entry> newSet = new TreeSet<Entry>(entryComparator);
                for (ManifestEntry e : mutation.getRemovedList()) {
                    oldSet.add(protobufToEntry(e));
                }
                for (ManifestEntry e : mutation.getAddedList()) {
                    newSet.add(protobufToEntry(e));
                }
                scanner.apply(mutation.getLevel(), oldSet, newSet);
            }
        } finally {
            Closeables.closeQuietly(is);
        }
    }

    FileOutputStream os;

    ManifestLog(File fn) throws IOException {
        os = new FileOutputStream(fn);
        ManifestHeader.newBuilder().build().writeDelimitedTo(os);
        os.getChannel().force(true);
    }

    void log(Integer level, SortedSet<Entry> oldSet, SortedSet<Entry> newSet)
            throws IOException {
        ManifestMutation.Builder mutationBuilder = ManifestMutation.newBuilder();
        mutationBuilder.setLevel(level);
        for (Entry e : oldSet) {
            mutationBuilder.addRemoved(entryToProtobuf(e));
        }
        for (Entry e : newSet) {
            mutationBuilder.addAdded(entryToProtobuf(e));
        }
        mutationBuilder.build().writeDelimitedTo(os);
        os.getChannel().force(true);
    }

    public void close() throws IOException {
        os.close();
    }

    static ManifestEntry entryToProtobuf(Manifest.Entry e) throws IOException {
        ManifestEntry.Builder builder = ManifestEntry.newBuilder();
        builder.setLevel(e.getLevel())
            .setFile(e.getFile().getCanonicalPath())
            .setFirstKey(e.getFirstKey())
            .setLastKey(e.getLastKey())
            .setCreationOrder(e.getCreationOrder());
        return builder.build();
    }

    static Manifest.Entry protobufToEntry(ManifestEntry e) {
        return new Manifest.Entry(e.getLevel(), e.getCreationOrder(),
                new File(e.getFile()), e.getFirstKey(), e.getLastKey());
    }
}
