/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.Map;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat;
import org.apache.bookkeeper.versioning.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.google.common.base.Charsets.UTF_8;

/**
 * This class encapsulates all the ledger metadata that is persistently stored
 * in zookeeper. It provides parsing and serialization methods of such metadata.
 *
 */
public class LedgerMetadata {
    static final Logger LOG = LoggerFactory.getLogger(LedgerMetadata.class);

    private static final String closed = "CLOSED";
    private static final String lSplitter = "\n";
    private static final String tSplitter = "\t";

    private static final int IN_RECOVERY = -102;

    public static final int LOWEST_COMPAT_METADATA_FORMAT_VERSION = 0;
    public static final int CURRENT_METADATA_FORMAT_VERSION = 2;
    public static final String VERSION_KEY = "BookieMetadataFormatVersion";

    private final int metadataFormatVersion;

    private final int ensembleSize;
    private final int writeQuorumSize;
    private final int ackQuorumSize;
    private final long length;
    private final long lastEntryId;

    private final LedgerMetadataFormat.State state;
    private final ImmutableSortedMap<Long, ImmutableList<BookieSocketAddress>> ensembles;
    private final Version version;

    private final boolean hasPassword;
    private final LedgerMetadataFormat.DigestType digestType;
    private final ByteString password;

    public LedgerMetadata(Builder builder) {
        assert(builder.metadataFormatVersion != UNINITIALIZED);
        assert(builder.ensembleSize != UNINITIALIZED);
        assert(builder.writeQuorumSize != UNINITIALIZED);
        assert(builder.ackQuorumSize != UNINITIALIZED);
        assert(builder.length != UNINITIALIZED);
        assert(builder.lastEntryId != UNINITIALIZED);
        assert(builder.ensembles.size() > 0);

        this.metadataFormatVersion = builder.metadataFormatVersion;
        this.ensembleSize = builder.ensembleSize;
        this.writeQuorumSize = builder.writeQuorumSize;
        this.ackQuorumSize = builder.ackQuorumSize;
        this.length = builder.length;
        this.lastEntryId = builder.lastEntryId;
        this.ensembles = builder.ensembles;
        this.state = builder.state;
        this.version = builder.version;
        this.password = builder.password;
        this.hasPassword = builder.hasPassword;
        this.digestType = builder.digestType;
    }

    /**
     * Get the Map of bookie ensembles for the various ledger fragments
     * that make up the ledger.
     *
     * @return SortedMap of Ledger Fragments and the corresponding
     * bookie ensembles that store the entries.
     */
    public ImmutableSortedMap<Long, ImmutableList<BookieSocketAddress>> getEnsembles() {
        return ensembles;
    }

    public int getEnsembleSize() {
        return ensembleSize;
    }

    public int getWriteQuorumSize() {
        return writeQuorumSize;
    }

    public int getAckQuorumSize() {
        return ackQuorumSize;
    }

    /**
     * In versions 4.1.0 and below, the digest type and password were not
     * stored in the metadata.
     *
     * @return whether the password has been stored in the metadata
     */
    boolean hasPassword() {
        return hasPassword;
    }

    byte[] getPassword() {
        return password.toByteArray();
    }

    BookKeeper.DigestType getDigestType() {
        if (digestType.equals(LedgerMetadataFormat.DigestType.HMAC)) {
            return BookKeeper.DigestType.MAC;
        } else {
            return BookKeeper.DigestType.CRC32;
        }
    }

    public long getLastEntryId() {
        return lastEntryId;
    }

    public long getLength() {
        return length;
    }

    public boolean isClosed() {
        return state == LedgerMetadataFormat.State.CLOSED;
    }

    public boolean isInRecovery() {
        return state == LedgerMetadataFormat.State.IN_RECOVERY;
    }

    LedgerMetadataFormat.State getState() {
        return state;
    }

    ImmutableList<BookieSocketAddress> getEnsemble(long entryId) {
        // the head map cannot be empty, since we insert an ensemble for
        // entry-id 0, right when we start
        return ensembles.get(ensembles.headMap(entryId + 1).lastKey());
    }

    ImmutableList<BookieSocketAddress> getCurrentEnsemble() {
        return ensembles.get(ensembles.lastKey());
    }

    /**
     * the entry id greater than the given entry-id at which the next ensemble change takes
     * place
     *
     * @param entryId
     * @return the entry id of the next ensemble change (-1 if no further ensemble changes) 
     */
    long getNextEnsembleChange(long entryId) {
        ImmutableSortedMap<Long, ImmutableList<BookieSocketAddress>> tailMap
            = ensembles.tailMap(entryId + 1);

        if (tailMap.isEmpty()) {
            return -1;
        } else {
            return tailMap.firstKey();
        }
    }

    /**
     * Generates a byte array of this object
     *
     * @return the metadata serialized into a byte array
     */
    public byte[] serialize() {
        if (metadataFormatVersion == 1) {
            return serializeVersion1();
        }
        LedgerMetadataFormat.Builder builder = LedgerMetadataFormat.newBuilder();
        builder.setQuorumSize(writeQuorumSize).setAckQuorumSize(ackQuorumSize)
            .setEnsembleSize(ensembleSize).setLength(length)
            .setState(state).setLastEntryId(lastEntryId);

        if (hasPassword) {
            builder.setDigestType(digestType).setPassword(password);
        }

        for (Map.Entry<Long, ImmutableList<BookieSocketAddress>> entry
                 : ensembles.entrySet()) {
            LedgerMetadataFormat.Segment.Builder segmentBuilder = LedgerMetadataFormat.Segment.newBuilder();
            segmentBuilder.setFirstEntryId(entry.getKey());
            for (BookieSocketAddress addr : entry.getValue()) {
                segmentBuilder.addEnsembleMember(addr.toString());
            }
            builder.addSegment(segmentBuilder.build());
        }

        StringBuilder s = new StringBuilder();
        s.append(VERSION_KEY).append(tSplitter).append(CURRENT_METADATA_FORMAT_VERSION).append(lSplitter);
        s.append(TextFormat.printToString(builder.build()));
        LOG.debug("Serialized config: {}", s);
        return s.toString().getBytes(UTF_8);
    }

    private byte[] serializeVersion1() {
        StringBuilder s = new StringBuilder();
        s.append(VERSION_KEY).append(tSplitter).append(metadataFormatVersion).append(lSplitter);
        s.append(writeQuorumSize).append(lSplitter).append(ensembleSize).append(lSplitter).append(length);

        for (Map.Entry<Long, ImmutableList<BookieSocketAddress>> entry
                 : ensembles.entrySet()) {
            s.append(lSplitter).append(entry.getKey());
            for (BookieSocketAddress addr : entry.getValue()) {
                s.append(tSplitter);
                s.append(addr.toString());
            }
        }

        if (state == LedgerMetadataFormat.State.IN_RECOVERY) {
            s.append(lSplitter).append(IN_RECOVERY).append(tSplitter).append(closed);
        } else if (state == LedgerMetadataFormat.State.CLOSED) {
            s.append(lSplitter).append(getLastEntryId()).append(tSplitter).append(closed);
        }

        LOG.debug("Serialized config: {}", s);

        return s.toString().getBytes(UTF_8);
    }

    /**
     * Parses a given byte array and transforms into a LedgerConfig object
     *
     * @param bytes
     *            byte array to parse
     * @param version
     *            version of the ledger metadata
     * @return LedgerConfig
     * @throws IOException
     *             if the given byte[] cannot be parsed
     */
    public static LedgerMetadata parseConfig(byte[] bytes, Version version) throws IOException {
        Builder builder = new Builder();
        builder.setVersion(version);

        String config = new String(bytes, UTF_8);

        LOG.debug("Parsing Config: {}", config);
        BufferedReader reader = new BufferedReader(new StringReader(config));
        String versionLine = reader.readLine();
        if (versionLine == null) {
            throw new IOException("Invalid metadata. Content missing");
        }
        if (versionLine.startsWith(VERSION_KEY)) {
            String parts[] = versionLine.split(tSplitter);
            builder.setMetadataFormatVersion(new Integer(parts[1]));
        } else {
            // if no version is set, take it to be version 1
            // as the parsing is the same as what we had before
            // we introduce versions
            builder.setMetadataFormatVersion(1);
            // reset the reader
            reader.close();
            reader = new BufferedReader(new StringReader(config));
        }

        if (builder.metadataFormatVersion < LOWEST_COMPAT_METADATA_FORMAT_VERSION
            || builder.metadataFormatVersion > CURRENT_METADATA_FORMAT_VERSION) {
            throw new IOException("Metadata version not compatible. Expected between "
                    + LOWEST_COMPAT_METADATA_FORMAT_VERSION + " and " + CURRENT_METADATA_FORMAT_VERSION
                                  + ", but got " + builder.metadataFormatVersion);
        }

        if (builder.metadataFormatVersion == 1) {
            return parseVersion1Config(builder, reader).build();
        }

        LedgerMetadataFormat.Builder pbbuilder = LedgerMetadataFormat.newBuilder();
        TextFormat.merge(reader, pbbuilder);
        LedgerMetadataFormat data = pbbuilder.build();
        builder.setWriteQuorumSize(data.getQuorumSize());
        if (data.hasAckQuorumSize()) {
            builder.setAckQuorumSize(data.getAckQuorumSize());
        } else {
            builder.setAckQuorumSize(data.getQuorumSize());
        }

        builder.setEnsembleSize(data.getEnsembleSize());
        builder.setLength(data.getLength());
        builder.setState(data.getState());
        builder.setLastEntryId(data.getLastEntryId());

        if (data.hasPassword()) {
            builder.setDigestType(data.getDigestType());
            builder.setPassword(data.getPassword());
        }

        ImmutableSortedMap.Builder<Long, ImmutableList<BookieSocketAddress>> ensembles
            = ImmutableSortedMap.naturalOrder();
        for (LedgerMetadataFormat.Segment s : data.getSegmentList()) {
            ImmutableList.Builder<BookieSocketAddress> addrs
                = ImmutableList.builder();
            for (String member : s.getEnsembleMemberList()) {
                addrs.add(new BookieSocketAddress(member));
            }
            ensembles.put(s.getFirstEntryId(), addrs.build());
        }
        builder.setEnsembles(ensembles.build());
        return builder.build();
    }

    static Builder parseVersion1Config(Builder builder,
                                       BufferedReader reader) throws IOException {
        try {
            int quorum = new Integer(reader.readLine());
            builder.setWriteQuorumSize(quorum).setAckQuorumSize(quorum);
            builder.setEnsembleSize(new Integer(reader.readLine()));
            builder.setLength(new Long(reader.readLine()));

            String line = reader.readLine();
            ImmutableSortedMap.Builder<Long, ImmutableList<BookieSocketAddress>> ensembles
                = ImmutableSortedMap.naturalOrder();
            while (line != null) {
                String parts[] = line.split(tSplitter);

                if (parts[1].equals(closed)) {
                    Long l = new Long(parts[0]);
                    if (l == IN_RECOVERY) {
                        builder.setState(LedgerMetadataFormat.State.IN_RECOVERY);
                    } else {
                        builder.setState(LedgerMetadataFormat.State.CLOSED);
                        builder.setLastEntryId(l);
                    }
                    break;
                } else {
                    builder.setState(LedgerMetadataFormat.State.OPEN);
                }

                ImmutableList.Builder<BookieSocketAddress> addrs
                    = ImmutableList.builder();
                for (int j = 1; j < parts.length; j++) {
                    addrs.add(new BookieSocketAddress(parts[j]));
                }
                ensembles.put(new Long(parts[0]), addrs.build());
                line = reader.readLine();
            }
            builder.setEnsembles(ensembles.build());
        } catch (NumberFormatException e) {
            throw new IOException(e);
        }
        return builder;
    }

    /**
     * Returns the last version.
     *
     * @return version
     */
    public Version getVersion() {
        return this.version;
    }

    /**
     * Is the metadata newer than given <i>newMeta</i>.
     *
     * @param newMeta the metadata to compare
     * @return true if <i>this</i> is newer than <i>newMeta</i>, false otherwise
     */
    boolean isNewerThan(LedgerMetadata newMeta) {
        if (null == version) {
            return false;
        }
        return Version.Occurred.AFTER == version.compare(newMeta.version);
    }

    /**
     * Is the metadata conflict with new updated metadata.
     *
     * @param newMeta
     *          Re-read metadata
     * @return true if the metadata is conflict.
     */
    boolean isConflictWith(LedgerMetadata newMeta) {
        /*
         *  if length & close have changed, then another client has
         *  opened the ledger, can't resolve this conflict.
         */

        if (metadataFormatVersion != newMeta.metadataFormatVersion ||
            ensembleSize != newMeta.ensembleSize ||
            writeQuorumSize != newMeta.writeQuorumSize ||
            ackQuorumSize != newMeta.ackQuorumSize ||
            length != newMeta.length ||
            !digestType.equals(newMeta.digestType) ||
            !password.equals(newMeta.password)) {
            return true;
        }
        if (state == LedgerMetadataFormat.State.CLOSED
            && lastEntryId != newMeta.lastEntryId) {
            return true;
        }
        // if ledger is closed, we can just take the new ensembles
        if (newMeta.state != LedgerMetadataFormat.State.CLOSED) {
            // allow new metadata to be one ensemble less than current metadata
            // since ensemble change might kick in when recovery changed metadata
            int diff = ensembles.size() - newMeta.ensembles.size();
            if (0 != diff && 1 != diff) {
                return true;
            }
            // ensemble distribution should be same
            // we don't check the detail ensemble, since new bookie will be set
            // using recovery tool.
            Iterator<Long> keyIter = ensembles.keySet().iterator();
            Iterator<Long> newMetaKeyIter = newMeta.ensembles.keySet().iterator();
            for (int i=0; i<newMeta.ensembles.size(); i++) {
                Long curKey = keyIter.next();
                Long newMetaKey = newMetaKeyIter.next();
                if (!curKey.equals(newMetaKey)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(meta:").append(new String(serialize(), UTF_8)).append(", version:").append(version).append(")");
        return sb.toString();
    }


    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder copyFrom(LedgerMetadata other) {
        return new Builder()
            .setEnsembleSize(other.ensembleSize)
            .setWriteQuorumSize(other.writeQuorumSize)
            .setAckQuorumSize(other.ackQuorumSize)
            .setLength(other.length)
            .setLastEntryId(other.lastEntryId)
            .setMetadataFormatVersion(other.metadataFormatVersion)
            .setState(other.state)
            .setVersion(other.version)
            .setDigestType(other.digestType)
            .setPassword(other.password)
            .setEnsembles(other.ensembles);
    }

    private final static int UNINITIALIZED = -15421;
    public static class Builder {
        int metadataFormatVersion = CURRENT_METADATA_FORMAT_VERSION;
        int ensembleSize = UNINITIALIZED;
        int writeQuorumSize = UNINITIALIZED;
        int ackQuorumSize = UNINITIALIZED;
        long length = 0;
        long lastEntryId = LedgerHandle.INVALID_ENTRY_ID;

        LedgerMetadataFormat.State state = LedgerMetadataFormat.State.OPEN;
        ImmutableSortedMap<Long, ImmutableList<BookieSocketAddress>> ensembles
            = ImmutableSortedMap.<Long, ImmutableList<BookieSocketAddress>>naturalOrder().build();

        Version version = Version.NEW;

        boolean hasPassword = false;
        LedgerMetadataFormat.DigestType digestType = LedgerMetadataFormat.DigestType.HMAC;
        ByteString password = ByteString.EMPTY;

        public Builder setMetadataFormatVersion(int version) {
            this.metadataFormatVersion = version;
            return this;
        }

        public Builder setEnsembleSize(int ensembleSize) {
            this.ensembleSize = ensembleSize;
            return this;
        }

        public Builder setWriteQuorumSize(int writeQuorumSize) {
            this.writeQuorumSize = writeQuorumSize;
            return this;
        }

        public Builder setAckQuorumSize(int ackQuorumSize) {
            this.ackQuorumSize = ackQuorumSize;
            return this;
        }

        public Builder setLength(long length) {
            this.length = length;
            return this;
        }

        public Builder setLastEntryId(long lastEntryId) {
            this.lastEntryId = lastEntryId;
            return this;
        }

        public Builder closeLedger(long lastEntryId) {
            return setLastEntryId(lastEntryId)
                .setState(LedgerMetadataFormat.State.CLOSED);
        }

        public Builder setState(LedgerMetadataFormat.State state) {
            this.state = state;
            return this;
        }

        public Builder setEnsembles(ImmutableSortedMap<Long, ImmutableList<BookieSocketAddress>> ensembles) {
            this.ensembles = ensembles;
            return this;
        }

        public Builder markLedgerInRecovery() {
            return setState(LedgerMetadataFormat.State.IN_RECOVERY);
        }

        public Builder setVersion(Version version) {
            this.version = version;
            return this;
        }

        public Builder setPassword(byte[] password) {
            return setPassword(ByteString.copyFrom(password));
        }

        public Builder setPassword(ByteString password) {
            this.password = password;
            this.hasPassword = true;
            return this;
        }

        public Builder setDigestType(BookKeeper.DigestType type) {
            if (type == BookKeeper.DigestType.MAC) {
                return setDigestType(LedgerMetadataFormat.DigestType.HMAC);
            } else if (type == BookKeeper.DigestType.CRC32) {
                return setDigestType(LedgerMetadataFormat.DigestType.CRC32);
            } else {
                throw new IllegalArgumentException("Unknown digestType: " + type);
            }
        }

        public Builder setDigestType(LedgerMetadataFormat.DigestType digestType) {
            this.digestType = digestType;
            return this;
        }

        public LedgerMetadata build() {
            return new LedgerMetadata(this);
        }
    }
}
