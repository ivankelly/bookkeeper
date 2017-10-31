/*
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

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.bookkeeper.client.LedgerMetadata.areByteArrayValMapsEqual;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Map;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.State;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.junit.Test;

/**
 * Unit test of {@link LedgerMetadata}.
 */
public class TestLedgerMetadata {

    private static ArrayList<BookieSocketAddress> createServers(int numBookies) {
        ArrayList<BookieSocketAddress> servers = Lists.newArrayListWithExpectedSize(numBookies);
        int basePort = 3181;
        for (int i = 0; i < numBookies; i++) {
            servers.add(new BookieSocketAddress("127.0.0.1", basePort + i));
        }
        return servers;
    }

    private final int ensembleSize;
    private final int writeQuorumSize;
    private final int ackQuorumSize;
    private final DigestType digestType;
    private final byte[] passwd = "test-password".getBytes(UTF_8);
    private final Map<String, byte[]> customMetadata = Maps.newHashMap();
    private final LedgerMetadata metadata;
    private final LongVersion version;
    private final ArrayList<BookieSocketAddress> ensemble;

    public TestLedgerMetadata() {
        this.ensembleSize = 5;
        this.writeQuorumSize = 3;
        this.ackQuorumSize = 2;
        this.digestType = DigestType.CRC32;
        this.customMetadata.put(
            "test-key",
            "test-value".getBytes(UTF_8));
        this.version = new LongVersion(System.currentTimeMillis());
        this.metadata = new LedgerMetadata(
            ensembleSize,
            writeQuorumSize,
            ackQuorumSize,
            digestType,
            passwd,
            customMetadata);
        this.metadata.setVersion(version);
        this.ensemble = createServers(5);
        this.metadata.addEnsemble(0L, ensemble);
    }

    private void verifyLedgerMetadata(LedgerMetadata deserializedMetadata) {
        assertEquals(ensembleSize, deserializedMetadata.getEnsembleSize());
        assertEquals(writeQuorumSize, deserializedMetadata.getWriteQuorumSize());
        assertEquals(ackQuorumSize, deserializedMetadata.getAckQuorumSize());
        assertEquals(DigestType.CRC32, deserializedMetadata.getDigestType());
        assertArrayEquals(passwd, deserializedMetadata.getPassword());
        areByteArrayValMapsEqual(this.customMetadata, deserializedMetadata.getCustomMetadata());
        assertEquals(version, deserializedMetadata.getVersion());
        assertEquals(1, deserializedMetadata.getEnsembles().size());
        assertEquals(ensemble, deserializedMetadata.getEnsembles().get(0L));
    }

    @Test
    public void testSerDeBytes() throws Exception {
        byte[] serializedMetadata = metadata.serialize();
        LedgerMetadata deserializedMetadata =
            LedgerMetadata.parseConfig(serializedMetadata, version, Optional.absent());
        verifyLedgerMetadata(deserializedMetadata);
    }

    @Test
    public void testCopyConstructor() throws Exception {
        LedgerMetadata copiedMetadata = new LedgerMetadata(metadata);
        verifyLedgerMetadata(copiedMetadata);
    }

    @Test
    public void testSerDeProto() throws Exception {
        LedgerMetadataFormat format = metadata.toProtoFormat();
        LedgerMetadata deserializedMetadata = LedgerMetadata.fromLedgerMetadataFormat(
            format,
            version,
            Optional.absent());
        verifyLedgerMetadata(deserializedMetadata);
    }

    @Test
    public void testIsNewerThan() throws Exception {
        LedgerMetadata newMetadadata = new LedgerMetadata(metadata);
        Version version = new LongVersion(this.version.getLongVersion() + 1);
        newMetadadata.setVersion(version);
        assertTrue(newMetadadata.isNewerThan(metadata));
    }

    @Test
    public void testClose() throws Exception {
        assertEquals(-1L, metadata.getLastEntryId());
        assertEquals(State.OPEN, metadata.getState());
        assertFalse(metadata.isClosed());
        metadata.close(1000L);
        assertEquals(1000L, metadata.getLastEntryId());
        assertEquals(State.CLOSED, metadata.getState());
        assertTrue(metadata.isClosed());
    }

}
