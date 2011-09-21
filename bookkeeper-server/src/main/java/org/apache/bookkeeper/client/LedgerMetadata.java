package org.apache.bookkeeper.client;

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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.bookkeeper.util.StringUtils;
import org.apache.log4j.Logger;

/**
 * This class encapsulates all the ledger metadata that is persistently stored
 * in zookeeper. It provides parsing and serialization methods of such metadata.
 *
 */
public class LedgerMetadata {
    static final Logger LOG = Logger.getLogger(LedgerMetadata.class);

    private static final String closed = "CLOSED";
    private static final String inRecovery = "INRECOVERY";

    private static final String lSplitter = "\n";
    private static final String tSplitter = "\t";

    // can't use -1 for NOTCLOSED because that is reserved for a closed, empty
    // ledger
    public static final int NOTCLOSED = -101;
    public static final int IN_RECOVERY = -102;

    public static final int METADATA_FORMAT_VERSION = 1;
    public static final String VERSION_KEY = "BookieMetadataFormatVersion";

    int metadataFormatVersion = 0;

    int ensembleSize;
    int quorumSize;
    long length;
    long close;
    private SortedMap<Long, ArrayList<InetSocketAddress>> ensembles = new TreeMap<Long, ArrayList<InetSocketAddress>>();
    ArrayList<InetSocketAddress> currentEnsemble;
    int znodeVersion = -1;
    
    public LedgerMetadata(int ensembleSize, int quorumSize) {
        this.ensembleSize = ensembleSize;
        this.quorumSize = quorumSize;

        /*
         * It is set in PendingReadOp.readEntryComplete, and
         * we read it in LedgerRecoveryOp.readComplete.
         */
        this.length = 0;
        this.close = NOTCLOSED;
        this.metadataFormatVersion = METADATA_FORMAT_VERSION;
    };

    private LedgerMetadata() {
        this(0, 0);
    }

    /**
     * Get the Map of bookie ensembles for the various ledger fragments
     * that make up the ledger.
     *
     * @return SortedMap of Ledger Fragments and the corresponding
     * bookie ensembles that store the entries.
     */
    public SortedMap<Long, ArrayList<InetSocketAddress>> getEnsembles() {
        return ensembles;
    }

    boolean isClosed() {
        return close != NOTCLOSED 
            && close != IN_RECOVERY;
    }
    
    void markLedgerInRecovery() {
        close = IN_RECOVERY;
    }

    void close(long entryId) {
        close = entryId;
    }

    void addEnsemble(long startEntryId, ArrayList<InetSocketAddress> ensemble) {
        assert ensembles.isEmpty() || startEntryId >= ensembles.lastKey();

        ensembles.put(startEntryId, ensemble);
        currentEnsemble = ensemble;
    }

    ArrayList<InetSocketAddress> getEnsemble(long entryId) {
        // the head map cannot be empty, since we insert an ensemble for
        // entry-id 0, right when we start
        return ensembles.get(ensembles.headMap(entryId + 1).lastKey());
    }

    /**
     * the entry id > the given entry-id at which the next ensemble change takes
     * place ( -1 if no further ensemble changes)
     *
     * @param entryId
     * @return
     */
    long getNextEnsembleChange(long entryId) {
        SortedMap<Long, ArrayList<InetSocketAddress>> tailMap = ensembles.tailMap(entryId + 1);

        if (tailMap.isEmpty()) {
            return -1;
        } else {
            return tailMap.firstKey();
        }
    }

    /**
     * Generates a byte array based on a LedgerConfig object received.
     *
     * @param config
     *            LedgerConfig object
     * @return byte[]
     */
    public byte[] serialize() {
        StringBuilder s = new StringBuilder();
        s.append(VERSION_KEY).append(tSplitter).append(metadataFormatVersion).append(lSplitter);
        s.append(quorumSize).append(lSplitter).append(ensembleSize).append(lSplitter).append(length);

        for (Map.Entry<Long, ArrayList<InetSocketAddress>> entry : ensembles.entrySet()) {
            s.append(lSplitter).append(entry.getKey());
            for (InetSocketAddress addr : entry.getValue()) {
                s.append(tSplitter);
                StringUtils.addrToString(s, addr);
            }
        }

        if (close == IN_RECOVERY) {
            s.append(lSplitter).append(close).append(tSplitter).append(inRecovery);
        } else if (close != NOTCLOSED) {
            s.append(lSplitter).append(close).append(tSplitter).append(closed);
        } 

        if (LOG.isDebugEnabled()) {
            LOG.debug("Serialized config: " + s.toString());
        }

        return s.toString().getBytes();
    }

    /**
     * Parses a given byte array and transforms into a LedgerConfig object
     *
     * @param array
     *            byte array to parse
     * @return LedgerConfig
     * @throws IOException
     *             if the given byte[] cannot be parsed
     */

    static LedgerMetadata parseConfig(byte[] bytes) throws IOException {

        LedgerMetadata lc = new LedgerMetadata();
        String config = new String(bytes);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Parsing Config: " + config);
        }

        String lines[] = config.split(lSplitter);
        
        try {
            int i = 0;
            if (lines[0].startsWith(VERSION_KEY)) {
                String parts[] = lines[0].split(tSplitter);
                lc.metadataFormatVersion = new Integer(parts[1]);
                i++;
            } else {
                lc.metadataFormatVersion = 0;
            }
            if (lc.metadataFormatVersion > METADATA_FORMAT_VERSION) {
                throw new IOException("Metadata format is newer than software can handle");
            }

            if ((lines.length+i) < 2) {
                throw new IOException("Quorum size or ensemble size absent from config: " + config);
            }

            lc.quorumSize = new Integer(lines[i++]);
            lc.ensembleSize = new Integer(lines[i++]);
            lc.length = new Long(lines[i++]);

            for (; i < lines.length; i++) {
                String parts[] = lines[i].split(tSplitter);

                if (parts[1].equals(closed)) {
                    lc.close = new Long(parts[0]);
                    break;
                } else if (parts[1].equals(inRecovery)) {
                    lc.close = IN_RECOVERY;
                    break;
                }

                ArrayList<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();
                for (int j = 1; j < parts.length; j++) {
                    addrs.add(StringUtils.parseAddr(parts[j]));
                }
                lc.addEnsemble(new Long(parts[0]), addrs);
            }
        } catch (NumberFormatException e) {
            throw new IOException(e);
        }
        return lc;
    }
    
    /**
     * Parses a given byte array and transforms into a LedgerConfig object
     *
     * @param array
     *            byte array to parse
     * @para, verion
     *            znode version
     * @return LedgerConfig
     * @throws IOException
     *             if the given byte[] cannot be parsed
     */

    static LedgerMetadata parseConfig(byte[] bytes, int version) throws IOException {
        LedgerMetadata lc = LedgerMetadata.parseConfig(bytes);
        lc.znodeVersion = version;
        
        return lc;
    }

}
