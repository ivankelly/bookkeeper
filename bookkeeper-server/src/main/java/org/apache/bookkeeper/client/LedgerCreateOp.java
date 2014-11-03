/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.bookkeeper.client;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import java.security.GeneralSecurityException;
import java.util.List;
import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.meta.LedgerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates asynchronous ledger create operation
 *
 */
class LedgerCreateOp implements GenericCallback<LedgerManager.LedgerIdAndMetadataVersion> {

    static final Logger LOG = LoggerFactory.getLogger(LedgerCreateOp.class);

    final CreateCallback cb;
    LedgerMetadata metadata = null;
    final LedgerMetadata.Builder metadataBuilder;
    final int ensembleSize;
    final int writeQuorumSize;
    LedgerHandle lh;
    final Object ctx;
    final byte[] passwd;
    final BookKeeper bk;
    final DigestType digestType;
    final long startTime;
    final OpStatsLogger createOpLogger;

    /**
     * Constructor
     *
     * @param bk
     *       BookKeeper object
     * @param ensembleSize
     *       ensemble size
     * @param quorumSize
     *       quorum size
     * @param digestType
     *       digest type, either MAC or CRC32
     * @param passwd
     *       passowrd
     * @param cb
     *       callback implementation
     * @param ctx
     *       optional control object
     */

    LedgerCreateOp(BookKeeper bk, int ensembleSize,
                   int writeQuorumSize, int ackQuorumSize,
                   DigestType digestType,
                   byte[] passwd, CreateCallback cb, Object ctx) {
        this.bk = bk;
        this.metadataBuilder = LedgerMetadata.newBuilder().setEnsembleSize(ensembleSize)
            .setWriteQuorumSize(writeQuorumSize).setAckQuorumSize(ackQuorumSize)
            .setDigestType(digestType).setPassword(passwd);
        this.ensembleSize = ensembleSize;
        this.writeQuorumSize = writeQuorumSize;
        this.digestType = digestType;
        this.passwd = passwd;
        this.cb = cb;
        this.ctx = ctx;
        this.startTime = MathUtils.nowInNano();
        this.createOpLogger = bk.getCreateOpLogger();
    }

    /**
     * Initiates the operation
     */
    public void initiate() {
        // allocate ensemble first

        /*
         * Adding bookies to ledger handle
         */
        List<BookieSocketAddress> ensemble;
        try {
            ensemble = bk.bookieWatcher.newEnsemble(ensembleSize, writeQuorumSize);
        } catch (BKNotEnoughBookiesException e) {
            LOG.error("Not enough bookies to create ledger");
            createComplete(e.getCode(), null);
            return;
        }

        /*
         * Add ensemble to the configuration
         */
        ImmutableSortedMap.Builder<Long, ImmutableList<BookieSocketAddress>> ensBuilder
            = ImmutableSortedMap.<Long, ImmutableList<BookieSocketAddress>>naturalOrder();
        ensBuilder.put(0L, ImmutableList.copyOf(ensemble));

        metadata = metadataBuilder.setEnsembles(ensBuilder.build()).build();

        // create a ledger with metadata
        bk.getLedgerManager().createLedger(metadata, this);
    }

    /**
     * Callback when created ledger.
     */
    @Override
    public void operationComplete(int rc, LedgerManager.LedgerIdAndMetadataVersion idAndVersion) {
        if (BKException.Code.OK != rc) {
            createComplete(rc, null);
            return;
        }

        LedgerMetadata newMetadata = LedgerMetadata.copyFrom(metadata)
            .setVersion(idAndVersion.getMetadataVersion()).build();
        try {
            lh = new LedgerHandle(bk, idAndVersion.getLedgerId(), newMetadata, digestType, passwd);
        } catch (GeneralSecurityException e) {
            LOG.error("Security exception while creating ledger: " + idAndVersion.getLedgerId(), e);
            createComplete(BKException.Code.DigestNotInitializedException, null);
            return;
        } catch (NumberFormatException e) {
            LOG.error("Incorrectly entered parameter throttle: " + bk.getConf().getThrottleValue(), e);
            createComplete(BKException.Code.IncorrectParameterException, null);
            return;
        }
        // return the ledger handle back
        createComplete(BKException.Code.OK, lh);
    }

    private void createComplete(int rc, LedgerHandle lh) {
        // Opened a new ledger
        if (BKException.Code.OK != rc) {
            createOpLogger.registerFailedEvent(MathUtils.elapsedMSec(startTime));
        } else {
            createOpLogger.registerSuccessfulEvent(MathUtils.elapsedMSec(startTime));
        }
        cb.createComplete(rc, lh, ctx);
    }

}
