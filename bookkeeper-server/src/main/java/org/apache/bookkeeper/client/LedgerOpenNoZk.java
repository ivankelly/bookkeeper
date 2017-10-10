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

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.security.GeneralSecurityException;

import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.OrderedSafeExecutor.OrderedSafeGenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.TimedGenericCallback;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the ledger open operation
 *
 */
class LedgerOpenOpNoZk {
    static final Logger LOG = LoggerFactory.getLogger(LedgerOpenOp.class);

    final BookKeeper bk;
    final long ledgerId;
    final OpenCallback cb;
    final LedgerMetadata metadata;
    final Object ctx;
    LedgerHandle lh;
    PaxosClient paxos;

    long startTime;
    OpStatsLogger openOpLogger;

    /**
     * Constructor.
     *
     * @param bk
     * @param ledgerId
     * @param digestType. Ignored if conf.getEnableDigestTypeAutodetection() is true
     * @param passwd
     * @param cb
     * @param ctx
     */
    public LedgerOpenOpNoZk(BookKeeper bk, long ledgerId,
                            LedgerMetadata metadata,
                            OpenCallback cb, Object ctx) {
        this.bk = bk;
        this.ledgerId = ledgerId;
        this.metadata = metadata;
        this.cb = cb;
        this.ctx = ctx;
    }

    /**
     * Inititates the ledger open operation
     */
    public void initiate() {
        startTime = MathUtils.nowInNano();

        openOpLogger = bk.getOpenOpLogger();

        try {
            lh = new LedgerHandle(bk, ledgerId, metadata,
                                  metadata.getDigestType(),
                                  metadata.getPassword());
        } catch (GeneralSecurityException gse) {
            LOG.error("Security exception while opening ledger: " + ledgerId, gse);
            openComplete(BKException.Code.DigestNotInitializedException, null);
            return;
        }
        paxos = new PaxosClient(bk);

        // check if ledger is closed?
        paxos.get(lh, "EOL")
            .whenComplete((closed, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Error reading closed state");
                        openComplete(BKException.Code.UnexpectedConditionException,
                                     null);
                    } else {
                        if (!closed.isPresent()) { // we need to recover
                            recover();
                        } else {
                            ByteString eol = closed.get();
                            long lac = Long.parseLong(eol.toStringUtf8());
                            metadata.close(lac);

                            try {
                                lh = new LedgerHandle(bk, ledgerId, metadata,
                                                      metadata.getDigestType(),
                                                      metadata.getPassword());
                                openComplete(BKException.Code.OK, lh);
                            } catch (GeneralSecurityException gse) {
                                LOG.error("Security exception while opening ledger: " + ledgerId, gse);
                                openComplete(BKException.Code.DigestNotInitializedException, null);
                            }
                        }
                    }
                });
    }

    void recover() {
        final OrderedSafeGenericCallback<Void> finalCb
            = new OrderedSafeGenericCallback<Void>(bk.mainWorkerPool, ledgerId) {
                    @Override
                    public void safeOperationComplete(int rc, Void result) {
                        if (rc == BKException.Code.OK) {
                            openComplete(BKException.Code.OK, lh);
                        } else if (rc == BKException.Code.UnauthorizedAccessException) {
                            openComplete(BKException.Code.UnauthorizedAccessException, null);
                        } else {
                            openComplete(bk.getReturnRc(BKException.Code.LedgerRecoveryException), null);
                        }
                    }

                    @Override
                    public String toString() {
                        return String.format("Recover(%d)", ledgerId);
                    }
                };
        final GenericCallback<Void> cb = new TimedGenericCallback<Void>(
                finalCb, BKException.Code.OK, bk.getRecoverOpLogger());
        new LedgerRecoveryOp(lh, cb)
                    .parallelRead(lh.enableParallelRecoveryRead)
                    .readBatchSize(lh.recoveryReadBatchSize)
                    .initiate();
    }

    void openComplete(int rc, LedgerHandle lh) {
        if (BKException.Code.OK != rc) {
            openOpLogger.registerFailedEvent(
                    MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
        } else {
            openOpLogger.registerSuccessfulEvent(
                    MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
        }
        cb.openComplete(rc, lh, ctx);
    }
}
