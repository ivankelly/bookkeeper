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

package org.apache.bookkeeper.admin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;

/**
 * Encapsulates updating the ledger metadata operation
 * 
 */
public class UpdateLedgerOp {

    private final static Logger LOG = LoggerFactory.getLogger(UpdateLedgerOp.class);
    private final BookKeeper bkc;
    private final BookKeeperAdmin admin;

    public UpdateLedgerOp(final BookKeeper bkc, final BookKeeperAdmin admin) {
        this.bkc = bkc;
        this.admin = admin;
    }

    /**
     * Update the bookie id present in the ledger metadata.
     * 
     * @param oldBookieId
     *            current bookie id
     * @param newBookieId
     *            new bookie id
     * @param rate
     *            no: of ledgers updating per second (default 5 per sec)
     * @param limit
     *            no: of ledgers selected for updation (default all ledgers).
     *            Stop update if reaching limit
     * @param printMessageCnt
     *            print progress of the ledger updation on every printMessageCnt
     * @throws IOException
     *             if there is an error when updating bookie id in ledger
     *             metadata
     * @throws InterruptedException
     *             interrupted exception when update ledger meta
     */
    public void updateBookieIdInLedgers(final BookieSocketAddress oldBookieId, final BookieSocketAddress newBookieId,
            final int rate, final int limit, final int printMessageCnt) throws BKException, IOException,
            InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final RateLimiter throttler = RateLimiter.create(rate);
        final Iterator<Long> ledgerItr = admin.listLedgers().iterator();
        ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "UpdateLedgerThread");
            }
        });
        final ExecutorService cbexecutor = Executors.newSingleThreadExecutor();
        executor.submit(new Runnable() {

            @Override
            public void run() {
                // iterate through all the ledgers
                try {
                    long issuedLedgerCnt = 0;
                    final Set<Future<Void>> outstanding = Collections.newSetFromMap(new ConcurrentHashMap<Future<Void>>());
                    final AtomicBoolean stop = new AtomicBoolean(false);
                    while (ledgerItr.hasNext() && !stop.get()) {
                        final Long lId = ledgerItr.next();
                        throttler.acquire();
                        final ReadLedgerMetadataCb readCb = new ReadLedgerMetadataCb(bkc, lId, oldBookieId, newBookieId);
                        outstanding.add(readCb);
                        readCb.addListener(executor, new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        readCb.get();
                                        outstanding.remove(readCb);
                                    } catch (ExecutionException ee) {
                                        LOG.error("Error updating ledger {}", lId, ee.getCause());
                                        stop.set(true);
                                    }
                                }
                            });

                        bkc.getLedgerManager().readLedgerMetadata(lId, readCb);
                        issuedLedgerCnt++;
                        final long updatedLedgerCnt = metaCb.getUpdatedLedgerCount();
                        // may print message on every printMessageCnt ledger
                        // update
                        maybePrintProgress(updatedLedgerCnt, issuedLedgerCnt, printMessageCnt);
                        if (limit != Integer.MIN_VALUE && issuedLedgerCnt >= limit) {
                            break;
                        }
                    }

                    for (Future<Void> f : outstanding) {
                        f.get();
                    }
                } catch (InterruptedException ie) {
                    LOG.error("Interrupted exception while updating ledger", ie);
                } finally {
                    latch.countDown(); // to avoid infinite wait
                }
            }

            private void maybePrintProgress(long updatedLedgerCnt, long issuedCount, int printMessageCnt) {
                if (printMessageCnt <= 0) {
                    return;
                }
                if (issuedCount % printMessageCnt == 0) {
                    LOG.info("Number of ledgers issued={}, updated={}", new Object[] { issuedCount, updatedLedgerCnt });
                }
            }
        });
        // Wait for the async method to complete.
        latch.await();
        executor.shutdown();
        LOG.info("Total number of ledgers updated = {}", metaCb.getUpdatedLedgerCount());
    }

    private final static class UpdateLedgerMetadataCb implements GenericCallback<CountDownLatch> {
        private volatile long updatedLedgerCount = 0;
        private List<CountDownLatch> ledgersWaitForCompletion = Collections
                .synchronizedList(new ArrayList<CountDownLatch>());
        private final AtomicBoolean stop;

        public UpdateLedgerMetadataCb(AtomicBoolean stop) {
            this.stop = stop;
        }

        @Override
        public void operationComplete(int rc, CountDownLatch obj) {
            if (rc != BKException.Code.OK) {
                // stops updating ledger meta data
                stop.set(true);
            } else {
                // updated successfully
                updatedLedgerCount++;
            }
            obj.countDown();
            ledgersWaitForCompletion.remove(obj);
        }

        public long getUpdatedLedgerCount() {
            return updatedLedgerCount;
        }

    }

    private final static class ReadLedgerMetadataCb extends AbstractFuture<Void> implements GenericCallback<LedgerMetadata> {
        final BookKeeper bkc;
        final Long ledgerId;
        final BookieSocketAddress curBookieAddr;
        final BookieSocketAddress toBookieAddr;

        public ReadLedgerMetadataCb(BookKeeper bkc, Long ledgerId, BookieSocketAddress curBookieAddr,
                                    BookieSocketAddress toBookieAddr) {
            this.bkc = bkc;
            this.ledgerId = ledgerId;
            this.curBookieAddr = curBookieAddr;
            this.toBookieAddr = toBookieAddr;
        }

        @Override
        public void operationComplete(int rc, LedgerMetadata metadata) {
            if (BKException.Code.NoSuchLedgerExistsException == rc) {
                set(null);
                return; // this is OK
            } else if (BKException.Code.OK != rc) {
                // open ledger failed.
                LOG.error("Get ledger metadata {} failed. Error code {}", new Object[] { ledgerId, rc });
                setException(BKException.create(rc));
                return;
            }
            boolean updateEnsemble = false;
            for (ArrayList<BookieSocketAddress> ensembles : metadata.getEnsembles().values()) {
                int index = ensembles.indexOf(curBookieAddr);
                if (-1 != index) {
                    ensembles.set(index, toBookieAddr);
                    updateEnsemble = true;
                }
            }
            if (!updateEnsemble) {
                set(null);
                return; // ledger doesn't contains the given curBookieId
            }
            final GenericCallback<Void> writeCb = new GenericCallback<Void>() {
                @Override
                public void operationComplete(int rc, Void result) {
                    if (rc != BKException.Code.OK) {
                        // metadata update failed
                        LOG.error("Ledger {} metadata update failed. Error code {}", new Object[] { ledgerId, rc });
                        setException(BKException.create(rc));
                    } else {
                        set(null);
                    }
                }
            };
            bkc.getLedgerManager().writeLedgerMetadata(ledgerId, metadata, writeCb);
        }
    }
}
