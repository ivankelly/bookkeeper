package org.apache.bookkeeper.test;

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
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.nio.ByteBuffer;
import org.junit.Test;

/**
 * Test that when the client creates connections, the connection
 * get cleaned up. Otherwise netty cleanup fails. This test verifies
 * the fix for BOOKKEEPER-5.
 */
public class TestShutdown extends BookKeeperClusterTestCase {

    public TestShutdown() {
        super(10);
    }

    @Test(timeout=30000)
    public void testShutdown() throws IOException {
        try {
            int numLedgers = 1000;
            int throttle = (((Double) Math.max(1.0, ((double) 10000/numLedgers))).intValue());
            bkc.getConf().setThrottleValue(throttle);
            LedgerHandle[] lhArray = new LedgerHandle[numLedgers];
            final CountDownLatch latch = new CountDownLatch(numLedgers);
            final AtomicInteger returnCode = new AtomicInteger(BKException.Code.OK);
            AddCallback addcb = new AddCallback() {
                    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                        if (rc != BKException.Code.OK) {
                            returnCode.set(rc);
                        }
                        latch.countDown();
                    }
                };
            for(int i = 0; i < numLedgers; i++) {
                lhArray[i] = bkc.createLedger(1, 1, BookKeeper.DigestType.CRC32,
                                              new byte[] {'a', 'b'});
                LOG.debug("Ledger handle: " + lhArray[i].getId());
            }
            LOG.info("Done creating ledgers.");
            Random r = new Random();

            for (int i = 0; i < numLedgers; i++) {
                ByteBuffer entry = ByteBuffer.allocate(4);
                entry.putInt(0xbeefcafe);
                entry.position(0);

                int nextLh = r.nextInt(numLedgers);
                lhArray[nextLh].asyncAddEntry(entry.array(), addcb, null);
            }

            latch.await();

            LOG.debug("*** WRITE COMPLETE ***");
            // close ledger
            for(int i = 0; i < lhArray.length; i++) {
                lhArray[i].close();
            }
        } catch (BKException e) {
            LOG.error("Test failed", e);
            fail("Test failed due to BookKeeper exception");
        } catch (InterruptedException e) {
            LOG.error("Test failed", e);
            fail("Test failed due to interruption");
        }
    }
}
