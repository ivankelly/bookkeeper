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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import static org.junit.Assert.*;

import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.conf.ClientConfiguration;

public class SlowBookieTest extends BookKeeperClusterTestCase {
    static Logger LOG = LoggerFactory.getLogger(SlowBookieTest.class);

    public SlowBookieTest() {
        super(4);
    }

    @Test
    public void testSlowBookie() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setZkServers(zkUtil.getZooKeeperConnectString()).setReadTimeout(360);

        BookKeeper bkc = new BookKeeper(conf);

        LedgerHandle lh = bkc.createLedger(4, 3, 2, BookKeeper.DigestType.CRC32, new byte[] {});
        byte[] entry = "Test Entry".getBytes();
        for (int i = 0; i < 10; i++) {
            lh.addEntry(entry);
        }
        final CountDownLatch b0latch = new CountDownLatch(1);
        final CountDownLatch b1latch = new CountDownLatch(1);
        try {
            sleepBookie(getBookie(0), b0latch);
            for (int i = 0; i < 10; i++) {
                lh.addEntry(entry);
            }
            sleepBookie(getBookie(2), b1latch); // should cover all quorums

            final AtomicInteger i = new AtomicInteger(0xdeadbeef);
            AsyncCallback.AddCallback cb = new AsyncCallback.AddCallback() {
                    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                        i.set(rc);
                    }
                };
            lh.asyncAddEntry(entry, cb, null);

            Thread.sleep(1000); // sleep a second to allow time to complete
            assertEquals(i.get(), 0xdeadbeef);
            b0latch.countDown();
            b1latch.countDown();
            Thread.sleep(2000);
            assertEquals(i.get(), BKException.Code.OK);
        } finally {
            b0latch.countDown();
            b1latch.countDown();
        }
    }
}
