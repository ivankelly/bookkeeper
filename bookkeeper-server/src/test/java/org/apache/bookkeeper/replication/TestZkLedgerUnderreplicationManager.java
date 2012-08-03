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

package org.apache.bookkeeper.replication;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.bookkeeper.conf.ServerConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

/**
 * Test the zookeeper implementation of the ledger replication manager
 */
public class TestZkLedgerUnderreplicationManager {
    static final Logger LOG = LoggerFactory.getLogger(TestZkLedgerUnderreplicationManager.class);

    ZooKeeperUtil zkUtil = null;
    ZooKeeper zkc = null;
    ServerConfiguration conf = null;
    ExecutorService executor = null;

    @Before
    public void setupZooKeeper() throws Exception {
        zkUtil = new ZooKeeperUtil();
        zkUtil.startServer();
        zkc = zkUtil.getZooKeeperClient();
        conf = new ServerConfiguration().setZkServers(zkUtil.getZooKeeperConnectString());

        executor = Executors.newCachedThreadPool();
    }

    @After
    public void teardownZooKeeper() throws Exception {
        if (zkUtil != null) {
            zkUtil.killServer();
            zkUtil = null;
        }
        if (executor != null) {
            executor = null;
        }
    }

    private ZooKeeper getNewZooKeeper() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        ZooKeeper zkc = new ZooKeeper(zkUtil.getZooKeeperConnectString(), 10000,
                new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        // handle session disconnects and expires
                        if (event.getState().equals(Watcher.Event.KeeperState.SyncConnected)) {
                            latch.countDown();
                        }
                    }
                });
        if (!latch.await(10000, TimeUnit.MILLISECONDS)) {
            zkc.close();
            fail("Could not connect to zookeeper server");
        }
        return zkc;
    }

    private Future<Long> getLedgerToReplicate(final LedgerUnderreplicationManager m) {
        return executor.submit(new Callable<Long>() {
                public Long call() {
                    try {
                        return m.getLedgerToRereplicate();
                    } catch (Exception e) {
                        LOG.error("Error getting ledger id", e);
                        return -1L;
                    }
                }
            });
    }

    @Test
    public void testBasicInteraction() throws Exception {
        Set<Long> ledgers = new HashSet<Long>();
        ledgers.add(0xdeadbeefL);
        ledgers.add(0xbeefcafeL);
        ledgers.add(0xffffbeefL);
        ledgers.add(0xfacebeefL);
        String missingReplica = "localhost:3181";

        int count = 0;
        LedgerUnderreplicationManager m = new ZkLedgerUnderreplicationManager(conf, zkc);
        Iterator<Long> iter = ledgers.iterator();
        while (iter.hasNext()) {
            m.markLedgerUnderreplicated(iter.next(), missingReplica);
            count++;
        }

        List<Future<Long>> futures = new ArrayList<Future<Long>>();
        for (int i = 0; i < count; i++) {
            futures.add(getLedgerToReplicate(m));
        }

        for (Future<Long> f : futures) {
            Long l = f.get(5, TimeUnit.SECONDS);
            assertTrue(ledgers.remove(l));
        }

        Future f = getLedgerToReplicate(m);
        try {
            f.get(5, TimeUnit.SECONDS);
            fail("Shouldn't be able to find a ledger to replicate");
        } catch (TimeoutException te) {
            // correct behaviour
        }
        Long newl = 0xfefefefefefeL;
        m.markLedgerUnderreplicated(newl, missingReplica);
        assertEquals("Should have got the one just added", newl, f.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void testLocking() throws Exception {
        String missingReplica = "localhost:3181";

        ZooKeeper zkc1 = getNewZooKeeper();
        ZooKeeper zkc2 = getNewZooKeeper();
        try {
            LedgerUnderreplicationManager m1 = new ZkLedgerUnderreplicationManager(conf, zkc1);
            LedgerUnderreplicationManager m2 = new ZkLedgerUnderreplicationManager(conf, zkc2);

            Long ledger = 0xfeadeefdacL;
            m1.markLedgerUnderreplicated(ledger, missingReplica);
            Future<Long> f = getLedgerToReplicate(m1);
            Long l = f.get(5, TimeUnit.SECONDS);
            assertEquals("Should be the ledger I just marked", ledger, l);

            f = getLedgerToReplicate(m2);
            try {
                f.get(5, TimeUnit.SECONDS);
                fail("Shouldn't be able to find a ledger to replicate");
            } catch (TimeoutException te) {
                // correct behaviour
            }
            zkc1.close(); // should kill the lock

            l = f.get(5, TimeUnit.SECONDS);
            assertEquals("Should be the ledger I marked", ledger, l);
        } finally {
            zkc1.close();
            zkc2.close();
        }
    }

    @Test
    public void testCompletion() throws Exception {
        ZooKeeper zkc1 = getNewZooKeeper();
        ZooKeeper zkc2 = getNewZooKeeper();
        String missingReplica = "localhost:3181";

        try {
            LedgerUnderreplicationManager m1 = new ZkLedgerUnderreplicationManager(conf, zkc1);
            LedgerUnderreplicationManager m2 = new ZkLedgerUnderreplicationManager(conf, zkc2);

            Long ledgerA = 0xfeadeefdacL;
            Long ledgerB = 0xdefadebL;
            m1.markLedgerUnderreplicated(ledgerA, missingReplica);
            m1.markLedgerUnderreplicated(ledgerB, missingReplica);

            Future<Long> fA = getLedgerToReplicate(m1);
            Future<Long> fB = getLedgerToReplicate(m1);

            Long lA = fA.get(5, TimeUnit.SECONDS);
            Long lB = fB.get(5, TimeUnit.SECONDS);

            assertTrue("Should be the ledgers I just marked",
                       (lA.equals(ledgerA) && lB.equals(ledgerB))
                       || (lA.equals(ledgerB) && lB.equals(ledgerA)));

            Future<Long> f = getLedgerToReplicate(m2);
            try {
                f.get(5, TimeUnit.SECONDS);
                fail("Shouldn't be able to find a ledger to replicate");
            } catch (TimeoutException te) {
                // correct behaviour
            }
            m1.markLedgerComplete(lA);
            zkc1.close(); // should kill the lock

            Long l = f.get(5, TimeUnit.SECONDS);
            assertEquals("Should be the ledger I marked", lB, l);
        } finally {
            zkc1.close();
            zkc2.close();
        }
    }

    @Test
    public void testRelease() throws Exception {
        ZooKeeper zkc1 = getNewZooKeeper();
        ZooKeeper zkc2 = getNewZooKeeper();
        String missingReplica = "localhost:3181";

        try {
            LedgerUnderreplicationManager m1 = new ZkLedgerUnderreplicationManager(conf, zkc1);
            LedgerUnderreplicationManager m2 = new ZkLedgerUnderreplicationManager(conf, zkc2);

            Long ledgerA = 0xfeadeefdacL;
            Long ledgerB = 0xdefadebL;
            m1.markLedgerUnderreplicated(ledgerA, missingReplica);
            m1.markLedgerUnderreplicated(ledgerB, missingReplica);

            Future<Long> fA = getLedgerToReplicate(m1);
            Future<Long> fB = getLedgerToReplicate(m1);

            Long lA = fA.get(5, TimeUnit.SECONDS);
            Long lB = fB.get(5, TimeUnit.SECONDS);

            assertTrue("Should be the ledgers I just marked",
                       (lA.equals(ledgerA) && lB.equals(ledgerB))
                       || (lA.equals(ledgerB) && lB.equals(ledgerA)));

            Future<Long> f = getLedgerToReplicate(m2);
            try {
                f.get(5, TimeUnit.SECONDS);
                fail("Shouldn't be able to find a ledger to replicate");
            } catch (TimeoutException te) {
                // correct behaviour
            }
            m1.markLedgerComplete(lA);
            m1.releaseLedger(lB);

            Long l = f.get(5, TimeUnit.SECONDS);
            assertEquals("Should be the ledger I marked", lB, l);
        } finally {
            zkc1.close();
            zkc2.close();
        }
    }

    @Test
    public void testManyFailures() throws Exception {
        ZooKeeper zkc1 = getNewZooKeeper();
        ZooKeeper zkc2 = getNewZooKeeper();
        String missingReplica1 = "localhost:3181";
        String missingReplica2 = "localhost:3182";

        try {
            LedgerUnderreplicationManager m1 = new ZkLedgerUnderreplicationManager(conf, zkc1);
            LedgerUnderreplicationManager m2 = new ZkLedgerUnderreplicationManager(conf, zkc2);

            Long ledgerA = 0xfeadeefdacL;
            m1.markLedgerUnderreplicated(ledgerA, missingReplica1);

            Future<Long> fA = getLedgerToReplicate(m1);

            m1.markLedgerUnderreplicated(ledgerA, missingReplica2);

            Long lA = fA.get(5, TimeUnit.SECONDS);

            assertEquals("Should be the ledger I just marked",
                         lA, ledgerA);
            m1.markLedgerComplete(lA);

            Future<Long> f = getLedgerToReplicate(m1);
            lA = f.get(5, TimeUnit.SECONDS);
            assertEquals("Should be the ledger I had marked previously",
                         lA, ledgerA);
        } finally {
            zkc1.close();
            zkc2.close();
        }
    }

    @Test
    public void test2reportSame() throws Exception {
        ZooKeeper zkc1 = getNewZooKeeper();
        ZooKeeper zkc2 = getNewZooKeeper();
        String missingReplica1 = "localhost:3181";

        try {
            LedgerUnderreplicationManager m1 = new ZkLedgerUnderreplicationManager(conf, zkc1);
            LedgerUnderreplicationManager m2 = new ZkLedgerUnderreplicationManager(conf, zkc2);

            Long ledgerA = 0xfeadeefdacL;
            m1.markLedgerUnderreplicated(ledgerA, missingReplica1);
            m2.markLedgerUnderreplicated(ledgerA, missingReplica1);

            Future<Long> fA = getLedgerToReplicate(m1);


            Long lA = fA.get(5, TimeUnit.SECONDS);

            assertEquals("Should be the ledger I just marked",
                         lA, ledgerA);
            m1.markLedgerComplete(lA);

            Future<Long> f = getLedgerToReplicate(m2);
            try {
                f.get(5, TimeUnit.SECONDS);
                fail("Shouldn't be able to find a ledger to replicate");
            } catch (TimeoutException te) {
                // correct behaviour
            }

        } finally {
            zkc1.close();
            zkc2.close();
        }

    }
}
