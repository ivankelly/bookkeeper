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

import com.google.common.base.Stopwatch;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.BookKeeperTestClient;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.metastore.InMemoryMetaStore;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.AutoRecoveryMain;
import org.apache.bookkeeper.replication.Auditor;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LedgerChainsTest extends Log0TestBase {
    static final Logger LOG = LoggerFactory.getLogger(Log0Test.class);

    public LedgerChainsTest() {
        super(5);
    }

    @Test
    public void testCantWriteToLedger0() throws Exception {
        BookKeeper client = BookKeeper.forConfig(new ClientConfiguration()).build();

        // write a ledger and close it
        List<BookieSocketAddress> bootstrapEnsemble = getBookieAddresses();

        LedgerChains chains = new LedgerChains(client);

        LedgerHandle ledger0 = chains.ledger0(bootstrapEnsemble).get();
        assertTrue(ledger0.isClosed());
        assertEquals("Ledger0 should be emtpy",
                     LedgerHandle.INVALID_ENTRY_ID, ledger0.getLastAddConfirmed());
        try {
            ledger0.addEntry("foobar".getBytes());
            fail("Shouldn't have been able to write anything");
        } catch (Exception e) {
            // correct
        }
    }

    @Test
    public void testBuildingChains() throws Exception {
        BookKeeper client = BookKeeper.forConfig(new ClientConfiguration()).build();

        // write a ledger and close it
        List<BookieSocketAddress> bootstrapEnsemble = getBookieAddresses();

        LedgerChains chains = new LedgerChains(client);
        LedgerHandle ledger0 = chains.ledger0(bootstrapEnsemble).get();
        LedgerHandle firstLedger = chains.writeNextLedger(ledger0).get();
        try {
            chains.writeNextLedger(ledger0).get();
            fail("shouldn't be able to chain again");
        } catch (Exception e) {
            // correct
        }
        assertNotEquals("ledger ids should differ",
                        ledger0.getId(), firstLedger.getId());

        firstLedger.addEntry("foobar".getBytes());

        LedgerHandle secondLedger = chains.writeNextLedger(firstLedger).get();
        assertNotEquals("ledger ids should differ",
                        secondLedger.getId(), firstLedger.getId());
        LedgerHandle thirdLedger = chains.writeNextLedger(secondLedger).get();
        assertNotEquals("ledger ids should differ",
                        thirdLedger.getId(), secondLedger.getId());
        try {
            secondLedger.addEntry("foobar".getBytes());
            fail("Shouldn't be able to write to previous ledger");
        } catch (Exception e) {
            // correct
        }
        thirdLedger.addEntry("foobar".getBytes());
    }

    @Test
    public void testTailingChain() throws Exception {
        BookKeeper client = BookKeeper.forConfig(new ClientConfiguration()).build();

        // write a ledger and close it
        List<BookieSocketAddress> bootstrapEnsemble = getBookieAddresses();

        LedgerChains chains = new LedgerChains(client);
        LedgerHandle ledger0 = chains.ledger0(bootstrapEnsemble).get();
        LedgerHandle writer1 = chains.writeNextLedger(ledger0).get();
        LedgerHandle tail1 = chains.readNextLedger(ledger0).get();

        long e1 = writer1.addEntry("foobar".getBytes());
        assertEquals("Shouldn't be anything visible yet",
                     LedgerHandle.INVALID_ENTRY_ID, tail1.readLastConfirmed());
        try {
            // try to move to next ledger, even though not closed
            chains.readNextLedger(tail1).get();
            fail("Should fail to move, current ledger is closed");
        } catch (Exception e) {
            // correct
        }
        long e2 = writer1.addEntry("foobar".getBytes());

        assertEquals("First entry should now be visible",
                     e1, tail1.readLastConfirmed());

        LedgerHandle writer2 = chains.writeNextLedger(writer1).get();
        Thread.sleep(1000); // wait a little for the commit to happen
        assertEquals("Second entry should now be visible",
                     e2, tail1.readLastConfirmed());

        assertTrue("tail1 should be closed", tail1.isClosed());

        LedgerHandle tail2 = chains.readNextLedger(tail1).get();
        assertEquals("Shouldn't be anything visible yet",
                     LedgerHandle.INVALID_ENTRY_ID, tail2.readLastConfirmed());
        long e3 = writer2.addEntry("barfoo".getBytes());
        assertEquals("Shouldn't be anything visible yet",
                     LedgerHandle.INVALID_ENTRY_ID, tail2.readLastConfirmed());
        writer2.addEntry("foobar".getBytes());
        assertEquals("Third entry should now be visible",
                     e3, tail2.readLastConfirmed());
    }
}
