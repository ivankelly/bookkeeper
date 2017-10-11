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

public class Log0Test extends Log0TestBase {
    static final Logger LOG = LoggerFactory.getLogger(Log0Test.class);

    public Log0Test() {
        super(5);
    }

    @Test
    public void testCantWriteToLedger0() throws Exception {
        LOG.info("client client");
        BookKeeper client = BookKeeper.forConfig(new ClientConfiguration()).build();

        // write a ledger and close it
        List<BookieSocketAddress> bootstrapEnsemble = getBookieAddresses();

        
    }

    @Test
    public void testCreateAndOpenWithRecovery() throws Exception {
        LOG.info("client client");
        BookKeeper client = BookKeeper.forConfig(new ClientConfiguration()).build();

        // write a ledger and close it
        List<BookieSocketAddress> bookies = getBookieAddresses();
        LOG.info("Create ledger");
        LedgerHandle lh = client.createLedger(1L, bookies, 5, 5, 3,
                                              BookKeeper.DigestType.MAC, "foobar".getBytes());
        long lac = 0;
        LOG.info("Write to ledger");
        for (int i = 0; i < 100; i++) {
            lac = lh.addEntry(("foobar"+i).getBytes());
        }
        // open a bookie and read it
        LOG.info("Open ledger");
        LedgerHandle openlh = client.openLedger(1L, bookies, 5, 5, 3,
                               BookKeeper.DigestType.MAC, "foobar".getBytes());
        LOG.info("Read last add confirmed");
        assertEquals("last add confirmed should be last entry",
                     lac, openlh.getLastAddConfirmed());
        assertEquals("create lac also be the same",
                     lac, lh.getLastAddConfirmed());

        try {
            lh.addEntry("shouldn't succeed".getBytes());
            fail("Shouldn't have been able to add more entries");
        } catch (Exception e) {
            // correct behaviour
        }
    }

    @Test
    public void testCreateAndTailing() throws Exception {
        BookKeeper client = BookKeeper.forConfig(new ClientConfiguration()).build();

        // write a ledger and close it
        List<BookieSocketAddress> bookies = getBookieAddresses();
        LedgerHandle lh = client.createLedger(1L, bookies, 5, 5, 3,
                                              BookKeeper.DigestType.MAC, "foobar".getBytes());
        LedgerHandle tailer = client.openLedgerNoRecovery(1L, bookies, 5, 5, 3,
                BookKeeper.DigestType.MAC, "foobar".getBytes());
        long lac = 0;
        for (int i = 0; i < 100; i++) {
            lac = lh.addEntry(("foobar"+i).getBytes());
            if (i == 0) {
                assertEquals("no entries confirmed yet",
                             LedgerHandle.INVALID_ENTRY_ID, tailer.readLastConfirmed());
            } else {
                assertEquals("previous entry is confirmed",
                             lac - 1, tailer.readLastConfirmed());
            }
        }

        lh.close();
        assertEquals("last entry is confirmed",
                     lac, tailer.readLastConfirmed());
        assertTrue("tailer is now closed",
                   tailer.isClosed());
    }

    @Test
    public void testEnsembleDoesntChange() throws Exception {

        // remove two nodes, works fine

        // remove third node, write fails
    }
}
