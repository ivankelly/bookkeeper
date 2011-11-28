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

package org.apache.bookkeeper.meta;

import java.io.IOException;
import java.util.Random;
import java.lang.reflect.Field;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.bookkeeper.test.BaseTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert.*;

public class LedgerLayoutTest extends BaseTestCase {
    DigestType digestType;

    public LedgerLayoutTest(DigestType digestType) {
        super(0);
        this.digestType = digestType;
    }

    @Test
    public void testLedgerLayout() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setLedgerManagerType(HierarchicalLedgerManager.NAME);
        String ledgerRootPath = "/testLedgerLayout";

        zkc.create(ledgerRootPath, new byte[0], 
                   Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        LedgerLayout layout = LedgerLayout.readLayout(zkc, ledgerRootPath);
        assertTrue("Layout should be null", layout == null);

        String testName = "foobar";
        int testVersion = 0xdeadbeef;
        // use layout defined in configuration also create it in zookeeper
        LedgerLayout layout2 = new LedgerLayout(testName, testVersion);
        layout2.store(zkc, ledgerRootPath);

        layout = LedgerLayout.readLayout(zkc, ledgerRootPath);
        assertEquals(testName, layout.getManagerType());
        assertEquals(testVersion, layout.getManagerVersion());
    }

    private void writeLedgerLayout(
                                  String ledgersRootPath,
                                  String managerType,
                                  int managerVersion, int layoutVersion)
        throws Exception {
        LedgerLayout layout = new LedgerLayout(managerType, managerVersion);

        Field f = LedgerLayout.class.getDeclaredField("layoutFormatVersion");
        f.setAccessible(true);
        f.set(layout, layoutVersion);

        layout.store(zkc, ledgersRootPath);
    }

    @Test
    public void testBadVersionLedgerLayout() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        // write bad version ledger layout
        writeLedgerLayout(conf.getZkLedgersRootPath(), FlatLedgerManager.NAME,
                FlatLedgerManager.CUR_VERSION, LedgerLayout.LAYOUT_FORMAT_VERSION + 1);
        
        try {
            LedgerLayout.readLayout(zkc, conf.getZkLedgersRootPath());
            fail("Shouldn't reach here!");
        } catch (IOException ie) {
            assertTrue("Invalid exception", ie.getMessage().contains("version not compatible"));
        }
    }

    @Test
    public void testAbsentLedgerManagerLayout() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        String ledgersLayout = conf.getZkLedgersRootPath() + "/" + LedgerLayout.LAYOUT_ZNODE;
        // write bad format ledger layout
        StringBuilder sb = new StringBuilder();
        sb.append(LedgerLayout.LAYOUT_FORMAT_VERSION).append("\n");
        zkc.create(ledgersLayout, sb.toString().getBytes(),
                                 Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        try {
            LedgerLayout.readLayout(zkc, conf.getZkLedgersRootPath());
            fail("Shouldn't reach here!");
        } catch (IOException ie) {
            assertTrue("Invalid exception", ie.getMessage().contains("version absent from"));
        }
    }

    @Test
    public void testBaseLedgerManagerLayout() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        String rootPath = conf.getZkLedgersRootPath();
        String ledgersLayout = rootPath + "/" + LedgerLayout.LAYOUT_ZNODE;
        // write bad format ledger layout
        StringBuilder sb = new StringBuilder();
        sb.append(LedgerLayout.LAYOUT_FORMAT_VERSION).append("\n")
          .append(FlatLedgerManager.NAME);
        zkc.create(ledgersLayout, sb.toString().getBytes(),
                                 Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        try {
            LedgerLayout.readLayout(zkc, rootPath);
            fail("Shouldn't reach here!");
        } catch (IOException ie) {
            assertTrue("Invalid exception", ie.getMessage().contains("Invalid Ledger Manager"));
        }
    }
}
