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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.meta.RpcLedgerManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.server.ServerResources;
import org.apache.bookkeeper.server.conf.RpcConfiguration;
import org.apache.bookkeeper.server.rpc.BookieRpcServerSpec;
import org.apache.bookkeeper.server.service.BookieRpcService;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.PortManager;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests the bookie recovery admin functionality.
 */
public class BookieRpcTest extends BookKeeperClusterTestCase {

    private final static Logger LOG = LoggerFactory.getLogger(BookieRpcTest.class);

    // rpc services in each bookie
    List<BookieRpcService> rpcServices = Lists.newArrayList();

    // Constructor
    public BookieRpcTest() {
        super(3);

        // client ledgerManagerFactory
        String ledgerManagerFactory = "org.apache.bookkeeper.client.meta.RpcLedgerManagerFactory";
        baseClientConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        baseConf.setRpcServerEnabled(true);
        LOG.info("Set client using ledger manager " + ledgerManagerFactory);
    }

    @Override
    protected BookieServer startBookie(ServerConfiguration conf)
      throws Exception {
        // start bookie server
        BookieServer server = new BookieServer(conf);
        server.start();
        return server;
    }

    protected BookieRpcService startBookieRpcService(BookieServer server)
      throws Exception {
        // start bookie rpc service
        StatsLogger rpcStatsLogger = NullStatsLogger.INSTANCE;
        int rpcPort = PortManager.nextFreePort();
        BookieSocketAddress bookieAddr = server.getLocalAddress();
        BookieSocketAddress rpcAddr = new BookieSocketAddress(
          bookieAddr.getHostName(),
          rpcPort);
        RpcConfiguration rpcConf = new RpcConfiguration(baseConf);
        BookieRpcServerSpec spec = BookieRpcServerSpec.newBuilder()
          .bookieSupplier(() -> server.getBookie())
          .rpcConf(rpcConf)
          .endpoint(rpcAddr)
          .statsLogger(rpcStatsLogger)
          .schedulerResource(ServerResources.create(rpcStatsLogger).scheduler())
          .build();

        BookieRpcService rpcService = new BookieRpcService(
          rpcConf,
          spec,
          rpcStatsLogger);
        rpcService.start();
        rpcServices.add(rpcService);
        LOG.info("start bookie at: {}, and bookieRpc at: {}", bookieAddr.toString(), rpcAddr.toString() );
        return rpcService;
    }

    // Need to create bookies first,
    // Then, set client config with related bookie address for rpc service,
    // At last, create client cluster -- bkc.
    @Override
    protected void startBKCluster() throws Exception {
        LOG.info("new startBKCluster " );

        baseClientConf.setZkServers(zkUtil.getZooKeeperConnectString());

        // Create Bookie Servers (B1, B2, B3)
        for (int i = 0; i < numBookies; i++) {
            startNewBookie();
        }

        List<BookieSocketAddress> addresses = Lists.newArrayListWithExpectedSize(bs.size());
        for (BookieServer server : bs) {
            startBookieRpcService(server);
            BookieSocketAddress rpcAddress = startBookieRpcService(server).getBuilder().endpoint().get();
            addresses.add(rpcAddress);
            LOG.info("BookieAddress: {}, RpcAddress: {}", rpcAddress );
        }

        baseClientConf.setClientBootstrapBookies(addresses);
        if (numBookies > 0) {
            bkc = new BookKeeperTestClient(baseClientConf);
        }
    }

    @Override
    protected void stopBKCluster() throws Exception {
        for (BookieRpcService rpcService : rpcServices) {
            rpcService.stop();
        }
        super.stopBKCluster();
    }

    private List<LedgerHandle> createLedgers(int numLedgers, int ensemble, int quorum)
            throws BKException, IOException,
        InterruptedException {
        List<LedgerHandle> lhs = new ArrayList<LedgerHandle>();
        for (int i = 0; i < numLedgers; i++) {
            lhs.add(bkc.createLedger(ensemble, quorum, DigestType.CRC32, "".getBytes()));
        }
        return lhs;
    }

    private void writeEntriesToLedgers(int numEntries, long startEntryId,
                                       List<LedgerHandle> lhs)
        throws BKException, InterruptedException {
        for (LedgerHandle lh : lhs) {
            for (int i = 0; i < numEntries; i++) {
                lh.addEntry(("LedgerId: " + lh.getId() + ", EntryId: " + (startEntryId + i)).getBytes());
            }
        }
    }

    class SyncLedgerMetaObject {
        boolean value;
        int rc;
        LedgerMetadata meta;

        public SyncLedgerMetaObject() {
            value = false;
            meta = null;
        }
    }

    // Test operations of create ledger, read Ledger metadata, write entries, close ledger, and delete ledger.
    // It should success trigger generateLedgerId, createLedgerMetadata, readLedgerMetadata, writeLedgerMetadata
    // and removeLedgerMetadata
    @Test
    public void testLedgersOperation() throws Exception {
        assertTrue(bkc.getUnderlyingLedgerManager().getClass().getSimpleName().equals("RpcLedgerManager"));

        int numLedgers = 3;
        // This is for operations test, and will call generateLedgerId, and createLedgerMetadata
        List<LedgerHandle> lhs = createLedgers(numLedgers, 3, 2);

        // Write the entries for the ledgers with dummy values.
        int numMsgs = 10;
        writeEntriesToLedgers(numMsgs, 0, lhs);

        for (LedgerHandle lh : lhs) {
            // Test readLedgerMetadata
            SyncLedgerMetaObject syncObj = new SyncLedgerMetaObject();
            bkc.getLedgerManager().readLedgerMetadata(lh.getId(), new GenericCallback<LedgerMetadata>() {
                @Override
                public void operationComplete(int rc, LedgerMetadata result) {
                    synchronized (syncObj) {
                        syncObj.rc = rc;
                        syncObj.meta = result;
                        syncObj.value = true;
                        syncObj.notify();
                    }
                }
            });
            synchronized (syncObj) {
                while (!syncObj.value) {
                    syncObj.wait();
                }
            }
            // verify read value is right.
            assertEquals(BKException.Code.OK, syncObj.rc);
            assertEquals(lh.getLedgerMetadata().toString(), syncObj.meta.toString());

            // this will use updateLedgerOp to call writeLedgerMetadata
            lh.close();
            bkc.deleteLedger(lh.getId());
        }

        try {
            lhs.get(0).addEntry("add entry after delete should fail".getBytes());
            Assert.fail("should have thrown exception");
        } catch (BKException.BKLedgerClosedException ex) {
        }

    }

    // Create 270 ledgers to test iterate:
    // In each iterate, it will return 100, which is RpcLedgerManager.DEFAULT_GET_LEDGER_RANGES_LIMIT, ledgers.
    // So expect iterate 3 times, then hasNext() will return false.
    @Test
    public void testGetLedgerRanges() throws Exception {
        int iterSize = RpcLedgerManager.DEFAULT_GET_LEDGER_RANGES_LIMIT;
        assertEquals(100, iterSize);
        List<LedgerHandle> lhs = createLedgers(270, 3, 2);
        LedgerManager.LedgerRangeIterator iterator = bkc.getLedgerManager().getLedgerRanges();

        assertNotNull(iterator);
        Thread.sleep(100);
        assertTrue(iterator.hasNext());

        if (iterator.hasNext()) {
            LedgerManager.LedgerRange ledgerRange = iterator.next();
            assertEquals(100, ledgerRange.size());
        }

        assertTrue(iterator.hasNext());
        if (iterator.hasNext()) {
            LedgerManager.LedgerRange ledgerRange = iterator.next();
            assertEquals(100, ledgerRange.size());
        }

        assertTrue(iterator.hasNext());
        if (iterator.hasNext()) {
            LedgerManager.LedgerRange ledgerRange = iterator.next();
            assertEquals(70, ledgerRange.size());
        }

        assertFalse(iterator.hasNext());
        assertFalse(iterator.hasNext());
    }

    // Test add listener: create ledgers, add listener to each ledger.
    // Verify listeners called while ledgermedata change and ledger delete.
    @Test
    public void testAddListener() throws Exception {
        assertTrue(bkc.getUnderlyingLedgerManager().getClass().getSimpleName().equals("RpcLedgerManager"));

        int numLedgers = 3;
        List<LedgerHandle> lhs = createLedgers(numLedgers, 3, 2);

        for (LedgerHandle lh : lhs) {
            // register a listener
            final AtomicInteger metadataChanged = new AtomicInteger(0);
            bkc.getLedgerManager().registerLedgerMetadataListener(lh.getId(),
              new LedgerMetadataListener() {
                  @Override
                  public void onChanged(long ledgerId, LedgerMetadata metadata) {
                      assertEquals(ledgerId, lh.getId());
                      metadataChanged.incrementAndGet();
                      LOG.info("listener metadataChanged :{}", metadataChanged.get());
                  }
              });

            // this will use updateLedgerOp to call writeLedgerMetadata
            lh.close();

            Thread.sleep(200);
            // verify we listened metadata changes. 2 = add listener + write
            assertEquals(2, metadataChanged.get());

            // this will call removeLedgerMetadata
            bkc.deleteLedger(lh.getId());

            Thread.sleep(200);
            assertEquals(3, metadataChanged.get());
        }
    }

    // Test add listener: create ledgers, add listener and remove listener to each ledger.
    // Verify listeners work as expected.
    @Test
    public void testRemoveListener() throws Exception {
        assertTrue(bkc.getUnderlyingLedgerManager().getClass().getSimpleName().equals("RpcLedgerManager"));

        int numLedgers = 3;
        List<LedgerHandle> lhs = createLedgers(numLedgers, 3, 2);

        for (LedgerHandle lh : lhs) {
            // register a listener
            final AtomicInteger metadataChanged = new AtomicInteger(0);
            LedgerMetadataListener listener = new LedgerMetadataListener() {
                @Override
                public void onChanged(long ledgerId, LedgerMetadata metadata) {
                    assertEquals(ledgerId, lh.getId());
                    metadataChanged.incrementAndGet();
                    LOG.info("listener metadataChanged :{}", metadataChanged.get());
                }
            };
            bkc.getLedgerManager().registerLedgerMetadataListener(lh.getId(), listener);

            // this will use updateLedgerOp to call writeLedgerMetadata
            lh.close();

            Thread.sleep(200);
            // verify we listened metadata changes. add listener + write
            assertEquals(2, metadataChanged.get());

            bkc.getLedgerManager().unregisterLedgerMetadataListener(lh.getId(), listener);

            // After unregister listener, counter will not change while delete the ledger.
            bkc.deleteLedger(lh.getId());

            Thread.sleep(200);
            assertEquals(2, metadataChanged.get());
        }
    }
}
