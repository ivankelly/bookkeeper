package org.apache.bookkeeper.proto;

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

import org.junit.*;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.Enumeration;
import java.util.Arrays;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.ssl.SSLContextFactory;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.OrderedSafeExecutor;

import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;

import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import org.apache.commons.io.IOUtils;
import java.io.IOException;
import java.io.File;
import java.io.InputStream;
import java.io.FileOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests with SSL enabled.
 */
public class TestSSL extends BookKeeperClusterTestCase {
    static Logger LOG = LoggerFactory.getLogger(TestPerChannelBookieClient.class);
    private final static String CERT_RESOURCE = "/testcert.p12";
    private final static String CERT_PASSWORD = "testpass";

    public TestSSL() {
        super(3);
        baseConf.setSSLEnabled(true).setSSLKeyStore(CERT_RESOURCE)
            .setSSLKeyStorePassword(CERT_PASSWORD);
    }

    /**
     * Connect to an SSL server a bunch of times. Make sure we can cleanly
     * shut down afterwards
     */
    @Test(timeout=60000)
    public void testSSLConnection() throws Exception {
        ClientSocketChannelFactory channelFactory
            = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                                                Executors.newCachedThreadPool());
        OrderedSafeExecutor executor = new OrderedSafeExecutor(1);

        InetSocketAddress addr = getBookie(0);
        ClientConfiguration conf = new ClientConfiguration().setUseSSL(true);
        AtomicLong bytesOutstanding = new AtomicLong(0);
        PerChannelBookieClient client = new PerChannelBookieClient(conf, executor, channelFactory,
                                                                   new SSLContextFactory(conf),
                                                                   addr, bytesOutstanding);
        int numConnects = 1000;
        final AtomicInteger success = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(numConnects);
        for (int i = 0; i < numConnects; i++) {
            client.connectIfNeededAndDoOp(new GenericCallback<Void>() {
                    @Override
                    public void operationComplete(int rc, Void result) {
                        // do nothing, we don't care about doing anything with the connection,
                        // we just want to trigger it connecting.
                        if (rc != 0) {
                            LOG.info("Op completed with error {}", rc);
                            success.set(rc);
                        }
                        latch.countDown();
                    }
                });
        }
        assertEquals("All ops should have succeeded", 0, success.get());
        assertTrue("All ops should have happened", latch.await(5, TimeUnit.SECONDS));
        client.close();
        channelFactory.releaseExternalResources();
        executor.shutdown();
    }

    /**
     * Verify that a server will not start if ssl is enabled but no cert
     * is specified
     */
    @Test
    public void testStartSSLServerNoKeyStore() throws Exception {
        ServerConfiguration conf = newServerConfiguration()
            .setSSLEnabled(true).setSSLKeyStore("");
        try {
            startNewBookie(conf);
            fail("Shouldn't have been able to start");
        } catch (IOException ioe) {
            assertTrue(true);
        }
    }

    /**
     * Verify that a server will not start if ssl is enabled but the
     * cert password is incorrect
     */
    @Test
    public void testStartSSLServerBadPassword() throws Exception {
        ServerConfiguration conf = newServerConfiguration()
            .setSSLEnabled(true).setSSLKeyStorePassword("badpassword");
        try {
            startNewBookie(conf);
            fail("Shouldn't have been able to start");
        } catch (IOException ioe) {
            assertTrue(true);
        }
    }

    /**
     * Verify that a server can start while loading the cert from
     * a file rather than a resource.
     */
    @Test
    public void testStartSSLServerFileCert() throws Exception {
        InputStream in = getClass().getResourceAsStream(CERT_RESOURCE);
        File f = File.createTempFile("keystore", ".p12");
        FileOutputStream out = new FileOutputStream(f);
        IOUtils.copy(in, out);
        in.close();
        out.close();

        ServerConfiguration conf = newServerConfiguration()
            .setSSLEnabled(true)
            .setSSLKeyStore(f.toString())
            .setSSLKeyStorePassword(CERT_PASSWORD);
        startNewBookie(conf);        
    }

    private void testClient(ClientConfiguration conf, int clusterSize) throws Exception {
        BookKeeper client = new BookKeeper(conf);
        byte[] passwd = "testPassword".getBytes();
        int numEntries = 100;
        LedgerHandle lh = client.createLedger(clusterSize, clusterSize,
                                              DigestType.CRC32, passwd);
        
        byte[] testEntry = "testEntry".getBytes();
        for (int i = 0; i <= numEntries; i++) {
            lh.addEntry(testEntry);
        }
        long lid = lh.getId();
        lh.close();
        lh = client.openLedger(lid, DigestType.CRC32, passwd);
        Enumeration<LedgerEntry> entries = lh.readEntries(0, numEntries);
        while (entries.hasMoreElements()) {
            LedgerEntry e = entries.nextElement();
            assertTrue("Entry contents incorrect", Arrays.equals(e.getEntry(), testEntry));
        }
        lh.close();
    }

    /**
     * Verify that a client without ssl enabled can connect to a cluster
     * with SSL
     */
    @Test
    public void testConnectToSSLClusterNonSSLClient() throws Exception {
        ClientConfiguration conf = new ClientConfiguration(baseClientConf).setUseSSL(false);
        testClient(conf, numBookies);
    }

    /**
     * Verify that a client will fail to connect to a server if it has asked for SSL,
     * but it is not available. Verify that if there are enough SSL servers to fill the
     * ensemble, it will eventually use those rather than the non-SSL
     */
    @Test
    public void testMixedCluster() throws Exception {
        ClientConfiguration clientConf = new ClientConfiguration(baseClientConf).setUseSSL(true);
        int origNumBookies = numBookies;

        ServerConfiguration bookieConf = newServerConfiguration()
            .setSSLEnabled(false);
        startNewBookie(bookieConf);
        testClient(clientConf, origNumBookies + 1);
    }
}
