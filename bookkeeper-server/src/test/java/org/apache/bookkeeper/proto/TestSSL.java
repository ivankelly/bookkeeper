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

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.proto.ssl.SSLContextFactory;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.OrderedSafeExecutor;

import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests with SSL enabled.
 */
public class TestSSL extends BookKeeperClusterTestCase {
    static Logger LOG = LoggerFactory.getLogger(TestPerChannelBookieClient.class);

    public TestSSL() {
        super(1);
        baseConf.setSSLEnabled(true).setSSLKeyStore("/testcert.p12").setSSLKeyStorePassword("testpass");
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

}
