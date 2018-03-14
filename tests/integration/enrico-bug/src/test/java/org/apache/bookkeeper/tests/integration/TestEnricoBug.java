/*
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
package org.apache.bookkeeper.tests.integration;

import com.github.dockerjava.api.DockerClient;

import java.util.Enumeration;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.tests.BookKeeperClusterUtils;

import org.jboss.arquillian.junit.Arquillian;
import org.jboss.arquillian.test.api.ArquillianResource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Arquillian.class)
public class TestEnricoBug {
    private static final Logger LOG = LoggerFactory.getLogger(TestEnricoBug.class);
    private static byte[] PASSWD = "foobar".getBytes();

    @ArquillianResource
    DockerClient docker;

    private String currentVersion = System.getProperty("currentVersion");

    @Before
    public void before() throws Exception {
        // First test to run, formats metadata and bookies
        if (BookKeeperClusterUtils.metadataFormatIfNeeded(docker, currentVersion)) {
            BookKeeperClusterUtils.formatAllBookies(docker, currentVersion);
        }
    }

    @Test
    public void testTailingWritingKilling() throws Exception {
        Assert.assertTrue(BookKeeperClusterUtils.startAllBookiesWithVersion(docker, "4.6.1"));

        String zookeeper = BookKeeperClusterUtils.zookeeperConnectString(docker);
        BookKeeper bk = new BookKeeper(zookeeper);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        LedgerHandle writelh = bk.createLedger(2, 2, 2, BookKeeper.DigestType.MAC, PASSWD);
        Future<?> writer = executor.submit((Callable<?>) () -> {
                while (true) {
                    writelh.addEntry("entry".getBytes());
                }
            });

        Future<?> reader = executor.submit((Callable<?>) () -> {
                long lastRead = 0;
                while (true) {
                    try (LedgerHandle readlh = bk.openLedgerNoRecovery(
                                 writelh.getId(), BookKeeper.DigestType.MAC, PASSWD)) {
                        long lac = readlh.getLastAddConfirmed();
                        if (lac > lastRead) {
                            Enumeration<LedgerEntry> entries = readlh.readEntries(lastRead, lac);
                            while (entries.hasMoreElements()) {
                                entries.nextElement();
                            }
                            lastRead = lac;
                        }
                    }
                }
            });

        Thread.sleep(5000);
        LOG.info("Killing some bookie");
        BookKeeperClusterUtils.runOnAnyBookie(docker, "pkill", "-9", "java");

        LOG.info("Wait 10 seconds, then kill some bookie");
        Thread.sleep(10000);
        BookKeeperClusterUtils.runOnAnyBookie(docker, "pkill", "-9", "java");

        LOG.info("Wait 10 seconds, then kill some bookie");
        Thread.sleep(10000);
        BookKeeperClusterUtils.runOnAnyBookie(docker, "pkill", "-9", "java");

        LOG.info("Wait 10 seconds, then kill some bookie");
        Thread.sleep(10000);
        BookKeeperClusterUtils.runOnAnyBookie(docker, "pkill", "-9", "java");

        LOG.info("Wait 10 seconds, then kill some bookie");
        Thread.sleep(10000);
        BookKeeperClusterUtils.runOnAnyBookie(docker, "pkill", "-9", "java");

        LOG.info("Wait 20 seconds, before checking client");

        try {
            reader.get(20, TimeUnit.SECONDS);
            writer.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            LOG.info("Timed out, all good");
        }

        LOG.info("Shutting down");
        executor.shutdownNow();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

}
