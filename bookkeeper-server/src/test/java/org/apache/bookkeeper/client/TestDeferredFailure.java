package org.apache.bookkeeper.client;

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

import com.google.common.collect.Lists;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.meta.MockLedgerManager;

import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.MockBookieClient;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.TestCallbacks.GenericCallbackFuture;
import org.apache.bookkeeper.test.TestCallbacks.AddCallbackFuture;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDeferredFailure {
    private static final Logger LOG = LoggerFactory.getLogger(TestDeferredFailure.class);

    MockLedgerManager lm = new MockLedgerManager();
    MockBookieWatcher bookieWatcher = new MockBookieWatcher();
    EnsemblePlacementPolicy placementPolicy = new DefaultEnsemblePlacementPolicy();
    OrderedScheduler scheduler = OrderedScheduler.newSchedulerBuilder().name("test-executor").numThreads(1).build();
    MockBookieClient bookieClient = new MockBookieClient(scheduler);
    BookKeeperClientStats clientStats = BookKeeperClientStats.newInstance(NullStatsLogger.INSTANCE);

    @Test
    public void testBlah() throws Exception {
        BookieSocketAddress b1 = new BookieSocketAddress("b1:3181");
        BookieSocketAddress b2 = new BookieSocketAddress("b2:3181");
        BookieSocketAddress b3 = new BookieSocketAddress("b3:3181");
        BookieSocketAddress b4 = new BookieSocketAddress("b4:3181");
        BookieSocketAddress b5 = new BookieSocketAddress("b5:3181");
        BookieSocketAddress b6 = new BookieSocketAddress("b6:3181");

        bookieWatcher.addBookies(b1, b2, b3, b4, b5, b6);
        long ledgerId = 1L;
        LedgerMetadata metadata = LedgerMetadataBuilder.create()
            .withEnsembleSize(3).withWriteQuorumSize(3).withAckQuorumSize(2)
            .withEnsembleEntry(0L, Lists.newArrayList(b1, b2, b3))
            .build();
        GenericCallbackFuture<LedgerMetadata> createFuture = new GenericCallbackFuture<>();
        lm.createLedgerMetadata(ledgerId, metadata, createFuture);

        MockLedgerManager writerLm = lm.newClient();
        LedgerHandle lh = new LedgerHandle(ClientInternalConf.defaultValues(),
                                           writerLm, bookieWatcher, placementPolicy,
                                           bookieClient, scheduler, scheduler,
                                           () -> false, clientStats,
                                           ledgerId, createFuture.get(),
                                           BookKeeper.DigestType.CRC32C, new byte[0],
                                           WriteFlag.NONE);
        lh.append("entry-0".getBytes());
        lh.append("entry-1".getBytes());

        // stop all writes for the writing client to the ledger manager
        CompletableFuture<Void> stallWrites = writerLm.stallWrites();

        // stall writes on b1
        bookieWatcher.removeBookies(b1);
        bookieClient.errorBookies(b1); // <- when writes are unstalled, they will be errors
        CompletableFuture<Void> stallB1 = bookieClient.stallBookie(b1);

        lh.append("entry-2".getBytes()); // adds bookie to delayFailed list
        stallB1.complete(null);
        lh.append("entry-3".getBytes()); // triggers delayFailure handler
        while (lh.getLedgerMetadata().getEnsembles().size() == 1) {}

        // stall writes on b2
        bookieWatcher.removeBookies(b2);
        bookieClient.errorBookies(b2);
        CompletableFuture<Void> stallB2 = bookieClient.stallBookie(b2);
        lh.append("entry-4".getBytes());
        stallB2.complete(null);
        lh.append("entry-5".getBytes());
        while (lh.getLedgerMetadata().getEnsembles().size() == 2) {}

        // stall writes on b3
        bookieWatcher.removeBookies(b3);
        bookieClient.errorBookies(b3);
        CompletableFuture<Void> stallB3 = bookieClient.stallBookie(b3);

        lh.append("entry-5".getBytes());
        stallB3.complete(null);
        lh.append("entry-6".getBytes());
        while (lh.getLedgerMetadata().getEnsembles().size() == 3) {}

        lh.append("entry-7".getBytes());
        lh.append("entry-8".getBytes());
        lh.append("entry-9".getBytes());

        // at this stage b1, b2 & b3 will have been removed from the lh's metadata.
        // entries 6, 7, 8 & 9 have been written to bookies with no record
        // in the ledger's metadata in the ledger manager
        bookieClient.removeErrors(b1, b2, b3);

        GenericCallbackFuture<LedgerMetadata> readFuture = new GenericCallbackFuture<>();
        lm.readLedgerMetadata(ledgerId, readFuture);
        LOG.info("Read metadata {}", readFuture.get());
        LOG.info("Client is using metadata {}", lh.getLedgerMetadata());
        ReadOnlyLedgerHandle reader = new ReadOnlyLedgerHandle(ClientInternalConf.defaultValues(),
                                                               lm, bookieWatcher, placementPolicy,
                                                               bookieClient, scheduler, scheduler,
                                                               () -> false, clientStats,
                                                               ledgerId, readFuture.get(),
                                                               BookKeeper.DigestType.CRC32C, new byte[0], false);

        GenericCallbackFuture<Void> recoverFuture = new GenericCallbackFuture<>();
        reader.recover(recoverFuture);
        recoverFuture.get();

        Assert.assertEquals(9, reader.getLastAddConfirmed());
        try (LedgerEntries e = reader.read(0, 9)) {
            for (int i = 0; i < 9; i++) {
                Assert.assertEquals(new String(e.getEntry(i).getEntryBytes()), "entry-" + i);
            }
        }
    }
}
