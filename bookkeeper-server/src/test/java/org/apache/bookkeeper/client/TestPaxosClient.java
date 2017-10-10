/**
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.proto.WriterId;
import org.apache.bookkeeper.proto.PaxosValue;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestPaxosClient extends Log0TestBase {

    private static final Logger LOG = LoggerFactory.getLogger(
            TestPaxosClient.class);

    private static final int numOfBookies = 5;
    private long ledgerId = 1;
    private BookKeeper bkc;

    public TestPaxosClient() throws Exception {
        super(numOfBookies);
        bkc = BookKeeper.forConfig(new ClientConfiguration()).build();
    }

    LedgerHandle createLedger() throws Exception {
        return bkc.createLedger(ledgerId++, getBookieAddresses(),
                                numOfBookies, numOfBookies,
                                (int)Math.ceil((numOfBookies + 0.1)/2),
                                DigestType.CRC32, "testPasswd".getBytes());
    }

    @Test
    public void singleRoundSucceeds() throws Exception {
        LedgerHandle lh = createLedger();

        PaxosClient client = new PaxosClient(bkc);
        PaxosClient client2 = new PaxosClient(bkc);

        Map<String,ByteString> proposedValues = new HashMap<>();
        proposedValues.put("foobar", ByteString.copyFromUtf8("barbob"));
        proposedValues.put("fizzbuzz", ByteString.copyFromUtf8("bamboo"));

        Map<String,ByteString> differentValues = new HashMap<>();
        differentValues.put("foobar", ByteString.copyFromUtf8("wahwah"));
        differentValues.put("fizzbuzz", ByteString.copyFromUtf8("flipflop"));

        assertEquals("Original proposed values are set",
                     proposedValues,
                     client.propose(lh, proposedValues).get());
        assertEquals("Original proposed values are still set",
                     proposedValues,
                     client2.propose(lh, differentValues).get());
    }

    @Test
    public void manyProposers() throws Exception {
        LedgerHandle lh = createLedger();
        List<CompletableFuture<Map<String,ByteString>>> futures
            = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            PaxosClient client = new PaxosClient(bkc);

            Map<String,ByteString> proposedValues = new HashMap<>();
            proposedValues.put("foobar", ByteString.copyFromUtf8("bar" + i));
            proposedValues.put("fizzbuzz", ByteString.copyFromUtf8("boo" + i));

            futures.add(client.propose(lh, proposedValues));
        }

        for (CompletableFuture<Map<String,ByteString>> f : futures) {
            Map<String,ByteString> m = f.get(60, TimeUnit.SECONDS);
            assertTrue("Should have foobar key", m.containsKey("foobar"));
            assertTrue("Should have fizzbuzz", m.containsKey("fizzbuzz"));
            assertEquals("Add should be same", futures.get(0).get(), m);
        }
    }

    private static PaxosValue pv(String value, WriterId writer) {
        return new PaxosValue(ByteString.copyFromUtf8(value), writer);
    }

    @Test
    public void testWinnerSelection() throws Exception {
        WriterId w1 = new WriterId(0, new UUID(0, 1));
        WriterId w2 = new WriterId(1, new UUID(0, 1));
        WriterId w3 = new WriterId(0, new UUID(0, 2));
        WriterId w4 = new WriterId(3, new UUID(0, 1));

        Set<Map<String,PaxosValue>> results = ImmutableSet.of(
                ImmutableMap.of("flipflop", pv("zzz", w4)),
                ImmutableMap.of("foobar", pv("aaa", w1),
                                "barfoo", pv("bbb", w2)),
                ImmutableMap.of("foobar", pv("ccc", w3),
                                "barfoo", pv("ddd", w2),
                                "flipflop", pv("xxxx", w1)),
                ImmutableMap.of("barfoo", pv("eee", w1)));
        Map<String,ByteString> defaults = ImmutableMap.of(
                "fizzbuzz", ByteString.copyFromUtf8("fff"));

        Map<String,ByteString> result = PaxosClient.findWinners(results,
                                                                defaults);

        for (Map.Entry<String,ByteString> e : result.entrySet()) {
            LOG.info("result  {} -> {}", e.getKey(), e.getValue().toStringUtf8());
        }
        assertEquals("Highest writer should win in each case",
                ImmutableMap.of(
                        "foobar", ByteString.copyFromUtf8("ccc"),
                        "barfoo", ByteString.copyFromUtf8("ddd"),
                        "fizzbuzz", ByteString.copyFromUtf8("fff"),
                        "flipflop", ByteString.copyFromUtf8("zzz")),
                result);
    }

    @Test
    public void testGet() throws Exception {
        LedgerHandle lh = createLedger();

        PaxosClient writer = new PaxosClient(bkc);
        PaxosClient reader = new PaxosClient(bkc);

        Map<String,ByteString> proposedValues = new HashMap<>();
        proposedValues.put("foobar", ByteString.copyFromUtf8("barbob"));
        proposedValues.put("fizzbuzz", ByteString.copyFromUtf8("bamboo"));

        assertEquals("Original proposed values are set",
                     proposedValues,
                     writer.propose(lh, proposedValues).get());
        assertEquals("Reader client can read",
                     proposedValues.get("foobar"),
                     reader.get(lh, "foobar").get().get());
        assertEquals("Reader client can read",
                     proposedValues.get("fizzbuzz"),
                     reader.get(lh, "fizzbuzz").get().get());
        assertEquals("Writer client can read",
                     proposedValues.get("foobar"),
                     reader.get(lh, "foobar").get().get());
        assertEquals("Writer client can read",
                     proposedValues.get("fizzbuzz"),
                     reader.get(lh, "fizzbuzz").get().get());
    }
}
