/*
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
 */
package org.apache.bookkeeper.tools.cli.commands.client;
import org.apache.bookkeeper.client.api.WriteFlag;

import com.beust.jcommander.Parameter;
import com.google.common.util.concurrent.UncheckedExecutionException;

import com.google.common.hash.Hashing;
import com.google.common.hash.Hasher;


import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.concurrent.CompletableFuture;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.bookkeeper.client.*;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerHandleAdv;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.meta.LedgerMetadataSerDe;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.tools.cli.helpers.ClientCommand;
import org.apache.bookkeeper.tools.framework.CliFlags;
import org.apache.bookkeeper.tools.framework.CliSpec;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.LedgerIdFormatter;
import io.netty.buffer.ByteBufAllocator;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookieClient;

/**
 * Command to recopy a given ledger.
 */
public class GenerateChecksums extends ClientCommand<GenerateChecksums.Flags> {

    private static final String NAME = "generate-checksums";
    private static final String DESC = "Read each ledger, generate checksums and store to file";
    private static final String DEFAULT = "";

    public GenerateChecksums() {
        this(new Flags());
    }

    private GenerateChecksums(Flags flags) {
        super(CliSpec.<GenerateChecksums.Flags>newBuilder()
                  .withName(NAME)
                  .withDescription(DESC)
                  .withFlags(flags)
                  .build());
    }

    @Accessors(fluent = true)
    @Setter
    public static class Flags extends CliFlags {
        @Parameter(names = { "--batch-size" }, description = "Number of entries to copy at a time")
        private long batchSize = 1000;
    }

    @Override
    protected void run(BookKeeper bk, Flags flags) throws Exception {
        try (BookKeeperAdmin admin = new BookKeeperAdmin((org.apache.bookkeeper.client.BookKeeper) bk)) {
            for (long lid : admin.listLedgers()) {
                try (FileOutputStream f = new FileOutputStream("ledger-" + lid + ".murmur3")) {
                    f.write(generateChecksum(admin, lid, flags.batchSize));
                }
                System.out.println("Checksum generated for " + lid);
            }
        } catch (Throwable t) {
            t.printStackTrace();
            throw new UncheckedExecutionException(t.getMessage(), t);
        }
    }

    static byte[] generateChecksum(BookKeeperAdmin admin, long ledgerId, long batchSize)
            throws Exception {
        CompletableFuture<LedgerHandle> promise = new CompletableFuture<>();
        admin.asyncOpenLedger(ledgerId, (rc, lh, ignore) -> {
                if (rc != BKException.Code.OK) {
                    promise.completeExceptionally(BKException.create(rc));
                } else {
                    promise.complete(lh);
                }
            }, null);
        Hasher hasher = Hashing.murmur3_128().newHasher();
        try (LedgerHandle rh = promise.get()) {
            hasher.putString(rh.getLedgerMetadata().getDigestType().toString(), StandardCharsets.UTF_8);
            hasher.putBytes(rh.getLedgerMetadata().getPassword());
            for (long eid = 0; eid <= rh.getLastAddConfirmed(); eid += batchSize) {
                try (LedgerEntries entries = rh.read(eid, Math.min(eid + (batchSize - 1), rh.getLastAddConfirmed()))) {
                    for (LedgerEntry e : entries) {
                        hasher.putBytes(e.getEntryBytes());
                        hasher.putLong(e.getLength());
                    }
                }
            }
        }
        return hasher.hash().asBytes();
    }
}
