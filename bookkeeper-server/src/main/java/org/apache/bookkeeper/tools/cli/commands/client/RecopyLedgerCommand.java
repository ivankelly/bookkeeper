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
import java.io.FileOutputStream;
import java.io.IOException;
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
public class RecopyLedgerCommand extends ClientCommand<RecopyLedgerCommand.Flags> {

    private static final String NAME = "recopy";
    private static final String DESC = "Read and rewrite a ledger, optionally avoiding some bookies";
    private static final String DEFAULT = "";

    private LedgerIdFormatter ledgerIdFormatter;

    public RecopyLedgerCommand() {
        this(new Flags());
    }

    private RecopyLedgerCommand(Flags flags) {
        super(CliSpec.<RecopyLedgerCommand.Flags>newBuilder()
                  .withName(NAME)
                  .withDescription(DESC)
                  .withFlags(flags)
                  .build());
    }

    @Accessors(fluent = true)
    @Setter
    public static class Flags extends CliFlags {

        @Parameter(names = { "-l", "--ledgerid" }, description = "Ledger ID")
        private long ledgerId;

        @Parameter(names = { "-a", "--all" }, description = "Recopy all ledgers")
        private boolean all = false;

        @Parameter(names = { "--avoid-bookies" }, description = "Avoid writing to a set of bookies (comma separated)")
        private String avoid = "";

        @Parameter(names = { "--batch-size" }, description = "Number of entries to copy at a time")
        private long batchSize = 1000;
    }

    @Override
    protected void run(BookKeeper bk, Flags flags) throws Exception {
        try (BookKeeperAdmin admin = new BookKeeperAdmin((org.apache.bookkeeper.client.BookKeeper) bk)) {
            Set<BookieSocketAddress> avoid = new HashSet<>();
            for (String s : flags.avoid.split(",")) {
                avoid.add(new BookieSocketAddress(s));
            }
            if (flags.all) {
                if (avoid.isEmpty()) {
                    System.out.println("No bookies to avoid, so no ledgers copied");
                    return;
                }
                ClientContext ctx = ((org.apache.bookkeeper.client.BookKeeper) bk).getClientCtx();
                LedgerManager lm = ctx.getLedgerManager();
                for (long lid : admin.listLedgers()) {
                    boolean hasBookie = lm.readLedgerMetadata(lid).get()
                        .getValue().getAllEnsembles()
                        .values().stream()
                        .flatMap(ensemble -> ensemble.stream())
                        .anyMatch((b) -> avoid.contains(b));
                    if (hasBookie) {
                        System.out.println("Ledger " + lid + " has bookie in avoidSet, copying");
                        recopyLedger(bk, admin, lid, avoid, flags.batchSize);
                    }
                }
            } else if (flags.ledgerId != -1) {
                recopyLedger(bk, admin, flags.ledgerId, avoid, flags.batchSize);
            } else {
                throw new Exception("Must specify a ledger id or all");
            }
        } catch (Throwable t) {
            t.printStackTrace();
            throw new UncheckedExecutionException(t.getMessage(), t);
        }
    }

    private void recopyLedger(BookKeeper bk, BookKeeperAdmin admin, long ledgerId, Set<BookieSocketAddress> avoid,
                              long batchSize)
            throws Exception {
        CompletableFuture<LedgerHandle> promise = new CompletableFuture<>();
        admin.asyncOpenLedger(ledgerId, (rc, lh, ignore) -> {
                if (rc != BKException.Code.OK) {
                    promise.completeExceptionally(BKException.create(rc));
                } else {
                    promise.complete(lh);
                }
            }, null);
        ClientContext ctx = ((org.apache.bookkeeper.client.BookKeeper) bk).getClientCtx();
        LedgerManager lm = ctx.getLedgerManager();
        LedgerMetadataSerDe serDe = new LedgerMetadataSerDe();
        try (LedgerHandle rh = promise.get()) {
            Versioned<LedgerMetadata> origVersionedMetadata = rh.getVersionedLedgerMetadata();
            LedgerMetadata origMetadata = origVersionedMetadata.getValue();

            try (FileOutputStream outf = new FileOutputStream(String.valueOf(rh.getId()) + ".backup.bin")) {
                outf.write(serDe.serialize(origMetadata));
            }

            List<BookieSocketAddress> newEnsemble = ctx.getPlacementPolicy().newEnsemble(
                    origMetadata.getEnsembleSize(), origMetadata.getWriteQuorumSize(),
                    origMetadata.getAckQuorumSize(), origMetadata.getCustomMetadata(),
                    avoid).getResult();
            LedgerMetadata newMetadata = LedgerMetadataBuilder.from(origMetadata)
                .clearEnsembles()
                .newEnsembleEntry(0L, newEnsemble)
                .build();
            LedgerRewriter rewriter = new LedgerRewriter(ledgerId, newMetadata,
                                                         rh.getDistributionSchedule(),
                                                         rh.getDigestManager(),
                                                         ctx.getBookieClient());
            long lac = -1;
            long i = 0;

            for (long eid = 0; eid <= rh.getLastAddConfirmed(); eid += batchSize) {
                try (LedgerEntries entries = rh.read(eid, Math.min(eid + (batchSize - 1), rh.getLastAddConfirmed()))) {
                    List<CompletableFuture<Void>> futures= new ArrayList<>();
                    for (LedgerEntry e : entries) {
                        futures.add(rewriter.write(e.getEntryId(),
                                                   lac, e.getLength(),
                                                   e.getEntryBuffer().retain()));
                        lac = e.getEntryId();
                        i++;
                    }
                    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
                }
            }
            lm.writeLedgerMetadata(rh.getId(), newMetadata,
                                   origVersionedMetadata.getVersion());
            System.out.println("Rewrote " + i + " entries for ledger " + rh.getId());
        }
    }
}
