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
public class ReferencedBookies extends ClientCommand<CliFlags> {

    private static final String NAME = "referenced-bookies";
    private static final String DESC = "List all the bookies referenced by ledgers";
    private static final String DEFAULT = "";

    public ReferencedBookies() {
        this(new CliFlags());
    }

    private ReferencedBookies(CliFlags flags) {
        super(CliSpec.<CliFlags>newBuilder()
                  .withName(NAME)
                  .withDescription(DESC)
                  .withFlags(flags)
                  .build());
    }

    @Override
    protected void run(BookKeeper bk, CliFlags flags) throws Exception {
        Set<BookieSocketAddress> referencedBookies = new HashSet<>();
        try (BookKeeperAdmin admin = new BookKeeperAdmin((org.apache.bookkeeper.client.BookKeeper) bk)) {
            for (long lid : admin.listLedgers()) {
                referencedBookies.addAll(getReferencedBookies(admin, lid));
            }
            int i = 1;
            for (BookieSocketAddress a : referencedBookies) {
                System.out.println(String.format("%d: %s", i++, a));
            }
        } catch (Throwable t) {
            t.printStackTrace();
            throw new UncheckedExecutionException(t.getMessage(), t);
        }
    }

    static Set<BookieSocketAddress> getReferencedBookies(BookKeeperAdmin admin, long ledgerId)
            throws Exception {
        Set<BookieSocketAddress> bookies = new HashSet<>();
        try (LedgerHandle lh = admin.openLedger(ledgerId)) {
            for (List<BookieSocketAddress> ensemble : lh.getLedgerMetadata().getAllEnsembles().values()) {
                bookies.addAll(ensemble);
            }
        }
        return bookies;
    }
}
