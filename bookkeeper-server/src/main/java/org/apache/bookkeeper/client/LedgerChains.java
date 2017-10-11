package org.apache.bookkeeper.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import com.google.protobuf.ByteString;

import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.WriterId;
import org.apache.bookkeeper.proto.PaxosValue;
import org.apache.bookkeeper.proto.DataFormats.LedgerSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LedgerChains {
    private static final Logger LOG = LoggerFactory.getLogger(
            LedgerChains.class);

    private static long LEDGER_0_ID = 1 << 62;
    private static BookKeeper.DigestType DIGEST_TYPE = BookKeeper.DigestType.CRC32;
    private static byte[] PASSWORD = "Foobar".getBytes();

    private final BookKeeper bk;

    LedgerChains(BookKeeper bk) {
        this.bk = bk;
    }

    CompletableFuture<LedgerHandle> ledger0(List<BookieSocketAddress> bootstrapEnsemble) {
        final CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
        int bookieCount = bootstrapEnsemble.size();
        bk.asyncOpenLedger(LEDGER_0_ID,
                           bootstrapEnsemble, bookieCount, bookieCount,
                           PaxosClient.majoritySize(bookieCount),
                           DIGEST_TYPE, PASSWORD,
                           (rc, ledger, ignore) -> {
                               if (rc != BKException.Code.OK) {
                                   future.completeExceptionally(
                                           BKException.create(rc));
                               } else {
                                   future.complete(ledger);
                               }
                           }, null);
        return future;
    }

    CompletableFuture<LedgerHandle> readNextLedger(LedgerHandle current) {
        return null;
    }

    CompletableFuture<LedgerHandle> writeNextLedger(LedgerHandle current) {
        final CompletableFuture<LedgerHandle> future = new CompletableFuture<>();

        current.asyncClose((rc, ledger, ignore) -> {
                if (rc != BKException.Code.OK) {
                    future.completeExceptionally(BKException.create(rc));
                } else {
                    chainNewLedger(current, future);
                }
            }, null);
        return future;
    }

    private void chainNewLedger(LedgerHandle current,
                                final CompletableFuture<LedgerHandle> future) {
        final UUID writer = UUID.randomUUID();
        LedgerSpec.Builder builder = LedgerSpec.newBuilder();
        builder.setEnsembleSize(current.metadata.getEnsembleSize())
            .setWriteQuorumSize(current.metadata.getWriteQuorumSize())
            .setAckQuorumSize(current.metadata.getAckQuorumSize())
            .setLedgerId(current.getId() + 1)
            .setWriterMsb(writer.getMostSignificantBits())
            .setWriterLsb(writer.getLeastSignificantBits());
        for (BookieSocketAddress bookie: current.metadata.getEnsemble(0)) {
            builder.addBookie(bookie.toString());
        }

        PaxosClient paxos = new PaxosClient(bk);
        Map<String,ByteString> values = new HashMap<>();
        values.put("nextLedger", builder.build().toByteString());

        paxos.propose(current, values).whenComplete(
                (map, throwable) -> {
                    if (throwable != null) {
                        future.completeExceptionally(throwable);
                    } else {
                        checkResult(writer, map, future);
                    }
                });
    }

    private void checkResult(UUID myWriterId,
                             Map<String,ByteString> returnedValues,
                             CompletableFuture<LedgerHandle> future) {
        try {
            LedgerSpec spec = LedgerSpec.newBuilder()
                .mergeFrom(returnedValues.get("nextLedger")).build();
            if (spec.getWriterMsb() != myWriterId.getMostSignificantBits()
                || spec.getWriterLsb() != myWriterId.getLeastSignificantBits()) {
                future.completeExceptionally(
                        new BKException.BKConflictingWriterException());
                return;
            }
            List<BookieSocketAddress> bookies = new ArrayList<>();
            for (String b : spec.getBookieList()) {
                bookies.add(new BookieSocketAddress(b));
            }
            LedgerHandle lh = bk.createLedger(spec.getLedgerId(),
                    bookies, spec.getEnsembleSize(), spec.getWriteQuorumSize(),
                    spec.getAckQuorumSize(), DIGEST_TYPE, PASSWORD);
            future.complete(lh);
        } catch (Exception e) {
            LOG.error("Error reading returned values when setting nextLedger",
                      e);
            future.completeExceptionally(
                    new BKException.BKUnexpectedConditionException());
        }
    }
}
