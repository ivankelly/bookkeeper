package org.apache.bookkeeper.tools.cli.commands.client;

import static org.apache.bookkeeper.proto.BookieProtocol.FLAG_RECOVERY_ADD;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import java.util.concurrent.CompletableFuture;
import java.util.ArrayList;
import java.util.List;

import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.apache.bookkeeper.client.DistributionSchedule;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.util.ByteBufList;

public class LedgerRewriter {
    private final long ledgerId;
    private final LedgerMetadata metadata;
    private final DistributionSchedule distSchedule;
    private final DigestManager digests;
    private final List<BookieSocketAddress> ensemble;
    private final BookieClient bookieClient;

    public LedgerRewriter(long ledgerId, LedgerMetadata metadata,
                          DistributionSchedule distSchedule,
                          DigestManager digests,
                          BookieClient bookieClient) {
        this.ledgerId = ledgerId;
        this.metadata = metadata;
        this.distSchedule = distSchedule;
        this.digests = digests;
        this.ensemble = metadata.getAllEnsembles().get(0L);
        this.bookieClient = bookieClient;
    }

    public CompletableFuture<Void> write(long entryId, long lac,
                                         long length,
                                         ByteBuf buffer) {
        DistributionSchedule.WriteSet writeSet = distSchedule.getWriteSet(entryId);

        ByteBufList toSend = digests.computeDigestAndPackageForSending(
                entryId, lac, length, buffer);
        try {
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (int i = 0; i < writeSet.size(); i++) {
                BookieSocketAddress bookie = ensemble.get(writeSet.get(i));
                CompletableFuture<Void> bookieAddFuture = new CompletableFuture<>();
                futures.add(bookieAddFuture);
                bookieClient.addEntry(bookie, ledgerId,
                                      metadata.getPassword(), entryId, toSend,
                                      (rc, ledgerId, entryId2, addr, ctx) -> {
                                          if (rc != BKException.Code.OK) {
                                              bookieAddFuture.completeExceptionally(
                                                      BKException.create(rc));
                                          } else {
                                              bookieAddFuture.complete(null);
                                          }
                                      }, null, FLAG_RECOVERY_ADD, false, WriteFlag.NONE);
            }
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .whenComplete((ignore, exception) -> {
                        ReferenceCountUtil.release(toSend);
                    });
        } finally {
            writeSet.recycle();
        }
    }
}
