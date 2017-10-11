package org.apache.bookkeeper.client;

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

    CompletableFuture<LedgerHandle> ledger0(List<BookieSocketAddress> bootstrapEnsemble)
            throws GeneralSecurityException {
        return null;
        /*int bookieCount = bootstrapEnsemble.size();
        bk.openLedger
            
        LedgerMetadata metadata = new LedgerMetadata(
                bookieCount, bookieCount,
                PaxosClient.majoritySize(bookieCount),
                DIGEST_TYPE, PASSWORD);
        metadata.addEnsemble(0L, new ArrayList<>(bootstrapEnsemble));

        try {
            return new ReadOnlyLedgerHandle(this, LEDGER_0_ID, metadata,
                                            digestType, passwd, false);
        } catch (GeneralSecurityException e) {
            throw new BKException.BKDigestNotInitializedException();
            }*/
    }

    CompletableFuture<LedgerHandle> readNextLedger(LedgerHandle current) {
        return null;
    }

    static CompletableFuture<LedgerHandle> writeNextLedger(LedgerHandle current) {
        return null;
    }
}
