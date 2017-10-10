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

public class PaxosClient {
    private static final Logger LOG = LoggerFactory.getLogger(
            PaxosClient.class);

    private static final int MAX_TIMEOUT_MS = 5000;
    final ScheduledExecutorService scheduler;
    final BookieClient bookieClient;
    final Random random;

    public PaxosClient(BookKeeper bkc) {
        this.scheduler = bkc.scheduler; // assumes this is single threaded
        this.bookieClient = bkc.bookieClient;
        random = new Random();
    }

    public CompletableFuture<Map<String,ByteString>> propose(
            LedgerHandle ledger, Map<String, ByteString> values) {
        final CompletableFuture<Map<String,ByteString>> future
            = new CompletableFuture<>();

        Context context = new Context(
                ledger.metadata.currentEnsemble, ledger.getId(),
                ledger.metadata.getPassword(), values, future);
        NewWriterOp newWriterOp = new NewWriterOp(
                context, new WriterId(0, UUID.randomUUID()));
        scheduler.submit(newWriterOp);

        return future;
    }

    public CompletableFuture<Optional<ByteString>> get(
            LedgerHandle ledger, String key) {
        final CompletableFuture<Optional<ByteString>> future
            = new CompletableFuture<>();
        GetValuesOp op = new GetValuesOp(
                ledger.getId(),
                ledger.metadata.currentEnsemble,
                ledger.metadata.getPassword(),
                key, future);
        scheduler.submit(op);
        return future;
    }

    static int majoritySize(int ensembleSize) {
        return (int)Math.ceil((ensembleSize+0.1)/2);
    }

    private class Context {
        final List<BookieSocketAddress> bookies;
        final long ledgerId;
        final byte[] masterKey;
        final Map<String,ByteString> proposedValues;
        final CompletableFuture<Map<String,ByteString>> future;

        Context(List<BookieSocketAddress> bookies,
                long ledgerId, byte[] masterKey,
                Map<String,ByteString> proposedValues,
                CompletableFuture<Map<String,ByteString>> future) {
            this.bookies = bookies;
            this.ledgerId = ledgerId;
            this.masterKey = masterKey;
            this.proposedValues = proposedValues;
            this.future = future;
        }

        int majoritySize() {
            return PaxosClient.majoritySize(bookies.size());
        }

        void success(Map<String,ByteString> values) {
            LOG.info("{} success", this);
            future.complete(values);
        }

        void error(Throwable t) {
            LOG.error("{} error", this, t);
            future.completeExceptionally(t);
        }

        void retry(WriterId writer) {
            int timeout = random.nextInt(MAX_TIMEOUT_MS);
            LOG.info("{} retrying with writer {} in {}ms",
                     this, writer, timeout);
            scheduler.schedule(new NewWriterOp(this, writer),
                               timeout,
                               TimeUnit.MILLISECONDS);
        }

        @Override
        public String toString() {
            return String.format("Ctx(ledger=%d,majority=%d)",
                                 ledgerId, majoritySize());
        }
    }

    static Map<String,ByteString> findWinners(
            Collection<Map<String,PaxosValue>> returned,
            Map<String,ByteString> defaults) {
        Map<String,ByteString> winningValues
            = returned.stream().flatMap(
                    m -> m.entrySet().stream())
            // merge maps, always taking the largest writer
            .sorted(Map.Entry.comparingByValue())
            .collect(Collectors.toMap(
                             Map.Entry::getKey,
                             Map.Entry::getValue,
                             (v1, v2) -> v2)) // sorted ascending, v2 is higher
            // strip the writer ids out of the map
            .entrySet().stream()
            .collect(Collectors.toMap(
                             Map.Entry::getKey,
                             e -> e.getValue().getValue()));
        Map<String,ByteString> merged = new HashMap<>(defaults);
        merged.putAll(winningValues);
        return merged;
    }

    private class NewWriterOp implements Runnable {
        private final Context context;
        private final WriterId writerId;

        private Map<BookieSocketAddress,Map<String,PaxosValue>> responses
            = new HashMap<>();
        private boolean complete = false;

        NewWriterOp(Context context,
                    WriterId writerId) {
            this.context = context;
            this.writerId = writerId;
        }

        @Override
        public void run() {
            LOG.info("{} sending requests", this);
            for (final BookieSocketAddress addr : context.bookies) {
                bookieClient.setNewWriter(
                        addr,
                        context.ledgerId,
                        context.masterKey, writerId,
                        context.proposedValues.keySet(),
                        (int rc, WriterId higherWriter,
                         Map<String,PaxosValue> currentValues,
                         Object ctx) -> {
                            handleResponse(addr, rc,
                                           higherWriter, currentValues);
                        }, null);
            }
        }

        private synchronized void handleResponse(
                BookieSocketAddress addr,
                int rc, WriterId higherWriter,
                Map<String,PaxosValue> currentValues) {
            if (complete) { return; }
            if (rc == BKException.Code.OK) {
                responses.put(addr, currentValues);
                LOG.info("{}, got OK response from {}, {} OK responses so far",
                         this, addr, responses.size());
                if (responses.size() >= context.majoritySize()) {
                    proposeValues(responses.values());
                    complete = true;
                }
            } else if (rc == BKException.Code.OldWriterException) {
                context.retry(writerId.surpass(higherWriter));
                complete = true;
            } else {
                context.error(BKException.create(rc));
                complete = true;
            }
        }

        private void proposeValues(
                Collection<Map<String,PaxosValue>> responses) {
            Map<String,ByteString> winningValues
                = findWinners(responses, context.proposedValues);
            scheduler.submit(
                    new ProposeValuesOp(context, writerId, winningValues));
        }

        @Override
        public String toString() {
            return String.format("NewWriterOp(ctx=%s,writer=%s)",
                                 context, writerId);
        }
    }

    private class ProposeValuesOp implements Runnable {

        private final Context context;
        private final WriterId writerId;
        private final Map<String,ByteString> values;

        private boolean complete = false;
        private final Set<BookieSocketAddress> respondedOk;

        ProposeValuesOp(Context context,
                        WriterId writerId,
                        Map<String,ByteString> values) {
            this.context = context;
            this.writerId = writerId;
            this.values = values;
            this.respondedOk = new HashSet<>();
        }

        @Override
        public void run() {
            LOG.info("{} sending requests", this);
            for (final BookieSocketAddress addr : context.bookies) {
                bookieClient.proposeValues(
                        addr,
                        context.ledgerId,
                        context.masterKey, writerId,
                        values,
                        (int rc, WriterId higherWriter, Object ctx) -> {
                            handleResponse(addr, rc, higherWriter);
                        }, null);
            }
        }

        private synchronized void handleResponse(
                BookieSocketAddress addr, int rc, WriterId higherWriter) {
            if (complete) { return; }
            if (rc == BKException.Code.OK) {
                respondedOk.add(addr);

                LOG.info("{}, got OK response from {}, {} OK responses so far",
                         this, addr, respondedOk.size());

                if (respondedOk.size() >= context.majoritySize()) {
                    complete = true;

                    // this can occur in background, once accepted
                    // by a majority, the values can't change
                    scheduler.submit(new CommitValuesOp(context, values));

                    context.success(values);
                }
            } else if (rc == BKException.Code.OldWriterException) {
                complete = true;
                context.retry(writerId.surpass(higherWriter));
            } else {
                complete = true;
                context.error(BKException.create(rc));
            }
        }

        @Override
        public String toString() {
            return String.format("ProposeValuesOp(ctx=%s,writer=%s)",
                                 context, writerId);
        }
    }

    private class CommitValuesOp implements Runnable {
        private final Context context;
        private final Map<String,ByteString> values;

        CommitValuesOp(Context context,
                       Map<String,ByteString> values) {
            this.context = context;
            this.values = values;
        }

        @Override
        public void run() {
            LOG.info("{} sending requests", this);
            for (final BookieSocketAddress addr : context.bookies) {
                bookieClient.commitValues(
                        addr,
                        context.ledgerId,
                        context.masterKey,
                        values,
                        (int rc, Object ctx) -> {
                            LOG.debug("{}, got response ({}: {} from {}",
                                      this, rc,
                                      BKException.getMessage(rc), addr);
                        }, null);
            }
        }

        @Override
        public String toString() {
            return String.format("CommitValuesOp(ctx=%s)", context);
        }
    }

    private class GetValuesOp implements Runnable {
        final long ledgerId;
        final List<BookieSocketAddress> bookies;
        final byte[] masterKey;
        final String key;
        final Set<String> keys;
        final CompletableFuture<Optional<ByteString>> future;
        int responses;
        boolean complete;

        GetValuesOp(long ledgerId, List<BookieSocketAddress> bookies,
                    byte[] masterKey,
                    String key,
                    CompletableFuture<Optional<ByteString>> future) {
            this.ledgerId = ledgerId;
            this.bookies = bookies;
            this.masterKey = masterKey;
            this.key = key;
            this.keys = new HashSet<>();
            this.keys.add(key);
            this.future = future;
            responses = 0;
            complete = false;
        }

        @Override
        public void run() {
            LOG.info("{} sending requests", this);
            for (final BookieSocketAddress addr : bookies) {
                bookieClient.getCommittedValues(
                        addr, ledgerId, masterKey, keys,
                        (int rc, Map<String,ByteString> values, Object ctx) -> {
                            handleResponse(rc, values);
                        }, null);
            }
        }

        private synchronized void handleResponse(int rc,
                                                 Map<String,ByteString> values) {
            if (complete) { return; }
            responses++;
            if (rc == BKException.Code.OK
                && values.containsKey(key)) {
                complete = true;
                future.complete(Optional.of(values.get(key)));
            } else if (responses >= majoritySize(bookies.size())) {
                future.complete(Optional.empty());
            }
        }

        @Override
        public String toString() {
            return String.format("GetValuesOp(ledger=%s)", ledgerId);
        }

    }
}
