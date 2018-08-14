package org.apache.bookkeeper.client;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import com.google.common.util.concurrent.RateLimiter;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.*;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FatClientBench {

    static class Args {
        @Parameter(names = { "--zookeeper", "-z" }, description = "zookeeper ensemble")
        String zookeeper = "zk+hierarchical://localhost:2181/ledgers";

        @Parameter(names = { "--rate", "-r" }, description = "Target rate")
        int rate = 1000;

        @Parameter(names = { "--latency-file" }, description = "File to write latencies")
        String latencyFile = "/tmp/big-fat-client.latences";
    }

    static final Logger LOG = LoggerFactory.getLogger(FatClientBench.class);

    static class LatencyLogger {
        final DataOutputStream os;
        final long initialTimestamp;

        LatencyLogger(String file) throws Exception {
            os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
            initialTimestamp = System.nanoTime();
        }

        void logLatency(long startTime, long endTime) {
            long latency = System.nanoTime() - startTime;
            try {
                os.writeLong(startTime - initialTimestamp);
                os.writeLong(latency);
            } catch (Exception e) {
                LOG.error("Error writing latency", e);
                System.exit(4);
            }
        }
    }

    public static void main(String[] argv) throws Exception {
        Args args = new Args();
        JCommander jCommander = new JCommander(args);
        jCommander.setProgramName("FatClientBench");
        jCommander.parse(argv);

        RateLimiter limiter = RateLimiter.create(args.rate);

        LatencyLogger logger = new LatencyLogger(args.latencyFile);

        ClientConfiguration bkConf = new ClientConfiguration();
        bkConf.setMetadataServiceUri(args.zookeeper);
        bkConf.setThrottleValue(0);
        bkConf.setAddEntryTimeout(10);
        bkConf.setReadEntryTimeout(10);
        bkConf.setSpeculativeReadTimeout(10);
        bkConf.setNumChannelsPerBookie(16);
        bkConf.setUseV2WireProtocol(true);
        bkConf.setEnableDigestTypeAutodetection(true);
        bkConf.setLedgerManagerFactoryClassName(HierarchicalLedgerManagerFactory.class.getName());
        BookKeeper bk = new BookKeeper(bkConf);

        ByteBuf payload = PooledByteBufAllocator.DEFAULT.directBuffer(1024);
        int[] pattern = {0xde, 0xad, 0xbe, 0xef};
        for (int i = 0; i < payload.capacity(); i++) {
            payload.writeByte(pattern[i % pattern.length]);
        }

        payload.retain(); // don't let it get cleaned up

        LedgerHandle lh = bk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32C, new byte[0]);
        while (true) {
            limiter.acquire();

            long startTime = System.nanoTime();
            lh.asyncAddEntry(payload, (rc, lh2, entryId, ctx) -> {
                    if (rc == BKException.Code.OK) {
                        logger.logLatency(startTime, System.nanoTime());
                    }
                }, null);
        }
    }
}
