/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.distributedlog;

import static com.google.common.base.Charsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import com.google.common.util.concurrent.RateLimiter;
import io.reactivex.Flowable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.feature.SettableFeature;
import org.apache.distributedlog.exceptions.*;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.feature.CoreFeatureKeys;
import org.apache.distributedlog.impl.logsegment.BKLogSegmentEntryReader;
import org.apache.distributedlog.util.FailpointUtils;
import org.apache.distributedlog.util.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Cases for RollLogSegments.
 */
public class TestCancelledRead extends TestDistributedLogBase {
    private static final Logger logger = LoggerFactory.getLogger(TestRollLogSegments.class);

    @Test(timeout = 600000)
    public void testWritingAndTailing() throws Exception {
        String name = "writing-and-tailing";
        DistributedLogConfiguration conf = new DistributedLogConfiguration()
            .setReadAheadWaitTime(5000)
            .setOutputBufferSize(0)
            .setCreateStreamIfNotExists(true)
            .setImmediateFlushEnabled(true)
            .setFailFastOnStreamNotReady(true)
            .setPeriodicFlushFrequencyMilliSeconds(0)
            .setLockTimeout(DistributedLogConstants.LOCK_IMMEDIATE)
            .setLogSegmentRollingIntervalMinutes(1);

        CompletableFuture<Void> f = new CompletableFuture<>();
        long entryId = 0;
        
        try (BKDistributedLogManager dlm = (BKDistributedLogManager) createNewDLM(conf, name);
             BKAsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned()) {


            for (int i = 0; i < 20; i++) {
                entryId++;
                try {
                    logger.info("Writing entry {}", entryId);
                    writer.write(DLMTestUtil.getLogRecordInstance(entryId, 100000)).get();
                    Thread.sleep(5000);
                } catch (ExecutionException ee) {
                    if (ee.getCause() instanceof StreamNotReadyException) {
                        logger.info("IKDEBUG stream not ready, must be rolling");
                    } else {
                        logger.error("Error in writer thread", ee);
                        f.completeExceptionally(ee);
                    }
                }
            }
        }

        Thread readThread = new Thread(() -> {
                try {
                    BKDistributedLogManager dlm2 = (BKDistributedLogManager) createNewDLM(conf, name);
                    BKAsyncLogReader reader = (BKAsyncLogReader) dlm2.getAsyncLogReader(DLSN.InitialDLSN);
                    while (true) {
                        reader.readNext().get();
                    }
                } catch (Throwable t) {
                    logger.info("IKDEBUG error reading", t);
                    f.completeExceptionally(t);
                }
        });
        readThread.start();

        logger.info("Pausing to allow catchup");
        Thread.sleep(5000);

        logger.info("Starting writing again");
        try (BKDistributedLogManager dlm = (BKDistributedLogManager) createNewDLM(conf, name);
             BKAsyncLogWriter writer = dlm.startAsyncLogSegmentNonPartitioned()) {
            while (true) {
                entryId++;
                try {
                    writer.write(DLMTestUtil.getLogRecordInstance(entryId, 100000)).get();
                    Thread.sleep(1000);
                } catch (ExecutionException ee) {
                    if (ee.getCause() instanceof StreamNotReadyException) {
                    } else {
                        throw ee;
                    }
                }
                if (f.isDone()) {
                    logger.info("IKDEBUG exiting");
                    f.get();
                }
            }
        }
    }
}
