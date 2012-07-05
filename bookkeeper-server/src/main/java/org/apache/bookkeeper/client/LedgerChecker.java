/**
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
import java.util.HashSet;
import java.util.Set;
import java.util.Map;

import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;

import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 *Checks the complete ledger and finds the UnderReplicated fragments if any
 */
public class LedgerChecker {
    private static Logger LOG = LoggerFactory.getLogger(LedgerChecker.class);

    public final BookieClient bookieClient;

    static class InvalidFragmentException extends Exception {
        private static final long serialVersionUID = 1467201276417062353L;
    }

    /**
     * This will collect all the entry read call backs and finally it will give
     * call back to previous call back API which is waiting for it once it meets
     * the expected call backs from down
     */
    private static class ReadManyEntriesCallback implements ReadEntryCallback {
        AtomicBoolean completed = new AtomicBoolean(false);
        final AtomicLong numEntries;
        final LedgerFragment fragment;
        final GenericCallback<LedgerFragment> cb;

        ReadManyEntriesCallback(long numEntries, LedgerFragment fragment,
                GenericCallback<LedgerFragment> cb) {
            this.numEntries = new AtomicLong(numEntries);
            this.fragment = fragment;
            this.cb = cb;
        }

        public void readEntryComplete(int rc, long ledgerId, long entryId,
                ChannelBuffer buffer, Object ctx) {
            if (rc == BKException.Code.OK) {
                if (numEntries.decrementAndGet() == 0
                        && !completed.getAndSet(true)) {
                    cb.operationComplete(rc, fragment);
                }
            } else if (!completed.getAndSet(true)) {
                cb.operationComplete(rc, fragment);
            }
        }
    }

    public LedgerChecker(BookKeeper bkc) {
        bookieClient = bkc.getBookieClient();
    }

    private void verifyLedgerFragment(LedgerFragment fragment,
            GenericCallback<LedgerFragment> cb) throws InvalidFragmentException {
        long firstStored = fragment.getFirstStoredEntryId();
        long lastStored = fragment.getLastStoredEntryId();

        if (firstStored == LedgerHandle.INVALID_ENTRY_ID) {
            if (lastStored != LedgerHandle.INVALID_ENTRY_ID) {
                throw new InvalidFragmentException();
            }
            cb.operationComplete(BKException.Code.OK, fragment);
            return;
        }
        if (firstStored == lastStored) {
            ReadManyEntriesCallback manycb = new ReadManyEntriesCallback(1,
                    fragment, cb);
            bookieClient.readEntry(fragment.getAddress(), fragment
                    .getLedgerId(), firstStored, manycb, null);
        } else {
            ReadManyEntriesCallback manycb = new ReadManyEntriesCallback(2,
                    fragment, cb);
            bookieClient.readEntry(fragment.getAddress(), fragment
                    .getLedgerId(), firstStored, manycb, null);
            bookieClient.readEntry(fragment.getAddress(), fragment
                    .getLedgerId(), lastStored, manycb, null);
        }
    }

    /**
     * This will collect all the fragment read call backs and finally it will
     * give call back to above call back API which is waiting for it once it
     * meets the expected call backs from down
     */
    private static class FullLedgerCallback implements
            GenericCallback<LedgerFragment> {
        final Set<LedgerFragment> badFragments;
        final AtomicLong numFragments;
        final GenericCallback<Set<LedgerFragment>> cb;

        FullLedgerCallback(long numFragments,
                GenericCallback<Set<LedgerFragment>> cb) {
            badFragments = new HashSet<LedgerFragment>();
            this.numFragments = new AtomicLong(numFragments);
            this.cb = cb;
        }

        public void operationComplete(int rc, LedgerFragment result) {
            if (rc != BKException.Code.OK) {
                badFragments.add(result);
            }
            if (numFragments.decrementAndGet() == 0) {
                cb.operationComplete(BKException.Code.OK, badFragments);
            }
        }
    }

    /**
     * Check that all the fragments in the passed in ledger, and report those
     * which are missing.
     */
    public void checkLedger(LedgerHandle lh,
            GenericCallback<Set<LedgerFragment>> cb) {
        // build a set of all fragment replicas
        Set<LedgerFragment> fragments = new HashSet<LedgerFragment>();

        Long curEntryId = null;
        ArrayList<InetSocketAddress> curEnsemble = null;
        for (Map.Entry<Long, ArrayList<InetSocketAddress>> e : lh
                .getLedgerMetadata().getEnsembles().entrySet()) {
            if (curEntryId != null) {
                for (int i = 0; i < curEnsemble.size(); i++) {
                    fragments.add(new LedgerFragment(lh.getId(), curEntryId, e
                            .getKey() - 1, i, curEnsemble, lh
                            .getDistributionSchedule()));
                }
            }
            curEntryId = e.getKey();
            curEnsemble = e.getValue();
        }

        if (curEntryId != null) {
            for (int i = 0; i < curEnsemble.size(); i++) {
                fragments.add(new LedgerFragment(lh.getId(), curEntryId, lh
                        .getLastAddConfirmed(), i, curEnsemble, lh
                        .getDistributionSchedule()));
            }
        }

        // verify all the collected fragment replicas
        FullLedgerCallback allFragmentsCb = new FullLedgerCallback(fragments
                .size(), cb);
        for (LedgerFragment r : fragments) {
            try {
                verifyLedgerFragment(r, allFragmentsCb);
            } catch (InvalidFragmentException ife) {
                LOG.error("Invalid fragment found : {}", r);
                allFragmentsCb.operationComplete(
                        BKException.Code.IncorrectParameterException, r);
            }
        }
    }
}