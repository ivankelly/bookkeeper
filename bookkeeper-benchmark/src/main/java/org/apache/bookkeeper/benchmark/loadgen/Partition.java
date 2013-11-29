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
 *
 */
package org.apache.bookkeeper.benchmark.loadgen;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.List;
import java.util.ArrayList;
import java.util.Queue;
import java.util.Map;
import java.util.HashMap;
import java.util.Enumeration;
import java.util.ConcurrentModificationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.ArrayDeque;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.DeleteCallback;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Partition implements CreateCallback, OpenCallback, AddCallback,
                           ReadCallback, CloseCallback, DeleteCallback {
    static final Logger LOG = LoggerFactory.getLogger(Partition.class);

    final static BookKeeper.DigestType BK_DIGEST_TYPE = BookKeeper.DigestType.MAC;
    final static byte[] BK_PASSWD = "TestPasswd".getBytes();
    final static int MAX_ENTRIES_PER_READ = 10000;
    final static int MAX_ENTRIES_PER_LEDGER = 1000000;

    final static int BK_ENSEMBLE_SIZE = 3;
    final static int BK_ACK_QUORUM_SIZE = 2;
    final static int BK_WRITE_QUORUM_SIZE = 3;

    enum State {
        INIT, OPENING, READING, ROLLING, NEWLEDGER, WRITING, ERROR
    };

    final PartitionState pstate;
    State state;
    final BookKeeper bk;
    final Queue<byte[]> messageQueue;
    final List<Long> previousLedgers;
    final Queue<Long> toRead;
    LedgerHandle currentLedger = null;

    Partition(BookKeeper bookkeeper, ClusterState cluster, int partitionId) {
        pstate = cluster.getPartitionState(partitionId);
        state = State.INIT;
        bk = bookkeeper;
        
        messageQueue = new ConcurrentLinkedQueue<byte[]>();
        previousLedgers = new ArrayList<Long>();
        toRead = new ArrayDeque<Long>();
    }

    void writeMessage(byte[] message) throws Exception {
        messageQueue.add(message);
        
        synchronized (this) {
            if (state == State.WRITING) {
                drainMessageQueue();
            } else if (state == State.INIT) {
                acquire();
            } else if (state == State.ERROR) {
                throw new Exception("Partition is in error state");
            }
        }
    }

    void kill() {
        synchronized (this) {
            state = State.ERROR;
        }
    }

    private void acquire() {
        synchronized (this) {
            if (state == State.INIT) {
                previousLedgers.addAll(pstate.getLedgers());
                toRead.addAll(previousLedgers);
                readNextLedger();
            } else {
                LOG.error("Tried to set acquire while in {} state (should be INIT)", state);
                state = State.ERROR;
            }
        }                    
    }

    private void readNextLedger() {
        synchronized (this) {
            if (toRead.size() > 0) {
                if (state == State.INIT
                    || state == State.READING) {
                    state = State.OPENING;
                }
                bk.asyncOpenLedger(toRead.remove(), BK_DIGEST_TYPE, BK_PASSWD, this, null);
            } else {
                if (state == State.READING
                    || state == State.INIT) {
                    bk.asyncCreateLedger(BK_ENSEMBLE_SIZE, BK_WRITE_QUORUM_SIZE, BK_ACK_QUORUM_SIZE,
                                         BK_DIGEST_TYPE, BK_PASSWD, this, null);
                    state = State.NEWLEDGER;
                } else {
                    LOG.error("Tried to set create while in {} state (should be READING)", state);
                    state = State.ERROR;
                }
            }
        }
    }

    private void drainMessageQueue() {
        while (messageQueue.size() != 0) {
            synchronized (this) {
                if (state != State.WRITING) {
                    return;
                }
                if (messageQueue.size() > 0) {
                    byte[] msg = messageQueue.remove();
                    currentLedger.asyncAddEntry(msg, this, null);
                }
            }
        }
    }

    @Override
    public void createComplete(int rc, LedgerHandle lh, Object ctx) {
        synchronized (this) {
            if (state == State.NEWLEDGER && rc == BKException.Code.OK) {
                pstate.addNewLedger(lh.getId(), previousLedgers);
                previousLedgers.add(lh.getId());

                currentLedger = lh;
                state = State.WRITING;
            } else {
                state = State.ERROR;
            }
        }
        drainMessageQueue();
    }
            
    @Override
    public void openComplete(int rc, LedgerHandle lh, Object ctx) {
        synchronized (this) {
            if (rc != BKException.Code.OK || state != State.OPENING) {
                if (state != State.ERROR) {
                    LOG.error("Error opening ledger {}", rc);
                }
                state = State.ERROR;
                return;
            }
            state = State.READING;
        }
        long lastEntry = Math.min(lh.getLastAddConfirmed(), (long)MAX_ENTRIES_PER_READ);
        lh.asyncReadEntries(0, lastEntry, this, null);
    }

    @Override
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
        if (rc != BKException.Code.OK) {
            synchronized (this) {
                if (state != State.ERROR) {
                    LOG.error("Error writing to ledger {}", rc);
                }
                state = State.ERROR;
            }
            return;
        }
        if (entryId > MAX_ENTRIES_PER_LEDGER) {
            synchronized (this) {
                if (state == State.WRITING) {
                    state = State.ROLLING;
                }
                if (state == State.ROLLING
                    && lh.getLastAddPushed() == lh.getLastAddConfirmed()) { 
                    LOG.info("Starting to roll");
                    lh.asyncClose(this, null);
                }
            }
        }
    }

    @Override
    public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq,
                             Object ctx) {
        synchronized (this) {
            if (state != State.READING || rc != BKException.Code.OK) {
                LOG.error("Error reading {}, state {}", rc, state);
            }
        }
        long lastEntryRead = -1;
        while (seq.hasMoreElements()) {
            lastEntryRead = seq.nextElement().getEntryId();
        }
        LOG.info("lastEntryRead {} LAC {}", lastEntryRead, lh.getLastAddConfirmed());
        if (lastEntryRead >= lh.getLastAddConfirmed()) {
            synchronized (this) {
                assert (state == State.READING);
                if (toRead.size() > 0) {
                    readNextLedger();
                } else {
                    bk.asyncCreateLedger(BK_ENSEMBLE_SIZE, BK_WRITE_QUORUM_SIZE, BK_ACK_QUORUM_SIZE,
                                         BK_DIGEST_TYPE, BK_PASSWD, this, null);
                    state = State.NEWLEDGER;
                }
            }
        } else {
            long firstEntry = lastEntryRead + 1;
            long lastEntry = Math.min(lastEntryRead + MAX_ENTRIES_PER_READ, lh.getLastAddConfirmed());

            lh.asyncReadEntries(firstEntry, lastEntry, this, null);
        }
    }

    @Override
    public void deleteComplete(int rc, Object ctx) {
        if (rc != BKException.Code.OK) {
            LOG.warn("Error deleting ledger: {}", rc);
        }
    }

    @Override
    public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
        synchronized (this) {
            if (rc != BKException.Code.OK || state != State.ROLLING) {
                if (state != State.ERROR) {
                    LOG.error("Error closing ledger {}", rc);
                }
                state = State.ERROR;
            }
            bk.asyncCreateLedger(BK_ENSEMBLE_SIZE, BK_WRITE_QUORUM_SIZE, BK_ACK_QUORUM_SIZE,
                                 BK_DIGEST_TYPE, BK_PASSWD, this, null);
            state = State.NEWLEDGER;
        }
    }

    public static void main(String[] args) throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setZkServers("localhost:2181");
        BookKeeper bk = new BookKeeper(conf);
        ClusterState cs = new ClusterState(1);
        Partition p = new Partition(bk, cs, 0);
        byte[] msg = "Foobar".getBytes();
        for (int i = 0; i < 10000000; i++) {
            if ((i % 1000) == 0) {
                LOG.info("wrote {}", i);
            }
            p.writeMessage(msg);
        }
        p.kill();
        BookKeeper bk2 = new BookKeeper(conf);
        Partition p2 = new Partition(bk, cs, 0);
        boolean seenError = false;
        while (true) {
            try {
                if (!seenError) {
                    p.writeMessage(msg);
                }
            } catch (Exception e) {
                seenError = true;
                LOG.info("Caught exception as planned");
            }
            p2.writeMessage(msg);
        }
    }
}

