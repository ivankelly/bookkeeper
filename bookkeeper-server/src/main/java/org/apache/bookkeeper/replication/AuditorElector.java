/**
 *
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
package org.apache.bookkeeper.replication;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.io.Serializable;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performing auditor election using Apache ZooKeeper. Using ZooKeeper as a
 * coordination service, when a bookie bids for auditor, it creates an ephemeral
 * sequential file (znode) on ZooKeeper and considered as their vote. Vote
 * format is 'V_sequencenumber'. Election will be done by comparing the
 * ephemeral sequential numbers and the bookie which has created the least znode
 * will be elected as Auditor. All the other bookies will be watching on their
 * predecessor znode according to the ephemeral sequence numbers.
 */
public class AuditorElector {
    private static final Logger LOG = LoggerFactory
            .getLogger(AuditorElector.class);
    // Represents the index of the auditor node
    private static final int AUDITOR_INDEX = 0;
    // Represents vote prefix
    private static final String VOTE_PREFIX = "V_";
    // Represents path Separator
    private static final String PATH_SEPARATOR = "/";
    // Represents urLedger path in zk
    private final String basePath;
    // Represents auditor election path in zk
    private final String electionPath;

    private final String bookieId;
    private final ServerConfiguration conf;
    private final ZooKeeper zkc;

    private String myVote;
    Auditor auditor;
    private volatile boolean running = true;

    /**
     * AuditorElector for performing the auditor election
     * 
     * @param bookieId
     *            - bookie identifier, comprises HostAddress:Port
     * @param conf
     *            - configuration
     * @param zkc
     *            - ZK instance
     * @throws UnavailableException
     *             throws unavailable exception while initializing the elector
     */
    public AuditorElector(String bookieId, ServerConfiguration conf,
            ZooKeeper zkc) throws UnavailableException {
        this.bookieId = bookieId;
        this.conf = conf;
        this.zkc = zkc;
        basePath = conf.getZkLedgersRootPath() + "/underreplication";
        electionPath = basePath + "/auditorelection";
        createElectorPath();
    }

    /**
     * Performing the auditor election using the ZooKeeper ephemeral sequential
     * znode. The bookie which has created the least sequential will be elect as
     * Auditor.
     * 
     * @throws UnavailableException
     *             when performing auditor election
     * 
     */
    public void doElection() throws UnavailableException {
        try {
            // creating my vote in zk. Vote format is 'V_numeric'
            createMyVote();
            List<String> children = zkc.getChildren(getVotePath(""), false);

            if (0 >= children.size()) {
                throw new IllegalArgumentException(
                        "Atleast one bookie server should present to elect the Auditor!");
            }

            // sorting in ascending order of sequential number
            Collections.sort(children, new ElectionComparator());
            String voteNode = StringUtils.substringAfterLast(myVote,
                    PATH_SEPARATOR);

            // starting Auditing service
            if (children.get(AUDITOR_INDEX).equals(voteNode)) {
                synchronized(this) {
                    if (!running) {
                        return;
                    }
                    // update the auditor bookie id in the election path. This is
                    // done for debugging purpose
                    zkc.setData(getVotePath(""), bookieId.getBytes(), -1);

                    auditor = new Auditor(bookieId, conf, zkc);
                    auditor.start();
                }
            } else {
                // If not an auditor, will be watching to my predecessor and
                // looking the previous node deletion.
                Watcher electionWatcher = new ElectionWatcher();
                int myIndex = children.indexOf(voteNode);
                int prevNodeIndex = myIndex - 1;
                if (null == zkc.exists(getVotePath(PATH_SEPARATOR)
                        + children.get(prevNodeIndex), electionWatcher)) {
                    // While adding, the previous znode doesn't exists.
                    // Again going to election.
                    doElection();
                }
            }
        } catch (KeeperException e) {
            throw new UnavailableException(
                    "Exception while performing auditor election", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UnavailableException(
                    "Interrupted while performing auditor election", e);
        }
    }

    private void createMyVote() throws KeeperException, InterruptedException {
        if (null == myVote || null == zkc.exists(myVote, false)) {
            myVote = zkc.create(getVotePath(PATH_SEPARATOR + VOTE_PREFIX),
                    bookieId.getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
        }
    }

    private String getVotePath(String vote) {
        return electionPath + vote;
    }

    private void createElectorPath() throws UnavailableException {
        try {
            if (zkc.exists(basePath, false) == null) {
                try {
                    zkc.create(basePath, new byte[0], Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException nee) {
                    // do nothing, someone else could have created it
                }
            }
            if (zkc.exists(getVotePath(""), false) == null) {
                try {
                    zkc.create(getVotePath(""), new byte[0],
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException nee) {
                    // do nothing, someone else could have created it
                }
            }
        } catch (KeeperException ke) {
            throw new UnavailableException(
                    "Failed to initialize Auditor Elector", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new UnavailableException(
                    "Failed to initialize Auditor Elector", ie);
        }
    }

    /**
     * Watching the predecessor bookies and will do election on predecessor node
     * deletion or expiration.
     */
    private class ElectionWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {
            if (event.getState() == KeeperState.Disconnected
                    || event.getState() == KeeperState.Expired) {
                LOG.error("Lost ZK connection, shutting down");
                shutdown();
            } else if (event.getType() == EventType.NodeDeleted) {
                try {
                    doElection();
                } catch (UnavailableException e) {
                    LOG.error("Exception when performing Auditor re-election",
                            e);
                    shutdown();
                }
            }
        }
    }

    /**
     * Shutting down AuditorElector
     */
    public void shutdown() {
        synchronized (this) {
            if (!running) {
                return;
            }
            running = false;
        }
        LOG.info("Shutting down AuditorElector");
        try {
            zkc.delete(myVote, -1);
        } catch (InterruptedException ie) {
            LOG.warn("InterruptedException while deleting myVote: " + myVote,
                    ie);
        } catch (KeeperException ke) {
            LOG.warn("Exception while deleting myVote:" + myVote, ke);
        }
        synchronized (this) {
            if (auditor != null) {
                auditor.shutdown();
                auditor = null;
            }
        }
    }

    /**
     * If current bookie is running as auditor, return the status of the
     * auditor. Otherwise return the status of elector.
     * 
     * @return
     */
    public boolean isRunning() {
        if (auditor != null) {
            return auditor.isRunning();
        }
        return running;
    }

    /**
     * Compare the votes in the ascending order of the sequence number. Vote
     * format is 'V_sequencenumber', comparator will do sorting based on the
     * numeric sequence value.
     */
    private static class ElectionComparator
        implements Comparator<String>, Serializable {
        /**
         * Return -1 if the first vote is less than second. Return 1 if the
         * first vote is greater than second. Return 0 if the votes are equal.
         */
        public int compare(String vote1, String vote2) {
            long voteSeqId1 = getVoteSequenceId(vote1);
            long voteSeqId2 = getVoteSequenceId(vote2);
            int result = voteSeqId1 < voteSeqId2 ? -1
                    : (voteSeqId1 > voteSeqId2 ? 1 : 0);
            return result;
        }

        private long getVoteSequenceId(String vote) {
            String voteId = StringUtils.substringAfter(vote, VOTE_PREFIX);
            return Long.parseLong(voteId);
        }
    }
}
