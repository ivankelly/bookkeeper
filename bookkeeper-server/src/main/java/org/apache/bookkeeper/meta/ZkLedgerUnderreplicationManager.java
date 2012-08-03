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

package org.apache.bookkeeper.meta;

import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.proto.DataFormats.LedgerRereplicationLayoutFormat;
import org.apache.bookkeeper.proto.DataFormats.UnderreplicatedLedgerFormat;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooDefs.Ids;

import com.google.protobuf.TextFormat;

import java.nio.charset.Charset;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.List;
import java.util.Collections;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkLedgerUnderreplicationManager implements LedgerUnderreplicationManager {
    static final Logger LOG = LoggerFactory.getLogger(ZkLedgerUnderreplicationManager.class);
    static final Charset UTF8 = Charset.forName("UTF-8");

    static final String LAYOUT="BASIC";
    static final int LAYOUT_VERSION=1;

    private static class Lock {
        private final String lockZNode;
        private final int ledgerZNodeVersion;

        Lock(String lockZNode, int ledgerZNodeVersion) {
            this.lockZNode = lockZNode;
            this.ledgerZNodeVersion = ledgerZNodeVersion;
        }

        String getLockZNode() { return lockZNode; }
        int getLedgerZNodeVersion() { return ledgerZNodeVersion; }
    };
    private final Map<Long, Lock> heldLocks = new ConcurrentHashMap<Long, Lock>();
    private final Pattern idExtractionPattern;
    private final Pattern subdirPattern;

    private final String basePath;
    private final String urLedgerPath;
    private final String urLockPath;
    private final String layoutZNode;

    private final ZooKeeper zkc;

    public ZkLedgerUnderreplicationManager(AbstractConfiguration conf, ZooKeeper zkc)
            throws KeeperException, InterruptedException, ReplicationException.CompatibilityException {
        basePath = conf.getZkLedgersRootPath() + "/underreplication";
        layoutZNode = basePath + "/LAYOUT";
        urLedgerPath = basePath + "/ledgers";
        urLockPath = basePath + "/locks";

        idExtractionPattern = Pattern.compile("\\p{XDigit}{4}/\\p{XDigit}{4}/\\p{XDigit}{4}/\\p{XDigit}{4}/urL(\\d+)$");
        subdirPattern = Pattern.compile("^\\p{XDigit}{4}$");
        this.zkc = zkc;

        checkLayout();
    }

    private void createOptimistic(String path, byte[] data) throws KeeperException, InterruptedException {
        try {
            zkc.create(path, data,
                       Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NoNodeException nne) {
            int lastSlash = path.lastIndexOf('/');
            if (lastSlash <= 0) {
                throw nne;
            }
            
            String parent = path.substring(0, lastSlash);
            createOptimistic(path, new byte[0]);
            zkc.create(path, data,
                       Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }        
    }

    private void checkLayout()
            throws KeeperException, InterruptedException, ReplicationException.CompatibilityException {
        if (zkc.exists(basePath, false) == null) {
            try {
                zkc.create(basePath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException nee) {
                // do nothing, someone each could have created it
            }
        }
        while (true) {
            if (zkc.exists(layoutZNode, false) == null) {
                LedgerRereplicationLayoutFormat.Builder builder
                    = LedgerRereplicationLayoutFormat.newBuilder();
                builder.setType(LAYOUT).setVersion(LAYOUT_VERSION);
                try {
                    zkc.create(layoutZNode, TextFormat.printToString(builder.build()).getBytes(UTF8),
                               Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException nne) {
                    // someone else managed to create it
                    continue;
                }
            } else {
                byte[] layoutData = zkc.getData(layoutZNode, false, null);

                LedgerRereplicationLayoutFormat.Builder builder
                    = LedgerRereplicationLayoutFormat.newBuilder();

                try {
                    TextFormat.merge(new String(layoutData, UTF8), builder);
                    LedgerRereplicationLayoutFormat layout = builder.build();
                    if (!layout.getType().equals(LAYOUT)
                            || layout.getVersion() != LAYOUT_VERSION) {
                        throw new ReplicationException.CompatibilityException(
                                "Incompatible layout found (" + LAYOUT + ":" + LAYOUT_VERSION + ")");
                    }
                } catch (TextFormat.ParseException pe) {
                    throw new ReplicationException.CompatibilityException(
                            "Invalid data found", pe);
                }
                break;
            }
        }
        if (zkc.exists(urLedgerPath, false) == null) {
            try {
                zkc.create(urLedgerPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException nee) {
                // do nothing, someone each could have created it
            }
        }
        if (zkc.exists(urLockPath, false) == null) {
            try {
                zkc.create(urLockPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException nee) {
                // do nothing, someone each could have created it
            }
        }
    }

    private long getLedgerId(String path) throws NumberFormatException {
        Matcher m = idExtractionPattern.matcher(path);
        if (m.find()) {
            return Long.valueOf(m.group(1));
        } else {
            throw new NumberFormatException("Couldn't find ledgerid in path");
        }
    }

    private String getParentZnodePath(String base, long ledgerId) {
        String subdir1 = Long.toString(ledgerId >> 48 & 0xffff, 16);
        String subdir2 = Long.toString(ledgerId >> 32 & 0xffff, 16);
        String subdir3 = Long.toString(ledgerId >> 16 & 0xffff, 16);
        String subdir4 = Long.toString(ledgerId & 0xffff, 16);
        
        return String.format("%s/%s/%s/%s/%s",
                             base, subdir1, subdir2, subdir3, subdir4);
    }

    private String getUrLedgerZnode(long ledgerId) {
        return String.format("%s/urL%010d", getParentZnodePath(urLedgerPath, ledgerId), ledgerId);
    }


    @Override
    public void markLedgerUnderreplicated(long ledgerId, String missingReplica)
            throws ReplicationException.UnavailableException {
        try {
            String znode = getUrLedgerZnode(ledgerId);
            while (true) {
                UnderreplicatedLedgerFormat.Builder builder = UnderreplicatedLedgerFormat.newBuilder();
                try {
                    builder.addReplica(missingReplica);
                    createOptimistic(znode,
                                     TextFormat.printToString(builder.build()).getBytes(UTF8));
                } catch (KeeperException.NodeExistsException nee) {
                    Stat s = zkc.exists(znode, false);
                    if (s == null) {
                        continue;
                    }
                    try {
                        byte[] bytes = zkc.getData(znode, false, s);
                        TextFormat.merge(new String(bytes, UTF8), builder);
                        UnderreplicatedLedgerFormat data = builder.build();
                        for (String r : data.getReplicaList()) {
                            if (r.equals(missingReplica)) {
                                break; // nothing to add
                            }
                        }
                        builder.addReplica(missingReplica);
                        zkc.setData(znode,
                                    TextFormat.printToString(builder.build()).getBytes(UTF8),
                                    s.getVersion());
                    } catch (KeeperException.NoNodeException nne) {
                        continue;
                    } catch (KeeperException.BadVersionException bve) {
                        continue;
                    } catch (TextFormat.ParseException pe) {
                        throw new ReplicationException.UnavailableException(
                                "Invalid data found", pe);
                    }
                }
                break;
            }
        } catch (KeeperException ke) {
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        }
    }

    @Override
    public void markLedgerComplete(long ledgerId) throws ReplicationException.UnavailableException {
        LOG.debug("markLedgerComplete(ledgerId={})", ledgerId);
        try {
            Lock l = heldLocks.get(ledgerId);
            if (l != null) {
                zkc.delete(getUrLedgerZnode(ledgerId), l.getLedgerZNodeVersion());
            }
        } catch (KeeperException.NoNodeException nne) {
            // this is ok
        } catch (KeeperException.BadVersionException bve) {
            // if this is the case, some has marked the ledger
            // for rereplication again. Leave the underreplicated
            // znode in place, so the ledger is checked.
        } catch (KeeperException ke) {
            LOG.error("Error deleting underreplicated ledger znode", ke);
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while contacting zookeeper", ie);
        } finally {
            releaseLedger(ledgerId);
        }
    }

    private long getLedgerToRereplicateFromHierarchy(String parent, long depth, Watcher w)
            throws KeeperException, InterruptedException {
        if (depth == 4) {
            List<String> children = zkc.getChildren(parent, w);
            
            Collections.shuffle(children);

            while (children.size() > 0) {
                String tryChild = children.get(0);
                try {
                    String lockPath = urLockPath + "/" + tryChild;
                    Stat stat = zkc.exists(parent + "/" + tryChild, false);
                    if (stat == null) {
                        children.remove(tryChild);
                        continue;
                    }

                    long ledgerId = getLedgerId(tryChild);
                    zkc.create(lockPath, new byte[0],
                               Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    heldLocks.put(ledgerId, new Lock(lockPath, stat.getVersion()));
                    return ledgerId;
                } catch (KeeperException.NodeExistsException nee) {
                    children.remove(tryChild);
                } catch (NumberFormatException nfe) {
                    children.remove(tryChild);
                }
            }
            return -1;
        }

        List<String> children = zkc.getChildren(parent, w);
        Collections.shuffle(children);

        while (children.size() > 0) {
            String tryChild = parent + "/" + children.get(0);
            long ledger = getLedgerToRereplicateFromHierarchy(tryChild, depth + 1, w);
            if (ledger != -1) {
                return ledger;
            }
        }
        return -1;
    }

    @Override
    public long getLedgerToRereplicate() throws ReplicationException.UnavailableException {
        LOG.debug("getLedgerToRereplicate()");
        try {
            while (true) {
                final CountDownLatch changedLatch = new CountDownLatch(1);
                Watcher w = new Watcher() {
                        public void process(WatchedEvent e) {
                            if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                                changedLatch.countDown();
                            }
                        }
                    };
                long ledger = getLedgerToRereplicateFromHierarchy(urLedgerPath, 0, w);
                if (ledger != -1) {
                    return ledger;
                }
                // nothing found, wait for a watcher to trigger
                changedLatch.await();
            }
        } catch (KeeperException ke) {
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while conecting zookeeper", ie);
        }
    }

    @Override
    public void releaseLedger(long ledgerId) throws ReplicationException.UnavailableException {
        LOG.debug("releaseLedger(ledgerId={})", ledgerId);
        try {
            Lock l = heldLocks.remove(ledgerId);
            if (l != null) {
                zkc.delete(l.getLockZNode(), -1);
            }
        } catch (KeeperException.NoNodeException nne) {
            // this is ok
        } catch (KeeperException ke) {
            LOG.error("Error deleting underreplicated ledger lock", ke);
            throw new ReplicationException.UnavailableException("Error contacting zookeeper", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ReplicationException.UnavailableException("Interrupted while conecting zookeeper", ie);
        }
    }
}
