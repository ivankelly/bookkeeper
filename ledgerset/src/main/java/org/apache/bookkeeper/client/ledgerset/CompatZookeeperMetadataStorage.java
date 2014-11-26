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
package org.apache.bookkeeper.client.ledgerset;

import java.io.IOException;

import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.google.protobuf.ByteString;

import org.apache.commons.configuration.Configuration;
import org.apache.bookkeeper.client.MetadataStorage;
import org.apache.bookkeeper.client.LedgerFuture;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.proto.ZKMetaFormats.MetastoreValueFormat;
import org.apache.bookkeeper.proto.ZKMetaFormats.MetastoreFieldFormat;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.InvalidProtocolBufferException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompatZookeeperMetadataStorage implements MetadataStorage {
    private static final Logger LOG = LoggerFactory.getLogger(CompatZookeeperMetadataStorage.class);
    final static String LEDGERSET_FIELD = "ledgerSetMetadata";

    final static String ZK_TIMEOUT = "zkTimeout";
    final static String ZK_SERVERS = "zkServers";
    public final static String BASE_NODE_CONF = "zkMetastoreBaseNode";
    final static String BASE_NODE_DEFAULT = "/metastore";

    private final String baseNode;
    private final String name;
    private final ZooKeeper zookeeper;
    private final ExecutorService executor;

    private volatile boolean closed = false;
    private final Set<CallbackFuture<?>> outstanding = new HashSet<CallbackFuture<?>>();

    CompatZookeeperMetadataStorage(Configuration conf, String namespace)
            throws KeeperException, IOException, InterruptedException {
        ThreadFactoryBuilder tfb = new ThreadFactoryBuilder()
            .setNameFormat("ZookeeperMetadataStorage-store-%d");
        executor = Executors.newSingleThreadExecutor(tfb.build());

        String zk = conf.getString(ZK_SERVERS, "localhost");
        int timeout = conf.getInt(ZK_TIMEOUT, 10000);
        baseNode = conf.getString(BASE_NODE_CONF, BASE_NODE_DEFAULT);

        ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(timeout);
        zookeeper = new ZooKeeper(zk, timeout, w);
        w.waitForConnection();

        this.name = namespace;
    }

    @Override
    public synchronized void close() throws IOException {
        synchronized (outstanding) {
            closed = true;
            for (CallbackFuture<?> f : outstanding) {
                f.setException(new MetadataStorage.ClosedException());
            }
            outstanding.clear();
        }
        executor.shutdown();
        try {
            zookeeper.close();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted shutting down zookeeper", ie);
        }
    }

    @Override
    public synchronized LedgerFuture<Versioned<byte[]>> read(String key) {
        final String path = getZookeeperPath(key);
        GetCb cb = new GetCb();
        zookeeper.getData(path, false, cb, null);
        return new ForwardingLedgerFuture<Versioned<byte[]>>(cb);
    }

    @Override
    public synchronized LedgerFuture<Version> write(String key, byte[] value, Version version) {
        final String path = getZookeeperPath(key);
        MetastoreValueFormat.Builder mv = MetastoreValueFormat.newBuilder();
        mv.addField(MetastoreFieldFormat.newBuilder()
                    .setKey(LEDGERSET_FIELD)
                    .setValue(ByteString.copyFrom(value)).build());
        byte[] data = mv.build().toByteArray();
        if (version != Version.NEW) {
            int v = ((ZookeeperVersion)version).getVersion();
            PutCb cb = new PutCb();
            zookeeper.setData(path, data, v, cb, null);
            return new ForwardingLedgerFuture<Version>(cb);
        } else {
            CreateCb cb = new CreateCb();
            ZkUtils.asyncCreateFullPathOptimistic(zookeeper, path, data,
                    Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                    cb, null);
            return new ForwardingLedgerFuture<Version>(cb);
        }
    }

    @Override
    public synchronized LedgerFuture<Void> delete(String key, Version version) {
        final String path = getZookeeperPath(key);

        final int v;
        if (version == Version.ANY) {
            v = -1;
        } else if (version instanceof ZookeeperVersion) {
            v = ((ZookeeperVersion)version).getVersion();
        } else {
            throw new IllegalArgumentException("Invalid type for version " + version.getClass());
        }

        DeleteCb cb = new DeleteCb();
        zookeeper.delete(path, v, cb, null);
        return new ForwardingLedgerFuture<Void>(cb);
    }

    void trackFuture(final CallbackFuture<?> future) {
        synchronized (outstanding) {
            if (closed) {
                future.setException(new MetadataStorage.ClosedException());
                return;
            }
            outstanding.add(future);
            future.addListener(new Runnable() {
                    @Override
                    public void run() {
                        synchronized (outstanding) {
                            outstanding.remove(future);
                        }
                    }
                }, executor);
        }
    }

    String getZookeeperPath(final String key) {
        StringBuilder sb = new StringBuilder(getTableZookeeperPath());
        return sb.append("/").append(key).toString();
    }

    String getTableZookeeperPath() {
        return baseNode + "/" + name;
    }

    class CallbackFuture<T> extends AbstractFuture<T> {
        CallbackFuture() {
            trackFuture(this);
        }

        @Override
        public boolean setException(Throwable throwable) {
            return super.setException(throwable);
        }
    }

    class GetCb extends CallbackFuture<Versioned<byte[]>> implements DataCallback {
        @Override
        public void processResult(final int rc, final String path, Object ctx,
                                  final byte[] data, final Stat stat) {
            if (rc == KeeperException.Code.OK.intValue()) {
                try {
                    MetastoreValueFormat mv = MetastoreValueFormat.newBuilder()
                        .mergeFrom(data).build();
                    Version version = new ZookeeperVersion(stat.getVersion());
                    for (MetastoreFieldFormat f : mv.getFieldList()) {
                        if (f.getKey().equals(LEDGERSET_FIELD)) {
                            set(new Versioned<byte[]>(f.getValue().toByteArray(), version));
                        }
                    }
                    setException(new MetadataStorage.NoKeyException());
                } catch (InvalidProtocolBufferException ipbe) {
                    LOG.error("Corrupt metadata in " + path, ipbe);
                    setException(new MetadataStorage.FatalException());
                }
            } else if (rc == KeeperException.Code.NONODE.intValue()) {
                setException(new MetadataStorage.NoKeyException());
            } else {
                LOG.error("Zookeeper operation failed with return code {}", rc);
                setException(new MetadataStorage.FatalException());
            }
        }
    }

    class CreateCb extends CallbackFuture<Version> implements StringCallback {
        @Override
        public void processResult(final int rc, String path, Object ctx, String name) {
            if (rc == KeeperException.Code.OK.intValue()) {
                set(new ZookeeperVersion(0));
            } else if (rc == KeeperException.Code.NODEEXISTS.intValue()) {
                setException(new MetadataStorage.BadVersionException());
            } else {
                LOG.error("Zookeeper operation failed with return code {}", rc);
                setException(new MetadataStorage.FatalException());
            }
        }
    }

    class PutCb extends CallbackFuture<Version> implements StatCallback {
        @Override
        public void processResult(final int rc, String path, Object ctx, final Stat stat) {
            if (rc == KeeperException.Code.OK.intValue()) {
                set(new ZookeeperVersion(stat.getVersion()));
            } else if (rc == KeeperException.Code.BADVERSION.intValue()) {
                setException(new MetadataStorage.BadVersionException());
            } else if (rc == KeeperException.Code.NONODE.intValue()) {
                setException(new MetadataStorage.NoKeyException());
            } else {
                LOG.error("Zookeeper operation failed with return code {}", rc);
                setException(new MetadataStorage.FatalException());
            }
        }
    }

    class DeleteCb extends CallbackFuture<Void> implements VoidCallback {
        @Override
        public void processResult(final int rc, String path, Object ctx) {
            if (rc == KeeperException.Code.OK.intValue()) {
                set(null);
            } else if (rc == KeeperException.Code.BADVERSION.intValue()) {
                setException(new MetadataStorage.BadVersionException());
            } else if (rc == KeeperException.Code.NONODE.intValue()) {
                setException(new MetadataStorage.NoKeyException());
            } else {
                LOG.error("Zookeeper operation failed with return code {}", rc);
                setException(new MetadataStorage.FatalException());
            }
        }
    }

    public static class ZookeeperVersion implements Version {
        int version;

        public ZookeeperVersion(int v) {
            this.version = v;
        }

        public ZookeeperVersion(ZookeeperVersion v) {
            this.version = v.version;
        }

        @Override
        public Occurred compare(Version v) {
            if (null == v) {
                throw new NullPointerException("Version is not allowed to be null.");
            }
            if (v == Version.NEW) {
                return Occurred.AFTER;
            } else if (v == Version.ANY) {
                return Occurred.CONCURRENTLY;
            } else if (!(v instanceof ZookeeperVersion)) {
                throw new IllegalArgumentException("Invalid version type");
            }
            ZookeeperVersion mv = (ZookeeperVersion)v;
            int res = version - mv.version;
            if (res == 0) {
                return Occurred.CONCURRENTLY;
            } else if (res < 0) {
                return Occurred.BEFORE;
            } else {
                return Occurred.AFTER;
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (null == obj ||
                !(obj instanceof ZookeeperVersion)) {
                return false;
            }
            ZookeeperVersion v = (ZookeeperVersion)obj;
            return 0 == (version - v.version);
        }

        @Override
        public String toString() {
            return "zkversion=" + version;
        }

        @Override
        public int hashCode() {
            return version;
        }

        int getVersion() {
            return version;
        }
    }
}
