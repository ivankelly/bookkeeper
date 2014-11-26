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
package org.apache.bookkeeper.client.ledgerset;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.MetadataStorage;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestZookeeperMetadataStore {
    private final static Logger LOG = LoggerFactory.getLogger(TestZookeeperMetadataStore.class);

    private final ZooKeeperUtil zookeeperServer = new ZooKeeperUtil();

    @Before
    public void setup() throws Exception {
        zookeeperServer.startServer();
    }

    @After
    public void teardown() throws Exception {
        zookeeperServer.killServer();
    }

    @Test
    public void testCRUD() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.setProperty(CompatZookeeperMetadataStorage.ZK_SERVERS,
                         zookeeperServer.getZooKeeperConnectString());
        CompatZookeeperMetadataStorage t = new CompatZookeeperMetadataStorage(conf, "ledgerSets");

        byte[] bytes1 = "test1".getBytes();
        Version v1 = t.write("key1", bytes1, Version.NEW).get();

        Versioned<byte[]> vv1 = t.read("key1").get();
        assertTrue("Values should match", Arrays.equals(bytes1,
                                                               vv1.getValue()));
        assertEquals("Versions should match", vv1.getVersion(), v1);

        byte[] bytes2 = "test2".getBytes();
        Version v2 = t.write("key1", bytes2, v1).get();

        Versioned<byte[]> vv2 = t.read("key1").get();
        assertTrue("Values should match", Arrays.equals(bytes2,
                                                        vv2.getValue()));
        assertEquals("Versions should match", vv2.getVersion(), v2);

        t.delete("key1", v2).get();

        try {
            t.read("key1").get();
            fail("Key shouldn't exist");
        } catch (ExecutionException ee) {
            // correct behaviour
            assertEquals(MetadataStorage.NoKeyException.class, ee.getCause().getClass());
        }
        t.close();
    }
}
