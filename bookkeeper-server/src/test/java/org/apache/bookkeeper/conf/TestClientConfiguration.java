/*
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

package org.apache.bookkeeper.conf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.junit.Test;

/**
 * Unit test of {@link ClientConfiguration}.
 */
public class TestClientConfiguration {

    private static ArrayList<BookieSocketAddress> createServers(int numBookies) {
        ArrayList<BookieSocketAddress> servers = Lists.newArrayListWithExpectedSize(numBookies);
        int basePort = 3181;
        for (int i = 0; i < numBookies; i++) {
            servers.add(new BookieSocketAddress("127.0.0.1", basePort + i));
        }
        return servers;
    }

    private final ClientConfiguration conf;

    public TestClientConfiguration() {
        this.conf = new ClientConfiguration();
    }

    @Test
    public void testNoBootstrapBookies() {
        assertNull(conf.getClientBootstrapBookies());
    }

    @Test
    public void testGetBootstrapBookies() {
        List<BookieSocketAddress> servers = createServers(3);
        conf.setProperty(
            "clientBootstrapBookies",
            "127.0.0.1:3181,127.0.0.1:3182,127.0.0.1:3183");
        assertEquals(servers, conf.getClientBootstrapBookies());
    }

    @Test
    public void testSetBootstrapBookies() {
        List<BookieSocketAddress> servers = createServers(3);
        conf.setClientBootstrapBookies(servers);
        assertEquals(
            Lists.newArrayList("127.0.0.1:3181", "127.0.0.1:3182", "127.0.0.1:3183"),
            conf.getProperty("clientBootstrapBookies"));
    }

    @Test
    public void testGetSetBootstrapBookies() {
        List<BookieSocketAddress> servers = createServers(3);
        conf.setClientBootstrapBookies(servers);
        List<BookieSocketAddress> readServers = conf.getClientBootstrapBookies();
        assertEquals(servers, readServers);
    }

}
