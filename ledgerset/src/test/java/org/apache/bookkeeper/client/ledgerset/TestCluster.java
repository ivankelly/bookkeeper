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

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;

class TestCluster extends BookKeeperClusterTestCase {
    public TestCluster(int numBookies) {
        super(numBookies);
    }

    public BookKeeper getClient() {
        return bkc;
    }

    public ClientConfiguration getBaseClientConfiguration() {
        return baseClientConf;
    }

    public ZooKeeperUtil getZooKeeperUtil() {
        return zkUtil;
    }
}
