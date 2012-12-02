/*
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

package org.apache.bookkeeper.bookie;

import java.util.concurrent.ArrayBlockingQueue;

import org.apache.bookkeeper.conf.ServerConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SlabAllocator {
    final static Logger LOG = LoggerFactory.getLogger(SlabAllocator.class);

    ArrayBlockingQueue<LedgerSlab> cleanSlabs;

    SlabAllocator(ServerConfiguration conf) {
        cleanSlabs = new ArrayBlockingQueue<LedgerSlab>(10000);
        for (int i = 0; i < 10000; i++) {
            cleanSlabs.offer(new LedgerSlab());
        }
    }

    LedgerSlab getCleanSlab(long ledgerId) throws InterruptedException {
        LedgerSlab s = cleanSlabs.take();
        LOG.debug("Allocating slab, free slabs {}", cleanSlabs.size());

        s.reset(ledgerId);
        return s;
    }

    void freeSlab(LedgerSlab slab) throws InterruptedException {
        cleanSlabs.put(slab);
        LOG.debug("Freeing slab, free slabs {}", cleanSlabs.size());
    }
}
