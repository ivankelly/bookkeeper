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

import java.util.Map;
import java.io.IOException;
import org.apache.bookkeeper.proto.BookieProtocol;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.nio.ByteBuffer;
import javolution.util.FastMap;

class LedgerSlab {
    long ledgerId = -1;
    AtomicLong maxEntryId = new AtomicLong(-1); // TODO remove -1s, with constants
    AtomicLong dataSize = new AtomicLong(0);
    
    FastMap<Long, ByteBuffer> entries
        = new FastMap<Long, ByteBuffer>();


    void reset(long ledgerId) {
        entries.clear();
        ledgerId = ledgerId;
        maxEntryId.set(-1);
        dataSize.set(0);
    }

    boolean isFull() {
        return dataSize.get() > 1024*10; // 10k
    }

    void addEntry(long entryId, ByteBuffer entry) {
        long curMax = maxEntryId.get();
        while (entryId > curMax) {
            maxEntryId.compareAndSet(curMax, entryId);
            curMax = maxEntryId.get();
        }
        entries.put(entryId, entry);
        dataSize.addAndGet(entry.capacity());
    }

    ByteBuffer getEntry(long entryId) {
        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            return entries.get(maxEntryId.get());
        }

        return entries.get(entryId);
    }

    synchronized void flush(EntryLogger entryLog, Map<Long, Long> offsets) throws IOException {
        for (Map.Entry<Long, ByteBuffer> e : entries.entrySet()) {
            long offset = entryLog.addEntry(ledgerId, e.getValue());
            offsets.put(e.getKey(), offset);
        }
        entries.clear();
    }
}
