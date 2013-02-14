/**
 * Copyright The Apache Software Foundation
 *
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
package org.apache.bookkeeper.bookie;

import java.util.Comparator;
import java.io.Serializable;

public class EntryKey {
    long ledgerId;
    long entryId;

    public EntryKey() {
        this(0, 0);
    }

    public EntryKey(long ledgerId, long entryId) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public long getEntryId() {
        return entryId;
    }

    /**
    * Comparator for the key portion
    */
    final public static KeyComparator COMPARATOR = new KeyComparator();

    // Only compares the key portion
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof EntryKey)) {
          return false;
        }
        EntryKey key = (EntryKey)other;
        return ledgerId == key.ledgerId &&
            entryId == key.entryId;
    }

    @Override
    public int hashCode() {
        return (int)(ledgerId * 13 ^ entryId * 17);
    }
}

/**
* Compare EntryKey.
*/
class KeyComparator implements Comparator<EntryKey>, Serializable {
    @Override
    public int compare(EntryKey left, EntryKey right) {
        long ret = left.ledgerId - right.ledgerId;
        if (ret == 0) {
            ret = left.entryId - right.entryId;
        }
        return (ret < 0)? -1 : ((ret > 0)? 1 : 0);
    }
}
