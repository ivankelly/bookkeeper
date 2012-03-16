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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * Implements a ledger inside a bookie. In particular, it implements operations
 * to write entries to a ledger and read entries from a ledger.
 *
 */
public class LedgerDescriptor {
    static LedgerDescriptor create(byte[] masterKey,
                                   long ledgerId,
                                   EntryLogger entryLogger,
                                   LedgerCache ledgerCache) {
        return new LedgerDescriptorImpl(masterKey, ledgerId, entryLogger, ledgerCache);
    }

    static LedgerDescriptor createReadOnly(long ledgerId,
                                           EntryLogger entryLogger,
                                           LedgerCache ledgerCache) {
        return new LedgerDescriptorReadOnlyImpl(ledgerId, entryLogger, ledgerCache);
    }

    void checkAccess(byte masterKey[]) throws BookieException, IOException;

    long getLedgerId();

    void incRef();
    void decRef();

    void setFenced() throws BookieException;
    boolean isFenced();

    long addEntry(ByteBuffer entry) throws IOException;
    ByteBuffer readEntry(long entryId) throws IOException;
}
