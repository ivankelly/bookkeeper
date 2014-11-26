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

import org.apache.bookkeeper.client.BookKeeper.DigestType;

public class Constants {
    public static final String LEDGERSET_DIGEST_TYPE = "ledgerset.digest";
    public static final DigestType LEDGERSET_DIGEST_TYPE_DEFAULT = DigestType.MAC;
    public static final String LEDGERSET_PASSWD = "ledgerset.password";
    public static final String LEDGERSET_PASSWD_DEFAULT = "defaultPassword";
    public static final String LEDGERSET_ENSEMBLE_SIZE = "ledgerset.ensemble";
    public static final int LEDGERSET_ENSEMBLE_SIZE_DEFAULT = 3;
    public static final String LEDGERSET_ACK_QUORUM = "ledgerset.ack-quorum";
    public static final int LEDGERSET_ACK_QUORUM_DEFAULT = 2;
    public static final String LEDGERSET_WRITE_QUORUM = "ledgerset.write-quorum";
    public static final int LEDGERSET_WRITE_QUORUM_DEFAULT = 3;

    public static final String LEDGERSET_READ_BATCH_SIZE = "ledgerset.read-batch-size";
    public static final int LEDGERSET_READ_BATCH_SIZE_DEFAULT = 50;

    public final static String LEDGERSET_MAX_LEDGER_SIZE_CONFKEY = "ledgerset.maxLedgerSize";
    public final static int LEDGERSET_MAX_LEDGER_SIZE_DEFAULT =  10*1024*1024;
}
