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
package org.apache.bookkeeper.client;

import java.io.Closeable;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.versioning.Version;

public interface MetadataStorage extends Closeable {
    LedgerFuture<Versioned<byte[]>> read(String key);
    LedgerFuture<Version> write(String key, byte[] value, Version v);
    LedgerFuture<Void> delete(String key, Version version);

    public class NoKeyException extends Exception {
        private static final long serialVersionUID = 1L;
    }

    public class BadVersionException extends Exception {
        private static final long serialVersionUID = 1L;
    }

    public class FatalException extends Exception {
        private static final long serialVersionUID = 1L;
    }

    public class ClosedException extends Exception {
        private static final long serialVersionUID = 1L;
    }
}
