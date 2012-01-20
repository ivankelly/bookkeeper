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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the file handle for a ledger's index file that maps entry ids to location.
 * It is used by LedgerCache.
 *
 * <p>
 * Ledger index file is made of a header and several fixed-length index pages, which records the offsets of data stored in entry loggers
 * <pre>&lt;header&gt;&lt;index pages&gt;</pre>
 * <b>Header</b> is formated as below:
 * <pre>&lt;magic bytes&gt;&lt;len of master key&gt;&lt;master key&gt;</pre>
 * <ul>
 * <li>magic bytes: 4 bytes, 'BKLE', version: 4 bytes
 * <li>len of master key: indicates length of master key. -1 means no master key stored in header.
 * <li>master key: master key
 * </ul>
 * <b>Index page</b> is a fixed-length page, which contains serveral entries which point to the offsets of data stored in entry loggers.
 * </p>
 */
class FileInfo {
    static Logger LOG = LoggerFactory.getLogger(FileInfo.class);

    static final int NO_MASTER_KEY = -1;

    private FileChannel fc;
    private final File lf;
    byte[] masterKey;

    /**
     * The fingerprint of a ledger index file
     */
    static final public int signature = ByteBuffer.wrap("BKLE".getBytes()).getInt();
    static final public int headerVersion = 0;

    static final long START_OF_DATA = 1024;
    private long size;
    private int useCount;
    private boolean isClosed;

    public FileInfo(File lf, byte[] masterKey) throws IOException {
        this.lf = lf;

        this.masterKey = masterKey;
    }

    //     if (size == 0) {
    //         fc.write(ByteBuffer.wrap(header));
    //         // write NO_MASTER_KEY, which means there is no master key
    //         ByteBuffer buf = ByteBuffer.allocate(4);
    //         buf.putInt(NO_MASTER_KEY);
    //         buf.flip();
    //         fc.write(buf);
    //     }
    // }

    synchronized private void checkOpen(boolean create) throws IOException {
        if (fc != null) {
            return;
        }
        boolean exists = lf.exists();
        if (masterKey == null && !exists) {
            throw new IOException(lf + " not found");
        }
        ByteBuffer bb = ByteBuffer.allocate(1024);
        if (!exists) { 
            if (create) {
                fc = new RandomAccessFile(lf, "rw").getChannel();
                size = fc.size();
                if (size == 0) {
                    bb.putInt(signature);
                    bb.putInt(headerVersion);
                    bb.putInt(masterKey.length);
                    bb.put(masterKey);
                    bb.rewind();
                    fc.write(bb);
                }
            }
        } else {
            fc = new RandomAccessFile(lf, "rw").getChannel();
            size = fc.size();
            
            while(bb.hasRemaining()) {
                fc.read(bb);
            }
            bb.flip();
            if (bb.getInt() != signature) {
                throw new IOException("Missing ledger signature");
            }
            int version = bb.getInt();
            if (version != headerVersion) {
                throw new IOException("Incompatible ledger version " + version);
            }
            int length = bb.getInt();
            if (length < 0 || length > bb.remaining()) {
                throw new IOException("Length " + length + " is invalid");
            }
            masterKey = new byte[length];
            bb.get(masterKey);
        }
    }

    synchronized public long size() throws IOException {
        checkOpen(false);
        long rc = size-START_OF_DATA;
        if (rc < 0) {
            rc = 0;
        }
        return rc;
    }

    synchronized public int read(ByteBuffer bb, long position) throws IOException {
        return readAbsolute(bb, position + START_OF_DATA);
    }

    private int readAbsolute(ByteBuffer bb, long start) throws IOException {
        checkOpen(false);
        int total = 0;
        while(bb.remaining() > 0) {
            int rc = fc.read(bb, start);
            if (rc <= 0) {
                throw new IOException("Short read");
            }
            total += rc;
            // should move read position
            start += rc;
        }
        return total;
    }

    synchronized public void close() throws IOException {
        isClosed = true;
        if (useCount == 0 && fc != null) {
            fc.close();
        }
    }

    synchronized public long write(ByteBuffer[] buffs, long position) throws IOException {
        checkOpen(true);
        long total = 0;
        try {
            fc.position(position+START_OF_DATA);
            while(buffs[buffs.length-1].remaining() > 0) {
                long rc = fc.write(buffs);
                if (rc <= 0) {
                    throw new IOException("Short write");
                }
                total += rc;
            }
        } finally {
            fc.force(true);
            long newsize = position+START_OF_DATA+total;
            if (newsize > size) {
                size = newsize;
            }
        }
        return total;
    }

    synchronized public byte[] getMasterKey() throws IOException {
        checkOpen(false);
        return masterKey;
    }

    synchronized public void use() {
        useCount++;
    }

    synchronized public void release() {
        useCount--;
        if (isClosed && useCount == 0 && fc != null) {
            try {
                fc.close();
            } catch (IOException e) {
                LOG.error("Error closing file channel", e);
            }
        }
    }

    public boolean delete() {
        return lf.delete();
    }
}
