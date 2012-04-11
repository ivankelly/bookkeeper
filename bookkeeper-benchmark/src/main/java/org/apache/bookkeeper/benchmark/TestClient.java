package org.apache.bookkeeper.benchmark;
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


import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.HashMap;

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.KeeperException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

/**
 * This is a simple test program to compare the performance of writing to
 * BookKeeper and to the local file system.
 *
 */

public class TestClient
    implements AddCallback, ReadCallback {
    private static final Logger LOG = LoggerFactory.getLogger(TestClient.class);

    BookKeeper x;
    LedgerHandle lh;
    Integer entryId;
    HashMap<Integer, Integer> map;

    OutputStream fStream;
    FileOutputStream fStreamLocal;
    long start, lastId;

    public TestClient() {
        entryId = 0;
        map = new HashMap<Integer, Integer>();
    }

    public TestClient(String servers) throws KeeperException, IOException, InterruptedException {
        this();
        x = new BookKeeper(servers);
        try {
            lh = x.createLedger(DigestType.MAC, new byte[] {'a', 'b'});
        } catch (BKException e) {
            LOG.error(e.toString());
        }
    }

    public TestClient(String servers, int ensSize, int qSize)
            throws KeeperException, IOException, InterruptedException {
        this();
        x = new BookKeeper(servers);
        try {
            lh = x.createLedger(ensSize, qSize, DigestType.MAC, new byte[] {'a', 'b'});
        } catch (BKException e) {
            LOG.error(e.toString());
        }
    }

    public TestClient(OutputStream fStream)
            throws FileNotFoundException {
        this.fStream = fStream;
        this.fStreamLocal = new FileOutputStream("./local.log");
    }


    public Integer getFreshEntryId(int val) {
        ++this.entryId;
        synchronized (map) {
            map.put(this.entryId, val);
        }
        return this.entryId;
    }

    public boolean removeEntryId(Integer id) {
        boolean retVal = false;
        synchronized (map) {
            map.remove(id);
            retVal = true;

            if(map.size() == 0) map.notifyAll();
            else {
                if(map.size() < 4)
                    LOG.error(map.toString());
            }
        }
        return retVal;
    }

    public void closeHandle() throws KeeperException, InterruptedException, BKException {
        lh.close();
    }
    /**
     * First says if entries should be written to BookKeeper (0) or to the local
     * disk (1). Second parameter is an integer defining the length of a ledger entry.
     * Third parameter is the number of writes.
     *
     * @param args
     */
    public static void main(String[] args) {

        int length = Integer.parseInt(args[1]);
        StringBuilder sb = new StringBuilder();
        while(length-- > 0) {
            sb.append('a');
        }

        Integer selection;
        try {
            selection = Integer.parseInt(args[0]);
        } catch(NumberFormatException e) {
            if (args[0].equals("bk")) {
                selection = 0;
            } else if (args[0].equals("fs")) {
                selection = 1;
            } else if (args[0].equals("hdfs")) {
                selection = 2;
            } else {
                selection = -1;
            }
                
        }
        switch(selection) {
        case 0:
            StringBuilder servers_sb = new StringBuilder();
            for (int i = 4; i < args.length; i++) {
                servers_sb.append(args[i] + " ");
            }

            String servers = servers_sb.toString().trim().replace(' ', ',');
            try {
                TestClient c = new TestClient(servers, Integer.parseInt(args[3]), Integer.parseInt(args[4]));
                c.writeSameEntryBatch(sb.toString().getBytes(), Integer.parseInt(args[2]));
                //c.writeConsecutiveEntriesBatch(Integer.parseInt(args[0]));
                c.closeHandle();
            } catch (Exception e) {
                LOG.error("Exception occurred", e);
            } 
            break;

        case 1:
            try {
                TestClient c = new TestClient(new FileOutputStream(args[2]));
                c.writeSameEntryBatchFS(sb.toString().getBytes(), Integer.parseInt(args[3]));
            } catch(FileNotFoundException e) {
                LOG.error("File not found", e);
            }
            break;

        case 2:
            try {
                FileSystem fs = FileSystem.get(new Configuration());
                TestClient c = new TestClient(fs.create(new Path(args[2])));
                c.writeSameEntryBatchFS(sb.toString().getBytes(), Integer.parseInt(args[3]));
            } catch(IOException e) {
                LOG.error("File not found", e);
            }
            break;

        default:
            System.err.println("Unknown option: " + args[0]);
            System.exit(2);
        }
    }

    void writeSameEntryBatch(byte[] data, int times) throws InterruptedException {
        start = System.currentTimeMillis();
        int count = times;
        LOG.debug("Data: " + new String(data) + ", " + data.length);
        while(count-- > 0) {
            lh.asyncAddEntry(data, this, this.getFreshEntryId(2));
        }
        LOG.debug("Finished " + times + " async writes in ms: " + (System.currentTimeMillis() - start));
        synchronized (map) {
            if(map.size() != 0)
                map.wait();
        }
        LOG.debug("Finished processing in ms: " + (System.currentTimeMillis() - start));

        LOG.debug("Ended computation");
    }

    void writeConsecutiveEntriesBatch(int times) throws InterruptedException {
        start = System.currentTimeMillis();
        int count = times;
        while(count-- > 0) {
            byte[] write = new byte[2];
            int j = count%100;
            int k = (count+1)%100;
            write[0] = (byte) j;
            write[1] = (byte) k;
            lh.asyncAddEntry(write, this, this.getFreshEntryId(2));
        }
        LOG.debug("Finished " + times + " async writes in ms: " + (System.currentTimeMillis() - start));
        synchronized (map) {
            if(map.size() != 0)
                map.wait();
        }
        LOG.debug("Finished processing writes (ms): " + (System.currentTimeMillis() - start));

        Integer mon = Integer.valueOf(0);
        synchronized(mon) {
            lh.asyncReadEntries(1, times - 1, this, mon);
            mon.wait();
        }
        LOG.error("Ended computation");
    }

    void writeSameEntryBatchFS(byte[] data, int times) {
        int count = times;
        LOG.debug("Data: " + data.length + ", " + times);
        try {
            start = System.currentTimeMillis();
            while(count-- > 0) {
                fStream.write(data);
                fStreamLocal.write(data);
                fStream.flush();
                if (fStream instanceof FSDataOutputStream) {
                    ((FSDataOutputStream)fStream).sync();
                }
            }
            fStream.close();
            System.out.println("Finished processing writes (ms): " + (System.currentTimeMillis() - start));
        } catch(IOException e) {
            LOG.error("IOException occurred", e);
        }
    }


    @Override
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
        this.removeEntryId((Integer) ctx);
    }

    @Override
    public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
        System.out.println("Read callback: " + rc);
        while(seq.hasMoreElements()) {
            LedgerEntry le = seq.nextElement();
            LOG.debug(new String(le.getEntry()));
        }
        synchronized(ctx) {
            ctx.notify();
        }
    }
}
