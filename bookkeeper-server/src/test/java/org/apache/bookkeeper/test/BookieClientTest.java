package org.apache.bookkeeper.test;

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

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.Executors;

import com.google.protobuf.ByteString;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.junit.Test;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.DataFormats.AddRequest;
import org.apache.bookkeeper.proto.DataFormats.ReadRequest;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.TestCase;

public class BookieClientTest extends TestCase {
    static Logger LOG = LoggerFactory.getLogger(BookieClientTest.class);
    BookieServer bs;
    File tmpDir;
    public int port = 13645;
    public ClientSocketChannelFactory channelFactory;
    public OrderedSafeExecutor executor;
    ServerConfiguration conf = new ServerConfiguration();

    @Override
    public void setUp() throws Exception {
        tmpDir = File.createTempFile("bookie", "test");
        tmpDir.delete();
        tmpDir.mkdir();

        // Since this test does not rely on the BookKeeper client needing to
        // know via ZooKeeper which Bookies are available, okay, so pass in null
        // for the zkServers input parameter when constructing the BookieServer.
        ServerConfiguration conf = new ServerConfiguration();
        conf.setZkServers(null).setBookiePort(port)
            .setJournalDirName(tmpDir.getPath())
            .setLedgerDirNames(new String[] { tmpDir.getPath() });
        bs = new BookieServer(conf);
        bs.start();
        channelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors
                .newCachedThreadPool());
        executor = new OrderedSafeExecutor(2);
    }

    @Override
    public void tearDown() throws Exception {
        bs.shutdown();
        recursiveDelete(tmpDir);
        channelFactory.releaseExternalResources();
        executor.shutdown();
    }

    private static void recursiveDelete(File dir) {
        File children[] = dir.listFiles();
        if (children != null) {
            for (File child : children) {
                recursiveDelete(child);
            }
        }
        dir.delete();
    }

    static class ResultStruct {
        int rc;
        ByteBuffer entry;
    }

    ReadEntryCallback recb = new ReadEntryCallback() {

        public void readEntryComplete(int rc, long ledgerId, long entryId, ChannelBuffer bb, Object ctx) {
            ResultStruct rs = (ResultStruct) ctx;
            synchronized (rs) {
                rs.rc = rc;
                if (bb != null) {
                    bb.readerIndex(16);
                    rs.entry = bb.toByteBuffer();
                    rs.notifyAll();
                }
            }
        }

    };

    WriteCallback wrcb = new WriteCallback() {
        public void writeComplete(int rc, long ledgerId, long entryId, InetSocketAddress addr, Object ctx) {
            if (ctx != null) {
                synchronized (ctx) {
                    ctx.notifyAll();
                }
            }
        }
    };

    @Test(timeout=60000)
    public void testWriteGaps() throws Exception {
        final Object notifyObject = new Object();
        byte[] passwd = new byte[20];
        Arrays.fill(passwd, (byte) 'a');
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", port);
        ResultStruct arc = new ResultStruct();

        BookieClient bc = new BookieClient(new ClientConfiguration(), channelFactory, executor);
        ChannelBuffer bb;
        AddRequest.Builder addReqBuilder = AddRequest.newBuilder()
            .setMasterKey(ByteString.copyFrom(passwd)).setLedgerId(1L);
        ReadRequest.Builder readReqBuilder = ReadRequest.newBuilder()
            .setLedgerId(1L);

        bb = createByteBuffer(1, 1, 1);
        bc.addEntry(addr, addReqBuilder.setEntryId(1).build(), bb, wrcb, arc);
        synchronized (arc) {
            arc.wait(1000);
            bc.readEntry(addr, readReqBuilder.setEntryId(1).build(), recb, arc);
            arc.wait(1000);
            assertEquals(0, arc.rc);
            assertEquals(1, arc.entry.getInt());
        }
        bb = createByteBuffer(2, 1, 2);
        bc.addEntry(addr, addReqBuilder.setEntryId(2).build(), bb, wrcb, null);
        bb = createByteBuffer(3, 1, 3);
        bc.addEntry(addr, addReqBuilder.setEntryId(3).build(), bb, wrcb, null);
        bb = createByteBuffer(5, 1, 5);
        bc.addEntry(addr, addReqBuilder.setEntryId(5).build(), bb, wrcb, null);
        bb = createByteBuffer(7, 1, 7);
        bc.addEntry(addr, addReqBuilder.setEntryId(7).build(), bb, wrcb, null);
        synchronized (notifyObject) {
            bb = createByteBuffer(11, 1, 11);
            bc.addEntry(addr, addReqBuilder.setEntryId(11).build(), bb, wrcb, notifyObject);
            notifyObject.wait();
        }
        synchronized (arc) {
            bc.readEntry(addr, readReqBuilder.setEntryId(6).build(), recb, arc);
            arc.wait(1000);
            assertEquals(BKException.Code.NoSuchEntryException, arc.rc);
        }
        synchronized (arc) {
            bc.readEntry(addr, readReqBuilder.setEntryId(7).build(), recb, arc);
            arc.wait(1000);
            assertEquals(0, arc.rc);
            assertEquals(7, arc.entry.getInt());
        }
        synchronized (arc) {
            bc.readEntry(addr, readReqBuilder.setEntryId(1).build(), recb, arc);
            arc.wait(1000);
            assertEquals(0, arc.rc);
            assertEquals(1, arc.entry.getInt());
        }
        synchronized (arc) {
            bc.readEntry(addr, readReqBuilder.setEntryId(2).build(), recb, arc);
            arc.wait(1000);
            assertEquals(0, arc.rc);
            assertEquals(2, arc.entry.getInt());
        }
        synchronized (arc) {
            bc.readEntry(addr, readReqBuilder.setEntryId(3).build(), recb, arc);
            arc.wait(1000);
            assertEquals(0, arc.rc);
            assertEquals(3, arc.entry.getInt());
        }
        synchronized (arc) {
            bc.readEntry(addr, readReqBuilder.setEntryId(4).build(), recb, arc);
            arc.wait(1000);
            assertEquals(BKException.Code.NoSuchEntryException, arc.rc);
        }
        synchronized (arc) {
            bc.readEntry(addr, readReqBuilder.setEntryId(11).build(), recb, arc);
            arc.wait(1000);
            assertEquals(0, arc.rc);
            assertEquals(11, arc.entry.getInt());
        }
        synchronized (arc) {
            bc.readEntry(addr, readReqBuilder.setEntryId(5).build(), recb, arc);
            arc.wait(1000);
            assertEquals(0, arc.rc);
            assertEquals(5, arc.entry.getInt());
        }
        synchronized (arc) {
            bc.readEntry(addr, readReqBuilder.setEntryId(10).build(), recb, arc);
            arc.wait(1000);
            assertEquals(BKException.Code.NoSuchEntryException, arc.rc);
        }
        synchronized (arc) {
            bc.readEntry(addr, readReqBuilder.setEntryId(12).build(), recb, arc);
            arc.wait(1000);
            assertEquals(BKException.Code.NoSuchEntryException, arc.rc);
        }
        synchronized (arc) {
            bc.readEntry(addr, readReqBuilder.setEntryId(13).build(), recb, arc);
            arc.wait(1000);
            assertEquals(BKException.Code.NoSuchEntryException, arc.rc);
        }
    }

    private ChannelBuffer createByteBuffer(int i, long lid, long eid) {
        ByteBuffer bb;
        bb = ByteBuffer.allocate(4 + 16);
        bb.putLong(lid);
        bb.putLong(eid);
        bb.putInt(i);
        bb.flip();
        return ChannelBuffers.wrappedBuffer(bb);
    }

    @Test(timeout=60000)
    public void testNoLedger() throws Exception {
        ResultStruct arc = new ResultStruct();
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", port);
        BookieClient bc = new BookieClient(new ClientConfiguration(), channelFactory, executor);
        synchronized (arc) {
            ReadRequest.Builder readReqBuilder = ReadRequest.newBuilder()
                .setLedgerId(2L).setEntryId(13L);

            bc.readEntry(addr, readReqBuilder.build(), recb, arc);
            arc.wait(1000);
            assertEquals(BKException.Code.NoSuchEntryException, arc.rc);
        }
    }
}
