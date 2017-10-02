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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.proto.BookieClient;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GetBookieInfoCallback;
import org.apache.bookkeeper.proto.BookkeeperProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.NewWriterCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ProposeValuesCallback;
import org.apache.bookkeeper.proto.WriterId;
import org.apache.bookkeeper.proto.PaxosValue;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.IOUtils;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class BookieClientTest {
    private final static Logger LOG = LoggerFactory.getLogger(
            BookieClientTest.class);

    BookieServer bs;
    File tmpDir;
    public int port = 13645;

    public EventLoopGroup eventLoopGroup;
    public OrderedSafeExecutor executor;
    ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();

    @Before
    public void setUp() throws Exception {
        tmpDir = IOUtils.createTempDir("bookieClient", "test");
        // Since this test does not rely on the BookKeeper client needing to
        // know via ZooKeeper which Bookies are available, okay, so pass in null
        // for the zkServers input parameter when constructing the BookieServer.
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setZkServers(null).setBookiePort(port)
            .setJournalDirName(tmpDir.getPath())
            .setLedgerDirNames(new String[] { tmpDir.getPath() });
        bs = new BookieServer(conf);
        bs.start();
        eventLoopGroup = new NioEventLoopGroup();
        executor = OrderedSafeExecutor.newBuilder()
                .name("BKClientOrderedSafeExecutor")
                .numThreads(2)
                .build();
    }

    @After
    public void tearDown() throws Exception {
        bs.shutdown();
        recursiveDelete(tmpDir);
        eventLoopGroup.shutdownGracefully();
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
        int rc = -123456;
        ByteBuffer entry;
    }

    ReadEntryCallback recb = new ReadEntryCallback() {

        public void readEntryComplete(int rc, long ledgerId, long entryId, ByteBuf bb, Object ctx) {
            ResultStruct rs = (ResultStruct) ctx;
            synchronized (rs) {
                rs.rc = rc;
                if (BKException.Code.OK == rc && bb != null) {
                    bb.readerIndex(24);
                    rs.entry = bb.nioBuffer();
                }
                rs.notifyAll();
            }
        }

    };

    WriteCallback wrcb = new WriteCallback() {
        public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
            if (ctx != null) {
                synchronized (ctx) {
                    if (ctx instanceof ResultStruct) {
                        ResultStruct rs = (ResultStruct) ctx;
                        rs.rc = rc;
                    }
                    ctx.notifyAll();
                }
            }
        }
    };

    @Test
    public void testWriteGaps() throws Exception {
        final Object notifyObject = new Object();
        byte[] passwd = new byte[20];
        Arrays.fill(passwd, (byte) 'a');
        BookieSocketAddress addr = new BookieSocketAddress("127.0.0.1", port);
        ResultStruct arc = new ResultStruct();

        BookieClient bc = new BookieClient(new ClientConfiguration(), eventLoopGroup, executor);
        ByteBuf bb = createByteBuffer(1, 1, 1);
        bc.addEntry(addr, 1, passwd, 1, bb, wrcb, arc, BookieProtocol.FLAG_NONE);
        synchronized (arc) {
            arc.wait(1000);
            assertEquals(0, arc.rc);
            bc.readEntry(addr, 1, 1, recb, arc);
            arc.wait(1000);
            assertEquals(0, arc.rc);
            assertEquals(1, arc.entry.getInt());
        }
        bb = createByteBuffer(2, 1, 2);
        bc.addEntry(addr, 1, passwd, 2, bb, wrcb, null, BookieProtocol.FLAG_NONE);
        bb = createByteBuffer(3, 1, 3);
        bc.addEntry(addr, 1, passwd, 3, bb, wrcb, null, BookieProtocol.FLAG_NONE);
        bb = createByteBuffer(5, 1, 5);
        bc.addEntry(addr, 1, passwd, 5, bb, wrcb, null, BookieProtocol.FLAG_NONE);
        bb = createByteBuffer(7, 1, 7);
        bc.addEntry(addr, 1, passwd, 7, bb, wrcb, null, BookieProtocol.FLAG_NONE);
        synchronized (notifyObject) {
            bb = createByteBuffer(11, 1, 11);
            bc.addEntry(addr, 1, passwd, 11, bb, wrcb, notifyObject, BookieProtocol.FLAG_NONE);
            notifyObject.wait();
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 6, recb, arc);
            arc.wait(1000);
            assertEquals(BKException.Code.NoSuchEntryException, arc.rc);
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 7, recb, arc);
            arc.wait(1000);
            assertEquals(0, arc.rc);
            assertEquals(7, arc.entry.getInt());
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 1, recb, arc);
            arc.wait(1000);
            assertEquals(0, arc.rc);
            assertEquals(1, arc.entry.getInt());
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 2, recb, arc);
            arc.wait(1000);
            assertEquals(0, arc.rc);
            assertEquals(2, arc.entry.getInt());
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 3, recb, arc);
            arc.wait(1000);
            assertEquals(0, arc.rc);
            assertEquals(3, arc.entry.getInt());
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 4, recb, arc);
            arc.wait(1000);
            assertEquals(BKException.Code.NoSuchEntryException, arc.rc);
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 11, recb, arc);
            arc.wait(1000);
            assertEquals(0, arc.rc);
            assertEquals(11, arc.entry.getInt());
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 5, recb, arc);
            arc.wait(1000);
            assertEquals(0, arc.rc);
            assertEquals(5, arc.entry.getInt());
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 10, recb, arc);
            arc.wait(1000);
            assertEquals(BKException.Code.NoSuchEntryException, arc.rc);
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 12, recb, arc);
            arc.wait(1000);
            assertEquals(BKException.Code.NoSuchEntryException, arc.rc);
        }
        synchronized (arc) {
            bc.readEntry(addr, 1, 13, recb, arc);
            arc.wait(1000);
            assertEquals(BKException.Code.NoSuchEntryException, arc.rc);
        }
    }

    private ByteBuf createByteBuffer(int i, long lid, long eid) {
        ByteBuf bb = Unpooled.buffer(4 + 24);
        bb.writeLong(lid);
        bb.writeLong(eid);
        bb.writeLong(eid - 1);
        bb.writeInt(i);
        return bb;
    }

    @Test
    public void testNoLedger() throws Exception {
        ResultStruct arc = new ResultStruct();
        BookieSocketAddress addr = new BookieSocketAddress("127.0.0.1", port);
        BookieClient bc = new BookieClient(new ClientConfiguration(), eventLoopGroup, executor);
        synchronized (arc) {
            bc.readEntry(addr, 2, 13, recb, arc);
            arc.wait(1000);
            assertEquals(BKException.Code.NoSuchLedgerExistsException, arc.rc);
        }
    }

    @Test
    public void testGetBookieInfo() throws IOException, InterruptedException {
        BookieSocketAddress addr = new BookieSocketAddress("127.0.0.1", port);
        BookieClient bc = new BookieClient(new ClientConfiguration(), new NioEventLoopGroup(), executor);
        long flags = BookkeeperProtocol.GetBookieInfoRequest.Flags.FREE_DISK_SPACE_VALUE |
                BookkeeperProtocol.GetBookieInfoRequest.Flags.TOTAL_DISK_CAPACITY_VALUE;

        class CallbackObj {
            int rc;
            long requested;
            long freeDiskSpace, totalDiskCapacity;
            CountDownLatch latch = new CountDownLatch(1);
            CallbackObj(long requested) {
                this.requested = requested;
                this.rc = 0;
                this.freeDiskSpace = 0L;
                this.totalDiskCapacity = 0L;
            }
        }
        CallbackObj obj = new CallbackObj(flags);
        bc.getBookieInfo(addr, flags, new GetBookieInfoCallback() {
            @Override
            public void getBookieInfoComplete(int rc, BookieInfo bInfo, Object ctx) {
                CallbackObj obj = (CallbackObj)ctx;
                obj.rc=rc;
                if (rc == Code.OK) {
                    if ((obj.requested & BookkeeperProtocol.GetBookieInfoRequest.Flags.FREE_DISK_SPACE_VALUE) != 0) {
                        obj.freeDiskSpace = bInfo.getFreeDiskSpace();
                    }
                    if ((obj.requested & BookkeeperProtocol.GetBookieInfoRequest.Flags.TOTAL_DISK_CAPACITY_VALUE) != 0) {
                        obj.totalDiskCapacity = bInfo.getTotalDiskSpace();
                    }
                }
                obj.latch.countDown();
            }

        }, obj);
        obj.latch.await();
        System.out.println("Return code: " + obj.rc + "FreeDiskSpace: " + obj.freeDiskSpace + " TotalCapacity: " + obj.totalDiskCapacity);
        assertTrue("GetBookieInfo failed with error " + obj.rc, obj.rc == Code.OK);
        assertTrue("GetBookieInfo failed with error " + obj.rc, obj.freeDiskSpace <= obj.totalDiskCapacity);
        assertTrue("GetBookieInfo failed with error " + obj.rc, obj.totalDiskCapacity > 0);
    }


    class TestNewWriterCallback
        extends CountDownLatch
        implements NewWriterCallback {
        int rc;
        WriterId higherWriter;
        Map<String,PaxosValue> currentValues;

        TestNewWriterCallback() { super(1); }

        @Override
        public void newWriterComplete(int rc, WriterId higherWriter,
                                      Map<String,PaxosValue> currentValues,
                                      Object ctx) {
            LOG.debug("New writer response, rc {}, higherWriter {}, values {}",
                      rc, higherWriter, currentValues);
            this.rc = rc;
            this.higherWriter = higherWriter;
            this.currentValues = currentValues;

            countDown();
        }
    }

    class TestProposeValuesCallback
        extends CountDownLatch
        implements ProposeValuesCallback {
        int rc;
        WriterId higherWriter;

        TestProposeValuesCallback() { super(1); }

        @Override
        public void proposeValuesComplete(int rc, WriterId higherWriter,
                                          Object ctx) {
            this.rc = rc;
            this.higherWriter = higherWriter;
            countDown();
        }
    }

    @Test
    public void testNewWriter() throws Exception {
        long ledgerId = 100;
        byte[] masterKey = new byte[0];

        BookieSocketAddress addr = new BookieSocketAddress("127.0.0.1", port);
        BookieClient bc = new BookieClient(new ClientConfiguration(),
                                           new NioEventLoopGroup(), executor);


        WriterId w1 = new WriterId(0, new UUID(0, 1L));
        WriterId w2 = new WriterId(0, new UUID(0, 2L));
        WriterId w3 = new WriterId(0, new UUID(0, 3L));
        Set<String> keys = new HashSet<>();
        keys.add("foobar");

        // try to set writer 1, should succeed
        TestNewWriterCallback cb1 = new TestNewWriterCallback();
        bc.setNewWriter(addr, ledgerId, masterKey,
                        w1, keys, cb1, null);
        assertTrue("Request should complete", cb1.await(10, TimeUnit.SECONDS));
        assertEquals("Request should succeed", cb1.rc, Code.OK);

        // try to set writer 3, should succeed
        TestNewWriterCallback cb2 = new TestNewWriterCallback();
        bc.setNewWriter(addr, ledgerId, masterKey,
                        w3, keys, cb2, null);
        assertTrue("Request should complete", cb2.await(10, TimeUnit.SECONDS));
        assertEquals("Request should succeed", cb2.rc, Code.OK);

        // try to set writer 2, should fail, 3 is higher
        TestNewWriterCallback cb3 = new TestNewWriterCallback();
        bc.setNewWriter(addr, ledgerId, masterKey,
                        w2, keys, cb3, null);
        assertTrue("Request should complete", cb3.await(10, TimeUnit.SECONDS));
        assertEquals("Request should not succeed", cb3.rc,
                     Code.OldWriterException);
        assertEquals("Should be w3", w3, cb3.higherWriter);

        // bump the epoch of writer 2 and try again
        WriterId w2_2 = w2.surpass(cb3.higherWriter);
        TestNewWriterCallback cb4 = new TestNewWriterCallback();
        bc.setNewWriter(addr, ledgerId, masterKey,
                        w2_2, keys, cb4, null);
        assertTrue("Request should complete", cb4.await(10, TimeUnit.SECONDS));
        assertEquals("Request should succeed", cb4.rc, Code.OK);
    }

    @Test
    public void testProposeValues() throws Exception {
        long ledgerId = 100;
        byte[] masterKey = new byte[0];

        BookieSocketAddress addr = new BookieSocketAddress("127.0.0.1", port);
        BookieClient bc = new BookieClient(new ClientConfiguration(),
                                           new NioEventLoopGroup(), executor);
        WriterId w1 = new WriterId(0, new UUID(0, 1L));
        WriterId w2 = new WriterId(0, new UUID(0, 2L));
        WriterId w3 = new WriterId(0, new UUID(0, 3L));

        Map<String,ByteString> proposedValues = new HashMap<>();
        proposedValues.put("foobar", ByteString.copyFromUtf8("barfoo"));

        Map<String,ByteString> proposedValues2 = new HashMap<>();
        proposedValues2.put("foobar", ByteString.copyFromUtf8("bazfob"));

        // try to set writer 1, should succeed
        TestNewWriterCallback cb1 = new TestNewWriterCallback();
        bc.setNewWriter(addr, ledgerId, masterKey,
                        w1, proposedValues.keySet(), cb1, null);
        assertTrue("Request should complete", cb1.await(10, TimeUnit.SECONDS));
        assertEquals("Request should succeed", cb1.rc, Code.OK);

        // Update the values on the bookies
        TestProposeValuesCallback cb2 = new TestProposeValuesCallback();
        bc.proposeValues(addr, ledgerId, masterKey,
                         w1, proposedValues, cb2, null);
        assertTrue("Request should complete", cb2.await(10, TimeUnit.SECONDS));
        assertEquals("Request should succeed", cb2.rc, Code.OK);

        // Set new writer w2
        TestNewWriterCallback cb3 = new TestNewWriterCallback();
        bc.setNewWriter(addr, ledgerId, masterKey,
                        w2, proposedValues.keySet(), cb3, null);
        assertTrue("Request should complete", cb3.await(10, TimeUnit.SECONDS));
        assertEquals("Request should succeed", cb3.rc, Code.OK);
        assertEquals("Previous value is visible",
                     cb3.currentValues.get("foobar").getValue(),
                     proposedValues.get("foobar"));
        assertEquals("Previous value was written by writer 1",
                     cb3.currentValues.get("foobar").getWriter(),
                     w1);

        // set new writer w3
        TestNewWriterCallback cb4 = new TestNewWriterCallback();
        bc.setNewWriter(addr, ledgerId, masterKey,
                        w3, proposedValues.keySet(), cb4, null);
        assertTrue("Request should complete", cb4.await(10, TimeUnit.SECONDS));
        assertEquals("Request should succeed", cb4.rc, Code.OK);
        assertEquals("Previous value is visible",
                     cb4.currentValues.get("foobar").getValue(),
                     proposedValues.get("foobar"));
        assertEquals("Previous value was written by writer 1",
                     cb4.currentValues.get("foobar").getWriter(),
                     w1);

        // update values with w2 should fail
        TestProposeValuesCallback cb5 = new TestProposeValuesCallback();
        bc.proposeValues(addr, ledgerId, masterKey,
                         w1, proposedValues, cb5, null);
        assertTrue("Request should complete", cb5.await(10, TimeUnit.SECONDS));
        assertEquals("Request should not succeed",
                     cb5.rc, Code.OldWriterException);

        // surpass and try again
        WriterId w2_2 = w2.surpass(cb5.higherWriter);

        // fails without setting writer id again
        TestProposeValuesCallback cb6 = new TestProposeValuesCallback();
        bc.proposeValues(addr, ledgerId, masterKey,
                         w2_2, proposedValues2, cb6, null);
        assertTrue("Request should complete", cb6.await(10, TimeUnit.SECONDS));
        assertEquals("Request should not succeed",
                     cb6.rc, Code.OldWriterException);

        // set new writer, previously proposed values should be there
        TestNewWriterCallback cb7 = new TestNewWriterCallback();
        bc.setNewWriter(addr, ledgerId, masterKey,
                        w2_2, proposedValues2.keySet(), cb7, null);
        assertTrue("Request should complete", cb7.await(10, TimeUnit.SECONDS));
        assertEquals("Request should succeed", cb7.rc, Code.OK);
        assertEquals("Previous value is visible",
                     cb7.currentValues.get("foobar").getValue(),
                     proposedValues.get("foobar"));
        assertEquals("Previous value was written by writer 1",
                     cb7.currentValues.get("foobar").getWriter(),
                     w1);

        // update values
        TestProposeValuesCallback cb8 = new TestProposeValuesCallback();
        bc.proposeValues(addr, ledgerId, masterKey,
                         w2_2, proposedValues2, cb8, null);
        assertTrue("Request should complete", cb8.await(10, TimeUnit.SECONDS));
        assertEquals("Request should succeed", cb8.rc, Code.OK);

        // ensure new values were updated
        WriterId w3_2 = w3.surpass(w2);

        TestNewWriterCallback cb9 = new TestNewWriterCallback();
        bc.setNewWriter(addr, ledgerId, masterKey,
                        w3_2, proposedValues2.keySet(), cb9, null);
        assertTrue("Request should complete", cb9.await(10, TimeUnit.SECONDS));
        assertEquals("Request should succeed", cb9.rc, Code.OK);
        assertEquals("Previous value is visible",
                     cb9.currentValues.get("foobar").getValue(),
                     proposedValues2.get("foobar"));
        assertEquals("Previous value was written by writer 1",
                     w2_2,
                     cb9.currentValues.get("foobar").getWriter());
    }
}
