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

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.bookkeeper.statemachine.StateMachine.Event;
import org.apache.bookkeeper.statemachine.StateMachine.Fsm;
import org.apache.bookkeeper.statemachine.StateMachine.FsmImpl;
import org.apache.bookkeeper.statemachine.StateMachine.State;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerSet;
import org.apache.bookkeeper.client.EntryIds;
import org.apache.bookkeeper.client.LedgerSet.EntryId;
import org.apache.bookkeeper.client.LedgerSet.Entry;
import org.apache.bookkeeper.client.MetadataStorage;
import org.apache.bookkeeper.client.ledgerset.TrimmingStates.TrimmingState;
import org.apache.bookkeeper.client.ledgerset.CreateLedgerStates.AddLedgerToMetadataState;
import org.apache.bookkeeper.client.ledgerset.CreateLedgerStates.CreatingNewLedgerState;
import org.apache.bookkeeper.client.ledgerset.WriterStates;
import org.apache.bookkeeper.client.ledgerset.ReaderStates;
import org.apache.bookkeeper.client.ledgerset.Events;
import org.apache.bookkeeper.client.ledgerset.DirectLedgerSet;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.proto.LedgerSetFormats.LedgerSetFormat;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLedgerSet {
    private final static Logger LOG = LoggerFactory.getLogger(TestLedgerSet.class);
    final byte[] data = "10000".getBytes();
    MetadataStorage metadataStorage = null;
    TestCluster cluster = new TestCluster(3);
    BookKeeper bkc = null;
    ClientConfiguration baseClientConf = null;

    @Before
    public void setup() throws Exception {
        cluster.setUp();
        bkc = cluster.getClient();
        baseClientConf = cluster.getBaseClientConfiguration();

        ClientConfiguration conf = new ClientConfiguration();
        conf.setProperty(CompatZookeeperMetadataStorage.ZK_SERVERS,
                cluster.getZooKeeperUtil().getZooKeeperConnectString());
        metadataStorage = new CompatZookeeperMetadataStorage(conf, "ledger-set");
    }

    @After
    public void teardown() throws Exception {
        if (metadataStorage != null) {
            metadataStorage.close();
            metadataStorage = null;
        }
        cluster.tearDown();
    }

    static class FsmWaitFuture extends AbstractFuture<Void> {
        Class<?> entityToWaitFor;

        FsmWaitFuture(Class<?> entityToWaitFor) {
            this.entityToWaitFor = entityToWaitFor;
        }

        Class<?> getEntity() {
            return entityToWaitFor;
        }

        void complete() {
            set(null);
        }
    }

    static class HijackSpec {
        Class<?> state;
        Class<?> event;

        HijackSpec(Class<?> state,
                   Class<?> event) {
            this.state = state;
            this.event = event;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof HijackSpec) {
                HijackSpec other = (HijackSpec)o;
                return this.state == other.state
                    && this.event == other.event;
            }
            return false;
        }

        @Override
        public int hashCode() {
            return state.hashCode() * 13 + event.hashCode();
        }

        @Override
        public String toString() {
            return state.toString() + ":" + event.toString() + ":" + hashCode();
        }
    }

    static interface EventHijacker {
        Event hijackEvent(Event e);
    }

    class TracingFsmImpl extends FsmImpl {
        List<FsmWaitFuture> stateListeners;
        List<FsmWaitFuture> eventListeners;
        Map<HijackSpec,EventHijacker> hijackers;

        TracingFsmImpl() {
            this(new ArrayList<FsmWaitFuture>(), new ArrayList<FsmWaitFuture>(),
                 new HashMap<HijackSpec, EventHijacker>());
        }

        TracingFsmImpl(List<FsmWaitFuture> stateListeners,
                       List<FsmWaitFuture> eventListeners,
                       Map<HijackSpec,EventHijacker> hijackers) {
            super(Executors.newSingleThreadScheduledExecutor(
                          new ThreadFactoryBuilder().setNameFormat("fsm-%s").build()));
            this.stateListeners = stateListeners;
            this.eventListeners = eventListeners;
            this.hijackers = hijackers;
        }

        Future<Void> waitForState(Class<?> state) {
            synchronized (stateListeners) {
                FsmWaitFuture f = new FsmWaitFuture(state);
                stateListeners.add(f);
                return f;
            }
        }

        Future<Void> waitForEvent(Class<?> event) {
            synchronized (eventListeners) {
                FsmWaitFuture f = new FsmWaitFuture(event);
                eventListeners.add(f);
                return f;
            }
        }

        void hijackEvent(Class<?> state, Class<?> event, EventHijacker h) {
            synchronized (hijackers) {
                HijackSpec spec = new HijackSpec(state, event);
                hijackers.put(spec, h);
            }
        }

        void completeListeners(List<FsmWaitFuture> listeners, Class<?> entity) {
            synchronized (listeners) {
                List<FsmWaitFuture> completed = new ArrayList<FsmWaitFuture>();
                for (FsmWaitFuture f : listeners) {
                    if (f.getEntity().equals(entity)) {
                        completed.add(f);
                        f.complete();
                    }
                }
                listeners.removeAll(completed);
            }
        }

        @Override
        public Fsm newChildFsm() {
            return new TracingFsmImpl(stateListeners, eventListeners, hijackers);
        }

        @Override
        public void setInitState(State initState) {
            super.setInitState(initState);
            completeListeners(stateListeners, initState.getClass());
        }

        @Override
        public void setState(State curState, State newState) {
            super.setState(curState, newState);
            completeListeners(stateListeners, newState.getClass());
        }

        @Override
        protected boolean processEvent(Event e) {
            synchronized (hijackers) {
                if (getState() != null) {
                    HijackSpec spec = new HijackSpec(getState().getClass(), e.getClass());
                    EventHijacker h = hijackers.get(spec);
                    if (h != null) {
                        e = h.hijackEvent(e);
                        if (e == null) {
                            // if null returns, event is swallowed
                            return false;
                        }
                    }
                }
            }
            return super.processEvent(e);
        }

        @Override
        public void sendEvent(Event e) {
            super.sendEvent(e);
            completeListeners(eventListeners, e.getClass());
        }
    }

    @Test(timeout=10000)
    public void testEntryIdCompare() {
        LedgerSet.EntryId e1 = new DirectLedgerSet.EntryId(1, 0);
        LedgerSet.EntryId e2 = new DirectLedgerSet.EntryId(1, 1);
        LedgerSet.EntryId e3 = new DirectLedgerSet.EntryId(1, 3);
        LedgerSet.EntryId e4 = new DirectLedgerSet.EntryId(2, 1);

        List<LedgerSet.EntryId> entries = new ArrayList<LedgerSet.EntryId>();
        entries.add(e4);
        entries.add(e2);
        entries.add(EntryIds.NullEntryId);
        entries.add(e1);
        entries.add(e3);
        Collections.sort(entries);

        assertEquals("Entry should match", EntryIds.NullEntryId, entries.remove(0));
        assertEquals("Entry should match", e1, entries.remove(0));
        assertEquals("Entry should match", e2, entries.remove(0));
        assertEquals("Entry should match", e3, entries.remove(0));
        assertEquals("Entry should match", e4, entries.remove(0));

        assertFalse(EntryIds.NullEntryId.compareTo(e2) == e2.compareTo(EntryIds.NullEntryId));
    }

    @Test(timeout=60000)
    public void testCreateWriteAndRollAndRead() throws Exception {

        final String setName = "partition";
        final ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        final int numEntriesPerLedger = 3;
        setMaxLedgerSize(conf, data.length * numEntriesPerLedger);
        int entriesWritten = 0;

        TracingFsmImpl fsm = new TracingFsmImpl();
        Future<Void> stateWait = fsm.waitForState(WriterStates.WritingState.class);
        Future<Void> eventWait = fsm.waitForEvent(Events.NewLedgerAvailableEvent.class);
        LedgerSet.Writer w = new DirectLedgerSetBuilder().setFsm(fsm).setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildWriter(setName).get();
        w.writeEntry(data).get();
        entriesWritten++;
        stateWait.get(3, TimeUnit.SECONDS);
        eventWait.get(3, TimeUnit.SECONDS);

        eventWait = fsm.waitForEvent(Events.NewLedgerAvailableEvent.class);

        EntryId lastWrittenId = null;
        for (int i = 0; i < (numEntriesPerLedger*1.5); i++) {
            lastWrittenId = w.writeEntry(data).get();
            entriesWritten++;
        }
        eventWait.get(3, TimeUnit.SECONDS);

        LOG.info("Start reading");
        TracingFsmImpl fsm2 = new TracingFsmImpl();
        stateWait = fsm2.waitForState(ReaderStates.ReadingState.class);
        LedgerSet.Reader r = new DirectLedgerSetBuilder().setFsm(fsm2)
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildFencingReader(setName).get();
        r.hasMoreEntries().get();
        stateWait.get(3, TimeUnit.SECONDS);
        EntryId lastReadId = null;
        for (int i = 0; i < entriesWritten; i++) {
            assertTrue("Should have more entries", r.hasMoreEntries().get());
            Entry e = r.nextEntry().get();
            lastReadId = e.getId();
            assertTrue("Entry should match what we wrote", Arrays.equals(data, e.getBytes()));
        }
        assertFalse("Shouldn't have any more entries", r.hasMoreEntries().get());

        assertEquals("Should have read everything written", lastWrittenId, lastReadId);
        try {
            w.writeEntry(data).get();
            fail("Shouldn't be able to write to original set");
        } catch (ExecutionException bke) {
            // normal behaviour
        }

        LedgerSet.Writer w2 = LedgerSet.newBuilder().setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildWriter(setName).get();

        w2.resume(lastReadId).get();
        w2.writeEntry(data).get(); // should succeed
    }

    @Test(timeout=60000)
    public void testSkipping() throws Exception {
        final String setName = "partition";
        final ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        final int numEntriesPerLedger = 10;
        setMaxLedgerSize(conf, data.length * numEntriesPerLedger);

        TracingFsmImpl fsm = new TracingFsmImpl();
        LedgerSet.Writer w = new DirectLedgerSetBuilder().setFsm(fsm).setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildWriter(setName).get();

        Future<Void> eventWait = fsm.waitForEvent(Events.LedgerClosedEvent.class);
        LedgerSet.EntryId beforeBoundaryEntry = null;
        for (int i = 0; i < numEntriesPerLedger; i++) {
            beforeBoundaryEntry = w.writeEntry(data).get();
        }
        eventWait.get(1, TimeUnit.SECONDS);
        LOG.debug("beforeBoundary {}", beforeBoundaryEntry);
        LedgerSet.EntryId afterBoundaryEntry = w.writeEntry(data).get();
        LOG.debug("afterBoundary {}", afterBoundaryEntry);

        for (int i = 0; i < numEntriesPerLedger; i++) {
            LOG.debug("wrote {}", w.writeEntry(data).get());
        }

        TracingFsmImpl fsm2 = new TracingFsmImpl();
        LedgerSet.Reader r = new DirectLedgerSetBuilder().setFsm(fsm2).setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildFencingReader(setName).get();

        r.skip(beforeBoundaryEntry).get();
        LOG.debug("skipped past before boundary {}", beforeBoundaryEntry);

        for (int i = 0; i < numEntriesPerLedger + 1; i++) {
            assertTrue("Should have more entries", r.hasMoreEntries().get());
            Entry e = r.nextEntry().get();
            assertEquals("Read entry should have higher entry id than skip",
                         e.getId().compareTo(beforeBoundaryEntry), 1);
            LOG.debug("read {}", e.getId());
        }
        assertFalse("Shouldn't have more entries", r.hasMoreEntries().get());

        TracingFsmImpl fsm3 = new TracingFsmImpl();
        LedgerSet.Reader r2 = new DirectLedgerSetBuilder().setFsm(fsm3).setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildFencingReader(setName).get();

        r2.skip(afterBoundaryEntry).get();
        LOG.debug("skipped past after boundary {}", afterBoundaryEntry);
        for (int i = 0; i < numEntriesPerLedger; i++) {
            assertTrue("Should have more entries", r2.hasMoreEntries().get());
            Entry e = r2.nextEntry().get();
            assertEquals("Read entry should have higher entry id than skip",
                         e.getId().compareTo(afterBoundaryEntry), 1);

            LOG.debug("read {}", e.getId());
        }
        assertFalse("Shouldn't have more entries", r2.hasMoreEntries().get());

        TracingFsmImpl fsm4 = new TracingFsmImpl();
        LedgerSet.Reader r3 = new DirectLedgerSetBuilder().setFsm(fsm4).setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildFencingReader(setName).get();

        r3.skip(beforeBoundaryEntry).get();
        r3.skip(afterBoundaryEntry).get();
        for (int i = 0; i < numEntriesPerLedger; i++) {
            assertTrue("Should have more entries", r3.hasMoreEntries().get());
            Entry e = r3.nextEntry().get();
            assertEquals("Read entry should have higher entry id than skip",
                         e.getId().compareTo(afterBoundaryEntry), 1);
        }
        assertFalse("Shouldn't have more entries", r3.hasMoreEntries().get());

        TracingFsmImpl fsm5 = new TracingFsmImpl();
        LedgerSet.Reader r4 = new DirectLedgerSetBuilder().setFsm(fsm5).setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildFencingReader(setName).get();

        try {
            r4.skip(new DirectLedgerSet.EntryId(0xdeadbeefL, 0xcafebeefL)).get();
            fail("Shouldn't succeed");
        } catch (ExecutionException ee) {
            // success
        }
    }

    @Test(timeout=60000)
    public void testClusterDownWhileWriting() throws Exception {
        final String setName = "partition";
        final ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        final int numEntriesPerLedgers = 10;
        setMaxLedgerSize(conf, data.length * numEntriesPerLedgers);

        TracingFsmImpl fsm = new TracingFsmImpl();
        LedgerSet.Writer w = new DirectLedgerSetBuilder().setFsm(fsm)
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildWriter(setName).get();

        for (int i = 0; i < 10; i++) {
            w.writeEntry(data).get();
        }
        cluster.killBookie(cluster.getBookie(0));
        boolean erroring = false;
        List<Future<EntryId>> futures = new ArrayList<Future<EntryId>>();

        for (int i = 0; i < 100; i++) {
            futures.add(w.writeEntry(data));
        }
        int i = 0;
        for (Future<EntryId> f : futures) {
            try {
                f.get();
                LOG.info("{} completed", i);
                if (erroring) {
                    fail("Should never get past write, if one error occurred");
                }
            } catch (ExecutionException ee) {
                // this is fine
                erroring = true;
            }
        }

        assertTrue("Should have errorred at some point", erroring);
    }

    @Test(timeout=60000)
    public void testTrimmingWhileWriting() throws Exception {
        final String setName = "partition";
        final ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        final int numEntriesPerLedgers = 10;
        setMaxLedgerSize(conf, data.length * numEntriesPerLedgers);

        LedgerSet.Trimmer t = LedgerSet.newBuilder().setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildTrimmer(setName).get();

        TracingFsmImpl fsm = new TracingFsmImpl();
        LedgerSet.Writer w = new DirectLedgerSetBuilder().setFsm(fsm).setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildWriter(setName).get();

        Future<Void> eventWait = fsm.waitForEvent(Events.LedgerClosedEvent.class);
        LedgerSet.EntryId lastEntry = null;
        for (int i = 0; i < numEntriesPerLedgers; i++) {
            lastEntry = w.writeEntry(data).get();
        }
        eventWait.get(1, TimeUnit.SECONDS);

        eventWait = fsm.waitForEvent(Events.LedgerClosedEvent.class);
        Future<Void> f = t.trim(lastEntry);
        for (int i = 0; i < numEntriesPerLedgers; i++) {
            lastEntry = w.writeEntry(data).get();
        }
        f.get();
        eventWait.get(1, TimeUnit.SECONDS);

        lastEntry = w.writeEntry(data).get();

        t.trim(lastEntry).get();
        int numEntries = 0;

        TracingFsmImpl fsm2 = new TracingFsmImpl();
        LedgerSet.Reader r = new DirectLedgerSetBuilder().setFsm(fsm2).setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildFencingReader(setName).get();

        while (r.hasMoreEntries().get()) {
            r.nextEntry().get();
            numEntries++;
        }

        assertEquals("Only one entry should be untrimmed", 1, numEntries);
    }

    /**
     * Test that if an error occurs while trimming,
     * the writer can still continue
     */
    @Test(timeout=60000)
    public void testErrorWhileWritingTrimming() throws Exception {
        final String setName = "partition";
        final ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        final int numEntriesPerLedgers = 10;
        setMaxLedgerSize(conf, data.length * numEntriesPerLedgers);

        TracingFsmImpl trimmerfsm = new TracingFsmImpl();

        LedgerSet.Trimmer trimmer = new DirectLedgerSetBuilder().setFsm(trimmerfsm)
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildTrimmer(setName).get();

        LedgerSet.Writer writer = LedgerSet.newBuilder().setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildWriter(setName).get();

        for (int i = 0; i < numEntriesPerLedgers * 1.5; i++) {
            writer.writeEntry(data);
        }
        EntryId trimId = writer.writeEntry(data).get();
        EventHijacker h = new EventHijacker() {
                boolean hijacked = false;
                @Override
                public Event hijackEvent(Event e) {
                    if (e instanceof Events.MetadataUpdatedEvent
                        && !hijacked) {
                        hijacked = true;
                        return new Events.MetadataUpdatedEvent(
                                new MetadataStorage.FatalException());
                    } else {
                        return e;
                    }
                }
            };
        trimmerfsm.hijackEvent(TrimmingState.class, Events.MetadataUpdatedEvent.class, h);

        Future<Void> trimf = trimmer.trim(trimId);
        while (!trimf.isDone()) {
            writer.writeEntry(data);
            Thread.sleep(100);
        }
        try {
            trimf.get();
            fail("Trim should have failed");
        } catch (ExecutionException ee) {
            // correct behaviour
        }
        trimId = writer.writeEntry(data).get();

        // make sure another trim works fine
        trimf = trimmer.trim(trimId);
        while (!trimf.isDone()) {
            writer.writeEntry(data);
            Thread.sleep(100);
        }
        trimf.get();
        writer.writeEntry(data).get();
    }

    /**
     * Test that if a writer cannot create a new ledger, it will be able to continue
     * to write to the old one while the problem is (hopefully) resolved.
     */
    @Test(timeout=60000)
    public void testErrorWhileRollingCreatingLedger() throws Exception {
        final String setName = "partition";
        final ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        final int numEntriesPerLedgers = 10;
        setMaxLedgerSize(conf, data.length * numEntriesPerLedgers);

        TracingFsmImpl writerfsm = new TracingFsmImpl();

        LedgerSet.Writer writer = new DirectLedgerSetBuilder().setFsm(writerfsm)
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildWriter(setName).get();

        for (int i = 0; i < numEntriesPerLedgers * 1.5; i++) {
            writer.writeEntry(data);
        }
        writer.writeEntry(data).get();

        final AtomicBoolean hijacking = new AtomicBoolean(true);
        EventHijacker h = new EventHijacker() {
                @Override
                public Event hijackEvent(Event e) {
                    if (e instanceof Events.LedgerCreatedEvent
                        && hijacking.get()) {
                        return new Events.LedgerCreatedEvent(
                                BKException.Code.NotEnoughBookiesException, null);
                    } else {
                        return e;
                    }
                }
            };
        writerfsm.hijackEvent(CreatingNewLedgerState.class, Events.LedgerCreatedEvent.class, h);
        Future<Void> eventWait = writerfsm.waitForEvent(Events.NewLedgerErrorEvent.class);

        while (!eventWait.isDone()) {
            writer.writeEntry(data);
            Thread.sleep(100);
        }
        for (int i = 0; i < numEntriesPerLedgers * 1.5; i++) {
            writer.writeEntry(data);
        }
        writer.writeEntry(data).get();

        hijacking.set(false);

        eventWait = writerfsm.waitForEvent(Events.NewLedgerAvailableEvent.class);
        while (!eventWait.isDone()) {
            writer.writeEntry(data);
            Thread.sleep(100);
        }
        EntryId lastEntry = writer.writeEntry(data).get();
        LedgerSet.Reader reader = LedgerSet.newBuilder().setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage).buildFencingReader(setName).get();
        EntryId lastReadEntry = null;
        while (reader.hasMoreEntries().get()) {
            lastReadEntry = reader.nextEntry().get().getId();
        }
        assertEquals("Should be able to read all entries", lastReadEntry, lastEntry);
    }

    /**
     * Test that if a writer cannot update the metadata when a new ledger is created,
     * the writer will be able to continue writing to the old one.
     * In this case, the update does actually reach the metadata, but the response is
     * blocked. Ensure that we can continue writting, and a subsequent reader can read
     * all entries.
     */
    @Test(timeout=60000)
    public void testErrorWhileRollingUpdatingMetadata() throws Exception {
        final String setName = "partition";
        final ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        final int numEntriesPerLedgers = 10;
        setMaxLedgerSize(conf, data.length * numEntriesPerLedgers);

        TracingFsmImpl writerfsm = new TracingFsmImpl();

        LedgerSet.Writer writer = new DirectLedgerSetBuilder().setFsm(writerfsm)
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildWriter(setName).get();

        for (int i = 0; i < numEntriesPerLedgers * 1.5; i++) {
            writer.writeEntry(data);
        }
        writer.writeEntry(data).get();

        final AtomicBoolean hijacking = new AtomicBoolean(true);
        EventHijacker h = new EventHijacker() {
                @Override
                public Event hijackEvent(Event e) {
                    if (e instanceof Events.MetadataUpdatedEvent
                        && hijacking.get()) {
                        return new Events.MetadataUpdatedEvent(new MetadataStorage.FatalException());
                    } else {
                        return e;
                    }
                }
            };
        writerfsm.hijackEvent(AddLedgerToMetadataState.class, Events.MetadataUpdatedEvent.class, h);
        Future<Void> eventWait = writerfsm.waitForEvent(Events.NewLedgerErrorEvent.class);

        while (!eventWait.isDone()) {
            writer.writeEntry(data);
            Thread.sleep(100);
        }
        for (int i = 0; i < numEntriesPerLedgers * 1.5; i++) {
            writer.writeEntry(data);
        }
        EntryId lastEntry = writer.writeEntry(data).get();

        hijacking.set(false);

        // even though we're not highjacking, there'll be a metadata conflict
        // as there'll be ledgers added to the metadata, we haven't seen them since
        // we had "ServiceDown"
        eventWait = writerfsm.waitForEvent(Events.ErrorEvent.class);
        List<Future<EntryId>> futures = new ArrayList<Future<EntryId>>();
        while (!eventWait.isDone()) {
            futures.add(writer.writeEntry(data));
            Thread.sleep(100);
        }
        for (int i = 0; i < numEntriesPerLedgers * 1.5; i++) {
            futures.add(writer.writeEntry(data));
        }
        try {
            for (Future<EntryId> f : futures) {
                lastEntry = f.get();
            }
            fail("Some writes must have failed");
        } catch (ExecutionException ee) {
            // correct behaviour, when we see that the metadata
            // is not consistent with what we have, we should stop writing
        }

        LedgerSet.Reader reader = LedgerSet.newBuilder().setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage).buildFencingReader(setName).get();
        EntryId lastReadEntry = null;
        while (reader.hasMoreEntries().get()) {
            lastReadEntry = reader.nextEntry().get().getId();
        }
        assertEquals("Should be able to read all entries", lastReadEntry, lastEntry);
    }

    /**
     * Test that trimming and closing are fine while
     * between reading and writing (no more entries)
     */
    @Test(timeout=60000)
    public void testTrimmingAndClosingWhenNoMoreEntries() throws Exception {
        final String setName = "partition";
        final ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        final int numEntriesPerLedgers = 10;
        setMaxLedgerSize(conf, data.length * numEntriesPerLedgers);

        LedgerSet.Trimmer trimmer = LedgerSet.newBuilder().setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildTrimmer(setName).get();

        LedgerSet.Writer writer = LedgerSet.newBuilder().setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage).buildWriter(setName).get();

        for (int i = 0; i < numEntriesPerLedgers * 1.5; i++) {
            writer.writeEntry(data);
        }
        writer.writeEntry(data).get();

        LedgerSet.Reader reader = LedgerSet.newBuilder().setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage).buildFencingReader(setName).get();
        EntryId lastEntry = null;
        while (reader.hasMoreEntries().get()) {
            lastEntry = reader.nextEntry().get().getId();
        }
        trimmer.trim(lastEntry).get();
    }

    /**
     * Test that closing works during reading state
     */
    @Test(timeout=60000)
    public void testClosingWhenReading() throws Exception {
        final String setName = "partition";
        final ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        final int numEntriesPerLedgers = 10;
        setMaxLedgerSize(conf, data.length * numEntriesPerLedgers);

        LedgerSet.Writer writer = LedgerSet.newBuilder().setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage).buildWriter(setName).get();

        for (int i = 0; i < numEntriesPerLedgers * 1.5; i++) {
            writer.writeEntry(data);
        }
        writer.writeEntry(data).get();

        LedgerSet.Reader reader = LedgerSet.newBuilder().setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage).buildFencingReader(setName).get();
        for (int i = 0; i < numEntriesPerLedgers * 0.5; i++) {
            assert(reader.hasMoreEntries().get());
            reader.nextEntry().get();
        }
        reader.close().get();
    }

    /**
     * Test that wait more entries returns immediately if there are already
     * entries available.
     */
    @Test(timeout=60000)
    public void testWaitWhenReading() throws Exception {
        final String setName = "partition";
        final ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        final int numEntriesPerLedgers = 3;
        setMaxLedgerSize(conf, data.length * numEntriesPerLedgers);

        LedgerSet.Writer writer = LedgerSet.newBuilder().setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage).buildWriter(setName).get();

        for (int i = 0; i < numEntriesPerLedgers * 1.5; i++) {
            writer.writeEntry(data);
        }
        Future<EntryId> f = writer.writeEntry(data);
        EntryId lastEntry = f.get();
        writer.close().get();

        LedgerSet.TailingReader reader = LedgerSet.newBuilder().setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage).buildTailingReader(setName).get();
        for (int i = 0; i < numEntriesPerLedgers * 0.5; i++) {
            assert(reader.hasMoreEntries().get());
            reader.nextEntry().get();
        }

        reader.waitForMoreEntries().get();
        EntryId lastReadEntry = null;
        while (reader.hasMoreEntries().get()) {
            lastReadEntry = reader.nextEntry().get().getId();
        }
        assertEquals("Should have read all written entries", lastEntry, lastReadEntry);
    }

    @Test(timeout=60000)
    public void testTailing() throws Exception {
        final String setName = "partition";
        final ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        final int numEntriesPerLedgers = 10;
        setMaxLedgerSize(conf, data.length * numEntriesPerLedgers);

        TracingFsmImpl writerfsm = new TracingFsmImpl();
        LedgerSet.Writer writer = new DirectLedgerSetBuilder().setFsm(writerfsm)
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildWriter(setName).get();

        writer.writeEntry(data).get();

        TracingFsmImpl readerfsm = new TracingFsmImpl();
        LedgerSet.TailingReader reader = new DirectLedgerSetBuilder().setFsm(readerfsm)
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildTailingReader(setName).get();

        // lac shouldn't be set, wont be able to read anything
        assertFalse("Should be no entries", reader.hasMoreEntries().get());
        Future<Void> f = reader.waitForMoreEntries();
        try {
            f.get(2, TimeUnit.SECONDS);
            fail("Shouldn't complete");
        } catch (TimeoutException te) {
            // correct
        }

        writer.writeEntry(data).get();
        f.get(30, TimeUnit.SECONDS);
        assertTrue("Should complete to read first entry", reader.hasMoreEntries().get());
        reader.nextEntry().get().getId();
        assertFalse("Should be no more entries readable", reader.hasMoreEntries().get());

        Future<Void> eventWait = writerfsm.waitForEvent(Events.LedgerClosedEvent.class);

        Future<EntryId> lastEntry = null;
        for (int i = 0; i < 8; i++) {
            lastEntry = writer.writeEntry(data);
        }
        lastEntry.get();

        eventWait.get(1, TimeUnit.SECONDS);

        Future<Void> stateWait = readerfsm.waitForState(
                ReaderStates.WaitMoreEntriesState.class);
        // should be able to read to end of ledger
        for (int i = 0; i < 9; i++) {
            if (!reader.hasMoreEntries().get()) {
                reader.waitForMoreEntries().get();
                assertTrue (reader.hasMoreEntries().get());
            }
            reader.nextEntry().get();
        }
        assertFalse("Should be no more entries", reader.hasMoreEntries().get());
        f = reader.waitForMoreEntries();
        stateWait.get(1, TimeUnit.SECONDS);
        writer.writeEntry(data).get();
        try {
            f.get(2, TimeUnit.SECONDS);
            fail("Shouldn't complete");
        } catch (TimeoutException te) {
            // correct
        }
        writer.writeEntry(data).get();
        f.get(30, TimeUnit.SECONDS);
        assertTrue("Should see first entry in new ledger", reader.hasMoreEntries().get());
    }

    @Test(timeout=60000)
    public void testReaderOpenedBeforeWriter() throws Exception {
        final String setName = "partition";
        final ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        final int numEntriesPerLedgers = 10;
        setMaxLedgerSize(conf, data.length * numEntriesPerLedgers);

        TracingFsmImpl readerfsm = new TracingFsmImpl();
        LedgerSet.TailingReader reader = new DirectLedgerSetBuilder().setFsm(readerfsm)
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildTailingReader(setName).get();

        // lac shouldn't be set, wont be able to read anything
        assertFalse("Shouldn't have entries yet", reader.hasMoreEntries().get());
        Future<Void> f = reader.waitForMoreEntries();
        try {
            f.get(2, TimeUnit.SECONDS);
            fail("Shouldn't complete");
        } catch (TimeoutException te) {
            // correct
        }

        TracingFsmImpl writerfsm = new TracingFsmImpl();
        LedgerSet.Writer writer = new DirectLedgerSetBuilder().setFsm(writerfsm)
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildWriter(setName).get();

        for (int i = 0; i < 5; i++) {
            writer.writeEntry(data).get();
        }
        f.get(30, TimeUnit.SECONDS);
        assertTrue("Should see entries now", reader.hasMoreEntries().get());
    }

    @Test(timeout=60000)
    public void testTrimmingWhileRolling() throws Exception{
        final String setName = "partition";
        final ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        final int numEntriesPerLedgers = 10;
        setMaxLedgerSize(conf, data.length * numEntriesPerLedgers);

        TracingFsmImpl trimmerfsm = new TracingFsmImpl();
        LedgerSet.Trimmer trimmer = new DirectLedgerSetBuilder().setFsm(trimmerfsm)
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildTrimmer(setName).get();

        TracingFsmImpl writerfsm = new TracingFsmImpl();
        LedgerSet.Writer writer = new DirectLedgerSetBuilder().setFsm(writerfsm)
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildWriter(setName).get();

        TracingFsmImpl readerfsm = new TracingFsmImpl();
        LedgerSet.TailingReader reader = new DirectLedgerSetBuilder().setFsm(readerfsm)
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildTailingReader(setName).get();

        for (int i = 0; i < numEntriesPerLedgers*5; i++) {
            writer.writeEntry(data).get();
        }
        Entry laste = null;
        for (int i = 0; i < numEntriesPerLedgers*2; i++) {
            if (!reader.hasMoreEntries().get()) {
                reader.waitForMoreEntries().get();
            }
            assertTrue("Should be able to read", reader.hasMoreEntries().get());
            laste = reader.nextEntry().get();
        }
        Future<Void> eventWait = trimmerfsm.waitForEvent(Events.MetadataUpdatedEvent.class);
        trimmer.trim(laste.getId()).get();
        eventWait.get(1, TimeUnit.SECONDS);

        for (int i = 0; i < numEntriesPerLedgers*2; i++) {
            if (!reader.hasMoreEntries().get()) {
                reader.waitForMoreEntries().get();
            }
            assertTrue("Should be able to read", reader.hasMoreEntries().get());
            laste = reader.nextEntry().get();
        }

        eventWait = writerfsm.waitForEvent(Events.MetadataReadEvent.class);
        for (int i = 0; i < numEntriesPerLedgers*2.5; i++) {
            writer.writeEntry(data).get();
        }
        eventWait.get(1, TimeUnit.SECONDS);

        eventWait = trimmerfsm.waitForEvent(Events.MetadataReadEvent.class);
        trimmer.trim(laste.getId()).get();
        eventWait.get(1, TimeUnit.SECONDS);
    }

    @Test(timeout=60000)
    public void testTrimmingWhenCurrentLedgerAlreadyTrimmed() throws Exception {
        final String setName = "partition";
        final ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        final int numEntriesPerLedgers = 10;
        setMaxLedgerSize(conf, data.length * numEntriesPerLedgers);

        TracingFsmImpl trimmerfsm = new TracingFsmImpl();
        LedgerSet.Trimmer trimmer = new DirectLedgerSetBuilder().setFsm(trimmerfsm)
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildTrimmer(setName).get();

        TracingFsmImpl writerfsm = new TracingFsmImpl();
        LedgerSet.Writer writer = new DirectLedgerSetBuilder().setFsm(writerfsm)
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildWriter(setName).get();

        TracingFsmImpl readerfsm = new TracingFsmImpl();
        LedgerSet.TailingReader reader = new DirectLedgerSetBuilder().setFsm(readerfsm)
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildTailingReader(setName).get();

        Future<Void> eventWait = writerfsm.waitForEvent(Events.LedgerClosedEvent.class);
        for (int i = 0; i < numEntriesPerLedgers; i++) {
            writer.writeEntry(data).get();
        }
        eventWait.get(1, TimeUnit.SECONDS);

        eventWait = writerfsm.waitForEvent(Events.LedgerClosedEvent.class);
        for (int i = 0; i < numEntriesPerLedgers; i++) {
            writer.writeEntry(data).get();
        }
        eventWait.get(1, TimeUnit.SECONDS);

        EntryId toTrim = writer.writeEntry(data).get();

        for (int i = 0; i < numEntriesPerLedgers*0.5; i++) {
            if (!reader.hasMoreEntries().get()) {
                reader.waitForMoreEntries().get();
            }
            assertTrue("Should be entries", reader.hasMoreEntries().get());
            reader.nextEntry().get();
        }
        trimmer.trim(toTrim).get();
        EntryId readerToTrim = reader.nextEntry().get().getId();
        try {
            trimmer.trim(readerToTrim).get();
            fail("Trim from reader should fail");
        } catch (ExecutionException ee) {
            // correct
        }

        EntryId firstUntrimmed = writer.writeEntry(data).get();
        for (int i = 0; i < numEntriesPerLedgers; i++) {
            writer.writeEntry(data).get();
        }

        try {
            EntryId lastReadEntry = readerToTrim;
            while (firstUntrimmed.compareTo(lastReadEntry) > 0) {
                   if (!reader.hasMoreEntries().get()) {
                       reader.waitForMoreEntries().get();
                   }
                   assertTrue("Should be entries", reader.hasMoreEntries().get());
                   lastReadEntry = reader.nextEntry().get().getId();
            }
            fail("Shouldn't be able to get here, it should fail at the gap");
        } catch (ExecutionException ee) {
            // correct
        }
    }

    @Test(timeout=60000)
    public void testTrimmingNonExistent() throws Exception {
        final String setName = "partition";
        final ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        final int numEntriesPerLedgers = 10;
        setMaxLedgerSize(conf, data.length * numEntriesPerLedgers);

        TracingFsmImpl trimmerfsm = new TracingFsmImpl();
        LedgerSet.Trimmer trimmer = new DirectLedgerSetBuilder().setFsm(trimmerfsm)
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildTrimmer(setName).get();

        TracingFsmImpl writerfsm = new TracingFsmImpl();
        LedgerSet.Writer writer = new DirectLedgerSetBuilder().setFsm(writerfsm)
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildWriter(setName).get();

        for (int i = 0; i < numEntriesPerLedgers*2; i++) {
            writer.writeEntry(data).get();
        }

        EntryId readerToTrim = new DirectLedgerSet.EntryId(0xdeadbeef, 0x52342);
        try {
            trimmer.trim(readerToTrim).get();
            fail("Trim from reader should fail");
        } catch (ExecutionException ee) {
            // correct
        }
    }

    @Test(timeout=60000)
    public void testCloseWhileWriting() throws Exception {
        final String setName = "partition";
        final ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        final int numEntriesPerLedgers = 1000;
        setMaxLedgerSize(conf, data.length * numEntriesPerLedgers);

        LedgerSet.Writer w = LedgerSet.newBuilder().setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildWriter(setName).get();

        for (int i = 0; i < 10; i++) {
            w.writeEntry(data);
        }
        w.close().get();

        LedgerSet.Reader r = LedgerSet.newBuilder().setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildFencingReader(setName).get();
        EntryId lastReadEntry = null;
        while (r.hasMoreEntries().get()) {
            lastReadEntry = r.nextEntry().get().getId();
        }

        LedgerSet.Writer w2 = LedgerSet.newBuilder().setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildWriter(setName).get();
        w2.resume(lastReadEntry).get();
        w2.writeEntry(data).get(); // make sure it writes metadata
        for (int i = 0; i < 10; i++) {
            w2.writeEntry(data);
        }

        LedgerSet.Reader r2 = LedgerSet.newBuilder().setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildFencingReader(setName).get();
        while (r2.hasMoreEntries().get()) {
            lastReadEntry = r2.nextEntry().get().getId();
        }

        LedgerSet.Writer w3 = LedgerSet.newBuilder().setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildWriter(setName).get();
        w3.resume(lastReadEntry).get();
        w3.writeEntry(data).get();

        Future<EntryId> writef = null;
        for (int i = 0; i < 100; i++) {
            writef = w2.writeEntry(data);
        }
        Future<Void> closef = w2.close();

        try {
            writef.get();
            fail("Should fail");
        } catch (ExecutionException ee) {
            // correct
            LOG.info("Correctly caught exception", ee);
        }

        closef.get();
    }

    /**
     * Write until there are two ledgers in the set. Then close the writer.
     * Start tailing, and read until end. Tailer will go into waiting for new
     * ledger state. Trim to update metadata, tailer should handle this gracefully.
     * Open a new writer on the set, and start writing. The tailer should, pick up
     * these new updates.
     */
    @Test(timeout=60000)
    public void testTrimWhileWaitingForLedger() throws Exception {
        final String setName = "partition";
        final ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        final int numEntriesPerLedgers = 10;
        setMaxLedgerSize(conf, data.length * numEntriesPerLedgers);

        TracingFsmImpl trimmerfsm = new TracingFsmImpl();
        LedgerSet.Trimmer trimmer = new DirectLedgerSetBuilder().setFsm(trimmerfsm)
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildTrimmer(setName).get();

        TracingFsmImpl writerfsm = new TracingFsmImpl();
        LedgerSet.Writer writer = new DirectLedgerSetBuilder().setFsm(writerfsm)
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildWriter(setName).get();
        writeUntilRoll(writer, writerfsm, conf);
        LedgerSet.EntryId trimUpTo = writer.writeEntry(data).get();
        writer.close().get();

        LedgerSet.TailingReader reader = LedgerSet.newBuilder().setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage).buildTailingReader(setName).get();
        readToEnd(reader);

        Future<Void> waitFuture = reader.waitForMoreEntries();

        trimmer.trim(trimUpTo).get();

        try {
            waitFuture.get(3, TimeUnit.SECONDS);
            fail("Shouldn't complete");
        } catch (TimeoutException te) {
            // expected
        }

        assertFalse("Shouldn't have completed", waitFuture.isDone());
        LedgerSet.Writer writer2 = LedgerSet.newBuilder().setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildWriter(setName).get();
        writer2.resume(trimUpTo).get();
        // write twice so LAC is updated
        writer2.writeEntry(data).get();
        writer2.writeEntry(data).get();

        waitFuture.get(100, TimeUnit.SECONDS);
    }

    private void addEmptyLedgerToLedgerSet(String setName, EntryId resumeFrom) throws Exception {
        final ClientConfiguration conf = new ClientConfiguration(baseClientConf);
        TracingFsmImpl writerfsm = new TracingFsmImpl();
        LedgerSet.Writer writer = new DirectLedgerSetBuilder()
            .setFsm(writerfsm)
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildWriter(setName).get();
        final CountDownLatch hijacked = new CountDownLatch(1);
        EventHijacker h = new EventHijacker() {
                @Override
                public Event hijackEvent(Event e) {
                    hijacked.countDown();
                    return null;
                }
            };
        writerfsm.hijackEvent(WriterStates.WritingState.class, Events.WriteEntryEvent.class, h);
        if (resumeFrom != null) {
            writer.resume(resumeFrom).get();
        }
        writer.writeEntry(data);
        hijacked.await();
    }

    /**
     * Test that if a ledger set is created, but nothing
     * ever succeeds in writing, we can open it for writing
     * again.
     */
    @Test(timeout=60000)
    public void testResumeEmptyLedgerSet() throws Exception {
        final String setName = "partition";
        final ClientConfiguration conf = new ClientConfiguration(baseClientConf);

        try {
            metadataStorage.read(setName).get();
            fail("Shouldn't be able to read any metdata");
        } catch (ExecutionException ee) {
            // correct
        }

        for (int i = 1; i <= 3; i++) {
            addEmptyLedgerToLedgerSet(setName, null);

            Versioned<byte[]> bytes = metadataStorage.read(setName).get();
            Versioned<LedgerSetFormat> metadata = Utils.parseMetadata(bytes);
            assertEquals("Should have " + i + " ledger in metadata",
                         i, metadata.getValue().getLedgerList().size());
        }

        LedgerSet.Writer writer = new DirectLedgerSetBuilder()
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildWriter(setName).get();
        EntryId resumeFrom = writer.writeEntry(data).get();

        for (int i = 5; i <= 8; i++) {
            addEmptyLedgerToLedgerSet(setName, resumeFrom);

            Versioned<byte[]> bytes = metadataStorage.read(setName).get();
            Versioned<LedgerSetFormat> metadata = Utils.parseMetadata(bytes);
            assertEquals("Should have " + i + " ledger in metadata",
                         i, metadata.getValue().getLedgerList().size());

        }

        writer = new DirectLedgerSetBuilder()
            .setConfiguration(conf)
            .setClient(bkc).setMetadataStorage(metadataStorage)
            .buildWriter(setName).get();
        writer.resume(resumeFrom).get();
        writer.writeEntry(data).get();

        Versioned<byte[]> bytes = metadataStorage.read(setName).get();
        Versioned<LedgerSetFormat> metadata = Utils.parseMetadata(bytes);
        assertEquals("Should have " + 9 + " ledger in metadata",
                9, metadata.getValue().getLedgerList().size());
    }

    private void writeUntilRoll(LedgerSet.Writer writer,
                                TracingFsmImpl fsm, ClientConfiguration conf)
            throws ExecutionException, InterruptedException, TimeoutException {
        Future<Void> eventWait = fsm.waitForEvent(Events.NewLedgerAvailableEvent.class);
        for (int i = 0; i < (getMaxLedgerSize(conf)/data.length) * 10; i++) {
            writer.writeEntry(data).get();
            if (eventWait.isDone()) {
                return;
            }
        }
        eventWait.get(10, TimeUnit.SECONDS);
        // if this hasn't completed by now there's a serious issue, check the logs
    }

    static int getMaxLedgerSize(ClientConfiguration conf) {
        return conf.getInt(Constants.LEDGERSET_MAX_LEDGER_SIZE_CONFKEY);
    }

    static void setMaxLedgerSize(ClientConfiguration conf, int size) {
        conf.setProperty(Constants.LEDGERSET_MAX_LEDGER_SIZE_CONFKEY, size);
    }

    private void readToEnd(LedgerSet.Reader reader)
            throws ExecutionException, InterruptedException {
        while (reader.hasMoreEntries().get()) {
            reader.nextEntry().get();
        }
    }
}
