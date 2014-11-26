/*
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

import org.apache.bookkeeper.statemachine.StateMachine.Fsm;
import org.apache.bookkeeper.statemachine.StateMachine.State;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.LedgerSetFormats.LedgerSetFormat;

import java.util.List;

import org.apache.bookkeeper.client.MetadataStorage;
import org.apache.bookkeeper.client.ledgerset.BaseStates.DeferringState;
import org.apache.bookkeeper.client.ledgerset.BaseStates.ErrorState;
import org.apache.bookkeeper.client.ledgerset.BaseStates.BaseState;
import org.apache.bookkeeper.client.ledgerset.Events.*;
import org.apache.bookkeeper.client.ledgerset.Utils.MetadataCorruptException;
import org.apache.bookkeeper.client.ledgerset.Contexts.*;

import static org.apache.bookkeeper.client.ledgerset.Utils.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

class CreateLedgerStates {
    static final Logger LOG = LoggerFactory.getLogger(CreateLedgerStates.class);

    static class CreatingNewLedgerState extends BaseState<LedgerSetContext>
        implements AsyncCallback.CreateCallback  {

        CreatingNewLedgerState(Fsm fsm, LedgerSetContext ctx) {
            super(fsm, ctx);
        }

        public State handleEvent(CreateLedgerEvent e) {
            fsm.deferEvent(e);

            BookKeeper.DigestType type
                = (BookKeeper.DigestType)ctx.getConf().getProperty(Constants.LEDGERSET_DIGEST_TYPE);
            String passwd = ctx.getConf().getString(Constants.LEDGERSET_PASSWD);
            int ensemble = ctx.getConf().getInt(Constants.LEDGERSET_ENSEMBLE_SIZE);
            int ack = ctx.getConf().getInt(Constants.LEDGERSET_ACK_QUORUM);
            int write = ctx.getConf().getInt(Constants.LEDGERSET_WRITE_QUORUM);
            ctx.getBookkeeperClient().asyncCreateLedger(
                    ensemble, write, ack, type, passwd.getBytes(Charsets.UTF_8), this, null);
            return this;
        }

        @Override
        public void createComplete(int rc, LedgerHandle lh, Object ctx2) {
            fsm.sendEvent(new LedgerCreatedEvent(rc, lh));
        }

        public State handleEvent(LedgerCreatedEvent ce) {
            if (ce.getReturnCode() == BKException.Code.OK) {
                return new AddLedgerToMetadataState(fsm, ctx, ce.getValue());
            } else {
                Throwable t = BKException.create(ce.getReturnCode());
                return new NonFatalErrorState(fsm, ctx, t);
            }
        }
    }

    static class AddLedgerToMetadataState extends DeferringState<LedgerSetContext> {
        final LedgerSetFormat newMetadata;
        final LedgerHandle newLedger;
        final long previousLastLedgerId;

        AddLedgerToMetadataState(Fsm fsm, LedgerSetContext ctx,
                                 LedgerHandle newLedger) {
            super(fsm, ctx);

            this.newLedger = newLedger;
            this.newMetadata = LedgerSetFormat.newBuilder()
                .mergeFrom(ctx.getMetadata().getValue())
                .addLedger(newLedger.getId()).build();

            List<Long> prevLedgers = ctx.getMetadata().getValue().getLedgerList();
            if (prevLedgers.size() > 0) {
                previousLastLedgerId = prevLedgers.get(prevLedgers.size()-1);
            } else {
                previousLastLedgerId = -1; // will be unused
            }
            writeMetadata(ctx, newMetadata, ctx.getMetadata().getVersion(), fsm);
        }

        public State handleEvent(MetadataUpdatedEvent ue) {
            try {
                Version version = ue.getNewVersion();
                Versioned<LedgerSetFormat> newVersionedMetadata
                    = new Versioned<LedgerSetFormat>(newMetadata, version);
                return new LedgerCreationSuccessState(fsm, ctx, newLedger, newVersionedMetadata);
            } catch (MetadataStorage.BadVersionException e) {
                readMetadata(ctx, fsm);
                return this;
            } catch (Throwable t) {
                return new NonFatalErrorState(fsm, ctx, t);
            }
        }

        public State handleEvent(MetadataReadEvent ose) {
            try {
                Versioned<LedgerSetFormat> metadata = parseMetadata(ose.getReadValue());
                List<Long> ledgers = metadata.getValue().getLedgerList();
                int size = ledgers.size();
                if (size == 0
                    || ledgers.get(size-1) != previousLastLedgerId) {
                    Exception ex = new IllegalStateException(
                            "Current ledger isn't last in list of ledger."
                            + " Cannot continue writing");
                    return new ErrorState(fsm, ex);
                }
                LedgerSetContext.Builder builder = LedgerSetContext.newBuilder(ctx);
                builder.setMetadata(metadata);
                return new AddLedgerToMetadataState(fsm, builder.build(), newLedger);
            } catch (MetadataCorruptException mce) {
                return new ErrorState(fsm, mce);
            } catch (Throwable t) {
                LOG.error("Couldn't reread metadata. Cannot continue writing");
                return new NonFatalErrorState(fsm, ctx, t);
            }
        }
    }

    static class LedgerCreationSuccessState extends BaseState<LedgerSetContext> {
        final LedgerHandle lh;
        final Versioned<LedgerSetFormat> metadata;

        LedgerCreationSuccessState(Fsm fsm, LedgerSetContext ctx,
                                   LedgerHandle lh, Versioned<LedgerSetFormat> metadata) {
            super(fsm, ctx);
            this.lh = lh;
            this.metadata = metadata;
        }

        public State handleEvent(CreateLedgerEvent e) {
            e.newLedgerAvailable(lh, metadata);
            return new ErrorState(fsm, new IllegalStateException("Creation fsm not reusable"));
        }
    }

    static class NonFatalErrorState extends BaseState<LedgerSetContext> {
        final Throwable t;

        NonFatalErrorState(Fsm fsm, LedgerSetContext ctx, Throwable t) {
            super(fsm, ctx);
            this.t = t;
        }

        public State handleEvent(CreateLedgerEvent e) {
            e.nonfatalError(t);

            return new ErrorState(fsm, new IllegalStateException("Creation fsm not reusable"));
        }
    }

}
