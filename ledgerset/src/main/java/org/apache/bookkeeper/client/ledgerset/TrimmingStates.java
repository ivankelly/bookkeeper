
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
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.proto.LedgerSetFormats.LedgerSetFormat;

import java.util.List;
import java.util.ArrayList;

import org.apache.bookkeeper.client.MetadataStorage;
import org.apache.bookkeeper.client.ledgerset.BaseStates.BaseState;
import org.apache.bookkeeper.client.ledgerset.BaseStates.TrimmerBaseState;
import org.apache.bookkeeper.client.ledgerset.BaseStates.DeferringState;
import org.apache.bookkeeper.client.ledgerset.BaseStates.ErrorState;
import org.apache.bookkeeper.client.ledgerset.Events.*;
import org.apache.bookkeeper.client.ledgerset.Utils.MetadataCorruptException;
import org.apache.bookkeeper.client.ledgerset.Contexts.*;
import static org.apache.bookkeeper.client.ledgerset.Utils.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TrimmingStates {
    static final Logger LOG = LoggerFactory.getLogger(TrimmingStates.class);

    static class TrimInitState extends TrimmerBaseState<BaseContext> {
        TrimInitState(Fsm fsm, BaseContext ctx) {
            super(fsm, ctx);
        }

        @Override
        public State handleEvent(TrimEvent e) {
            fsm.deferEvent(e);

            return new ReadMetadataState(fsm, ctx);
        }

        @Override
        public State handleEvent(CloseEvent e) {
            fsm.deferEvent(e);
            return new ErrorState(fsm, new IllegalStateException("Closed"));
        }
    }

    static class ReadMetadataState extends DeferringState<BaseContext> {
        ReadMetadataState(Fsm fsm, BaseContext ctx) {
            super(fsm, ctx);
            readMetadata(ctx, fsm);
        }

        public State handleEvent(MetadataReadEvent ose) {
            try {
                Versioned<LedgerSetFormat> metadata = parseMetadata(ose.getReadValue());

                LedgerSetContext.Builder builder = LedgerSetContext.newBuilder(ctx);
                builder.setMetadata(metadata);

                return new ReadyToTrimState(fsm, builder.build());
            } catch (MetadataCorruptException mce) {
                return new ErrorOneRequestState(fsm, ctx, mce);
            } catch (Throwable t) {
                LOG.error("Couldn't read metadata.");
                return new ErrorOneRequestState(fsm, ctx, t);
            }
        }
    }

    static class ReadyToTrimState extends DeferringState<LedgerSetContext> {
        ReadyToTrimState(Fsm fsm, LedgerSetContext ctx) {
            super(fsm, ctx);
        }

        public State handleEvent(TrimEvent e) {
            fsm.deferEvent(e);
            List<Long> ledgers = ctx.getMetadata().getValue().getLedgerList();

            if (!ledgers.contains(e.getLedgerId())) {
                Exception ex = new IllegalStateException(
                        "Current ledger isn't last in list of ledger."
                        + " Cannot continue writing");
                return new ErrorOneRequestState(fsm, ctx, ex);
            }

            return new TrimmingState(fsm, ctx, e.getLedgerId());
        }
    }

    static class TrimmingState extends DeferringState<LedgerSetContext>
        implements AsyncCallback.DeleteCallback {
        LedgerSetFormat newMetadata = null;
        final long trimmingToLedger;
        final List<Long> toDelete;

        TrimmingState(Fsm fsm, LedgerSetContext ctx, long trimmingToLedger)
            throws IllegalStateException {
            super(fsm, ctx);

            this.toDelete = new ArrayList<Long>();
            this.trimmingToLedger = trimmingToLedger;
            this.newMetadata = trimUpToLedger(ctx.getMetadata().getValue(),
                                              trimmingToLedger, toDelete);
            writeMetadata(ctx, this.newMetadata, ctx.getMetadata().getVersion(), fsm);
        }

        @Override
        public void deleteComplete(int rc, Object ctx) {
            if (rc != BKException.Code.OK) {
                assert (ctx instanceof Long);
                LOG.error("Error deleting ledger {}, return code {}", rc, (Long)ctx);
            }
        }

        public State handleEvent(MetadataUpdatedEvent ue) {
            try {
                Versioned<LedgerSetFormat> newMetadata
                    = new Versioned<LedgerSetFormat>(this.newMetadata, ue.getNewVersion());
                for (long l : toDelete) {
                    ctx.getBookkeeperClient().asyncDeleteLedger(l, this, (Object)l);
                }

                return new TrimSuccessState(fsm, ctx, newMetadata);
            } catch (MetadataStorage.BadVersionException e) {
                return new ReadMetadataState(fsm, ctx);
            } catch (Throwable t) {
                LOG.warn("Error trimming ledgers from metadata", t);
                return new ErrorOneRequestState(fsm, ctx, t);
            }
        }
    }

    static class TrimSuccessState extends TrimmerBaseState<LedgerSetContext> {
        final Versioned<LedgerSetFormat> metadata;

        TrimSuccessState(Fsm fsm, LedgerSetContext ctx,
                          Versioned<LedgerSetFormat> metadata) {
            super(fsm, ctx);
            this.metadata = metadata;
        }

        @Override
        public State handleEvent(TrimEvent e) {
            e.success(null);

            return new TrimInitState(fsm, ctx);
        }

        @Override
        public State handleEvent(CloseEvent e) {
            fsm.deferEvent(e);
            return new ErrorState(fsm, new IllegalStateException("Closed"));
        }
    }

    static class ErrorOneRequestState extends TrimmerBaseState<BaseContext> {
        final Throwable error;
        ErrorOneRequestState(Fsm fsm, BaseContext ctx, Throwable t) {
            super(fsm, ctx);
            this.error = t;
        }

        @Override
        public State handleEvent(TrimEvent e) {
            e.error(error);
            return new TrimInitState(fsm, ctx);
        }

        @Override
        public State handleEvent(CloseEvent e) {
            fsm.deferEvent(e);
            return new ErrorState(fsm, new IllegalStateException("Closed"));
        }
    }

    static LedgerSetFormat trimUpToLedger(LedgerSetFormat oldMetadata,
                                          long ledgerId, List<Long> toDelete)
            throws IllegalStateException {
        LedgerSetFormat.Builder builder = LedgerSetFormat.newBuilder()
            .mergeFrom(oldMetadata);

        builder.clearLedger();
        List<Long> ledgers = new ArrayList<Long>(oldMetadata.getLedgerList());
        if (!ledgers.contains(ledgerId)) {
            throw new IllegalStateException("Ledger doesn't exist in set");
        } else {
            while (ledgers.get(0) != ledgerId) {
                toDelete.add(ledgers.remove(0));
            }
            builder.addAllLedger(ledgers);
            LOG.debug("Removed up to {} from {}. {} removed",
                      new Object[] { ledgerId, oldMetadata.getLedgerList(), toDelete });
            return builder.build();
        }
    }
}
