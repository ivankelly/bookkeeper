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
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.client.LedgerFuture;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.bookkeeper.proto.LedgerSetFormats.LedgerSetFormat;

import org.apache.bookkeeper.client.ledgerset.Events.*;
import org.apache.bookkeeper.client.ledgerset.Contexts.*;

import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Utils {
    static final Logger LOG = LoggerFactory.getLogger(Utils.class);
    final static String LEDGERSET_FIELD = "ledgerSetMetadata";

    static class MetadataCorruptException extends Exception {
        private static final long serialVersionUID = 1L;
        MetadataCorruptException(String s) {
            super(s);
        }
        MetadataCorruptException(Exception e) {
            super(e);
        }
    }

    static void writeMetadata(BaseContext ctx, LedgerSetFormat newMetadata,
                              Version v, final Fsm fsm) {
        byte[] value = newMetadata.toByteArray();
        final LedgerFuture<Version> f = ctx.getMetadataStorage().write(ctx.getLedgerSetName(),
                                                                       value, v);
        f.addListener(new Runnable() {
                @Override
                public void run() {
                    MetadataUpdatedEvent event = null;
                    try {
                        event = new MetadataUpdatedEvent(f.get());
                    } catch (ExecutionException ee) {
                        event = new MetadataUpdatedEvent(ee.getCause());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        event = new MetadataUpdatedEvent(ie);
                    }
                    fsm.sendEvent(event);
                }
            }, ctx.getCallbackExecutor());
    }

    static void readMetadata(BaseContext ctx, final Fsm fsm) {
        final LedgerFuture<Versioned<byte[]>> f = ctx.getMetadataStorage().read(
                ctx.getLedgerSetName());
        f.addListener(new Runnable() {
                @Override
                public void run() {
                    MetadataReadEvent event = null;
                    try {
                        event = new MetadataReadEvent(f.get());
                    } catch (ExecutionException ee) {
                        event = new MetadataReadEvent(ee.getCause());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        event = new MetadataReadEvent(ie);
                    }
                    fsm.sendEvent(event);
                }
            }, ctx.getCallbackExecutor());
    }

    static Versioned<LedgerSetFormat> parseMetadata(Versioned<byte[]> v)
            throws MetadataCorruptException {
        LedgerSetFormat.Builder builder = LedgerSetFormat.newBuilder();
        byte[] bytes = v.getValue();
        if (bytes == null) {
            LOG.error("Metadata empty");
            throw new MetadataCorruptException("Metadata empty");
        }
        LedgerSetFormat metadata;
        try {
            metadata = builder.mergeFrom(bytes).build();
        } catch (InvalidProtocolBufferException ipbe) {
            LOG.error("Cannot parse metadata", ipbe);
            throw new MetadataCorruptException(ipbe);
        }
        return new Versioned<LedgerSetFormat>(metadata, v.getVersion());
    }
}
