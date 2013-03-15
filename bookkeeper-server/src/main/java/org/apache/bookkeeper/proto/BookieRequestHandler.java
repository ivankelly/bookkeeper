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
package org.apache.bookkeeper.proto;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serverside handler for bookkeeper requests
 */
class BookieRequestHandler extends SimpleChannelHandler
    implements BookkeeperInternalCallbacks.WriteCallback {

    static Logger LOG = LoggerFactory.getLogger(BookieRequestHandler.class);
    private final Bookie bookie;
    private final ChannelGroup allChannels;

    private final BKStats bkStats = BKStats.getInstance();
    private final boolean statsEnabled;

    BookieRequestHandler(ServerConfiguration conf, Bookie bookie, ChannelGroup allChannels) {
        this.bookie = bookie;
        this.allChannels = allChannels;
        this.statsEnabled = conf.isStatisticsEnabled();
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx,
                            ChannelStateEvent e)
            throws Exception {
        allChannels.add(ctx.getChannel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        Throwable throwable = e.getCause();
        LOG.error("Unhandled exception occurred in I/O thread or handler", throwable);
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        LOG.debug("Channel connected {}", e);
    }

    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        LOG.debug("Channel disconnected {}", e);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        if (!(e.getMessage() instanceof BookieProtocol.Request)) {
            ctx.sendUpstream(e);
            return;
        }
        BookieProtocol.Request req = (BookieProtocol.Request)e.getMessage();

        Channel c = ctx.getChannel();

        if (req.getProtocolVersion() < BookieProtocol.LOWEST_COMPAT_PROTOCOL_VERSION) {
            LOG.error("Invalid protocol version, Expected something greater than "
                      + BookieProtocol.LOWEST_COMPAT_PROTOCOL_VERSION
                      + ". Got " + req.getProtocolVersion());
            c.write(ResponseBuilder.buildErrorResponse(BookieProtocol.LOWEST_COMPAT_PROTOCOL_VERSION,
                                                       BookieProtocol.EBADVERSION, req));
            return;
        }

        if (req.getHeader().hasHandshakeRequest()) {
            DataFormats.HandshakeResponse handshakeResponse = DataFormats.HandshakeResponse.newBuilder()
                .setLowestCompatProtocolVersion(BookieProtocol.LOWEST_COMPAT_PROTOCOL_VERSION)
                .setCurrentProtocolVersion(BookieProtocol.CURRENT_PROTOCOL_VERSION).build();
            DataFormats.ResponseHeader h = DataFormats.ResponseHeader.newBuilder()
                .setHandshakeResponse(handshakeResponse).build();
            c.write(new BookieProtocol.Response(req.getProtocolVersion(), h));
        } else if (req.getHeader().hasAddRequest()) {
            handleAdd(req, c);
        } else if (req.getHeader().hasReadRequest()) {
            handleRead(req, c);
        } else {
            LOG.error("Unknown op type {}, sending error", req.getHeader());
            c.write(ResponseBuilder.buildErrorResponse(BookieProtocol.EBADREQ, req));
            if (statsEnabled) {
                bkStats.getOpStats(BKStats.STATS_UNKNOWN).incrementFailedOps();
            }
        }
    }

    class AddCtx {
        final Channel c;
        final BookieProtocol.Request r;
        final long startTime;

        AddCtx(Channel c, BookieProtocol.Request r) {
            this.c = c;
            this.r = r;

            if (statsEnabled) {
                startTime = MathUtils.now();
            } else {
                startTime = 0;
            }
        }
    }

    private void handleAdd(BookieProtocol.Request req, Channel c) {
        assert (req.getHeader().hasAddRequest());
        DataFormats.AddRequest add = req.getHeader().getAddRequest();

        if (bookie.isReadOnly()) {
            LOG.warn("BookieServer is running as readonly mode,"
                     + " so rejecting the request from the client!");
            c.write(ResponseBuilder.buildErrorResponse(BookieProtocol.EREADONLY, req));
            if (statsEnabled) {
                bkStats.getOpStats(BKStats.STATS_ADD).incrementFailedOps();
            }
            return;
        }

        int rc = BookieProtocol.EOK;
        try {
            if (add.getIsRecoveryAdd()) {
                bookie.recoveryAddEntry(req.getDataAsByteBuffer(), this, new AddCtx(c, req),
                                        add.getMasterKey().toByteArray());
            } else {
                bookie.addEntry(req.getDataAsByteBuffer(),
                                this, new AddCtx(c, req), add.getMasterKey().toByteArray());
            }
        } catch (IOException e) {
            LOG.error("Error writing " + add, e);
            rc = BookieProtocol.EIO;
        } catch (BookieException.LedgerFencedException lfe) {
            LOG.error("Attempt to write to fenced ledger", lfe);
            rc = BookieProtocol.EFENCED;
        } catch (BookieException e) {
            LOG.error("Unauthorized access to ledger " + add.getLedgerId(), e);
            rc = BookieProtocol.EUA;
        }
        if (rc != BookieProtocol.EOK) {
            c.write(ResponseBuilder.buildErrorResponse(rc, req));
            if (statsEnabled) {
                bkStats.getOpStats(BKStats.STATS_ADD).incrementFailedOps();
            }
        }
    }

    @Override
    public void writeComplete(int rc, long ledgerId, long entryId,
                              InetSocketAddress addr, Object ctx) {
        assert(ctx instanceof AddCtx);
        AddCtx addctx = (AddCtx)ctx;
        addctx.c.write(ResponseBuilder.buildAddResponse(addctx.r));

        if (statsEnabled) {
            // compute the latency
            if (0 == rc) {
                // for add operations, we compute latency in writeComplete callbacks.
                long elapsedTime = MathUtils.now() - addctx.startTime;
                bkStats.getOpStats(BKStats.STATS_ADD).updateLatency(elapsedTime);
            } else {
                bkStats.getOpStats(BKStats.STATS_ADD).incrementFailedOps();
            }
        }
    }

    private void handleRead(BookieProtocol.Request req, Channel c) {
        assert (req.getHeader().hasReadRequest());
        DataFormats.ReadRequest read = req.getHeader().getReadRequest();

        LOG.debug("Received new read request: {}", read);
        int errorCode = BookieProtocol.EIO;
        long startTime = 0;
        if (statsEnabled) {
            startTime = MathUtils.now();
        }
        ByteBuffer data = null;
        try {
            Future<Boolean> fenceResult = null;
            if (read.getIsFencingRequest()) {
                LOG.warn("Ledger " + read.getLedgerId() + " fenced by " + c.getRemoteAddress());

                if (read.hasMasterKey()) {
                    fenceResult = bookie.fenceLedger(read.getLedgerId(),
                                                     read.getMasterKey().toByteArray());
                } else {
                    LOG.error("Password not provided, Not safe to fence {}", read.getLedgerId());
                    if (statsEnabled) {
                        bkStats.getOpStats(BKStats.STATS_READ).incrementFailedOps();
                    }
                    throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
                }
            }
            data = bookie.readEntry(read.getLedgerId(), read.getEntryId());
            LOG.debug("##### Read entry ##### {}", data.remaining());
            if (null != fenceResult) {
                // TODO:
                // currently we don't have readCallback to run in separated read
                // threads. after BOOKKEEPER-429 is complete, we could improve
                // following code to make it not wait here
                //
                // For now, since we only try to wait after read entry. so writing
                // to journal and read entry are executed in different thread
                // it would be fine.
                try {
                    Boolean fenced = fenceResult.get(1000, TimeUnit.MILLISECONDS);
                    if (null == fenced || !fenced) {
                        // if failed to fence, fail the read request to make it retry.
                        errorCode = BookieProtocol.EIO;
                        data = null;
                    } else {
                        errorCode = BookieProtocol.EOK;
                    }
                } catch (InterruptedException ie) {
                    LOG.error("Interrupting fence read entry " + read, ie);
                    errorCode = BookieProtocol.EIO;
                    data = null;
                } catch (ExecutionException ee) {
                    LOG.error("Failed to fence read entry " + read, ee);
                    errorCode = BookieProtocol.EIO;
                    data = null;
                } catch (TimeoutException te) {
                    LOG.error("Timeout to fence read entry " + read, te);
                    errorCode = BookieProtocol.EIO;
                    data = null;
                }
            } else {
                errorCode = BookieProtocol.EOK;
            }
        } catch (Bookie.NoLedgerException e) {
            if (LOG.isTraceEnabled()) {
                LOG.error("Error reading " + read, e);
            }
            errorCode = BookieProtocol.ENOLEDGER;
        } catch (Bookie.NoEntryException e) {
            if (LOG.isTraceEnabled()) {
                LOG.error("Error reading " + read, e);
            }
            errorCode = BookieProtocol.ENOENTRY;
        } catch (IOException e) {
            if (LOG.isTraceEnabled()) {
                LOG.error("Error reading " + read, e);
            }
            errorCode = BookieProtocol.EIO;
        } catch (BookieException e) {
            LOG.error("Unauthorized access to ledger " + read.getLedgerId(), e);
            errorCode = BookieProtocol.EUA;
        }

        LOG.trace("Read entry rc = {} for {}",
                  new Object[] { errorCode, read });
        if (errorCode == BookieProtocol.EOK) {
            assert data != null;

            c.write(ResponseBuilder.buildReadResponse(data, req));
            if (statsEnabled) {
                long elapsedTime = MathUtils.now() - startTime;
                bkStats.getOpStats(BKStats.STATS_READ).updateLatency(elapsedTime);
            }
        } else {
            c.write(ResponseBuilder.buildErrorResponse(errorCode, req));
            if (statsEnabled) {
                bkStats.getOpStats(BKStats.STATS_READ).incrementFailedOps();
            }
        }
    }
}
