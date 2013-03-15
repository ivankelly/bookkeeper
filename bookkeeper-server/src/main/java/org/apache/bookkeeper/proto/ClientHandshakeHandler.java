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

import java.util.concurrent.atomic.AtomicBoolean;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.timeout.ReadTimeoutException;

import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;

import com.google.protobuf.ByteString;

import java.util.List;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for handshaking with server to find version
 */
class ClientHandshakeHandler extends SimpleChannelHandler {
    static Logger LOG = LoggerFactory.getLogger(BookieRequestHandler.class);

    final ClientHandshakeCallback callback;
    final ClientConfiguration conf;
    AtomicBoolean done = new AtomicBoolean(false);
    long reqSent = Long.MAX_VALUE;

    ClientHandshakeHandler(ClientConfiguration conf, ClientHandshakeCallback callback) {
        this.callback = callback;
        this.conf = conf;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        Throwable t = e.getCause();
        if (t instanceof ReadTimeoutException) {
            if ((MathUtils.now() - reqSent) > conf.getReadTimeout()*1000
                && done.compareAndSet(false, true)) {
                LOG.error("Timeout occurred handshaking with bookie, aborting connection", t);
                callback.connectFailure();
            }
        } else if (done.compareAndSet(false, true)) {
            LOG.error("Exception occurred handshaking with bookie, aborting connection", t);
            callback.connectFailure();
        }
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        Channel c = ctx.getChannel();
        DataFormats.HandshakeRequest.Builder handshake = DataFormats.HandshakeRequest.newBuilder()
            .setBackwardCompatBuffer(ByteString.copyFrom(new byte[3+8+8]));
        DataFormats.RequestHeader h = DataFormats.RequestHeader.newBuilder()
            .setHandshakeRequest(handshake.build()).build();
        reqSent = MathUtils.now();
        c.write(new BookieProtocol.Request(BookieProtocol.CURRENT_PROTOCOL_VERSION, h))
            .addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (!future.isSuccess() && done.compareAndSet(false, true)) {
                            callback.connectFailure();
                        }
                    }
                });
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        if (!(e.getMessage() instanceof BookieProtocol.Response) || done.get()) {
            ctx.sendUpstream(e);
            return;
        }
        final BookieProtocol.Response r = (BookieProtocol.Response)e.getMessage();
        if (r.getHeader().getErrorCode() == BookieProtocol.EBADVERSION
            && done.compareAndSet(false, true)) {
            callback.connectSuccess((byte)(BookieProtocol.FIRST_PROTOBUF_PROTOCOL_VERSION - 1));
        } else if (r.getHeader().getErrorCode() == BookieProtocol.EOK
                   && r.getHeader().hasHandshakeResponse()
                   && done.compareAndSet(false, true)) {
            callback.connectSuccess((byte)Math.min(r.getHeader().getHandshakeResponse().getCurrentProtocolVersion(),
                                   BookieProtocol.CURRENT_PROTOCOL_VERSION));
        } else {
            LOG.error("Received invalid response to handshake {}", r.getHeader());
        }
    }

    interface ClientHandshakeCallback {
        public void connectSuccess(byte protocolVersion);
        public void connectFailure();
    }
}
