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

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;

import org.apache.bookkeeper.proto.BookieProtocol.PacketHeader;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;

import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BookieProtoEncoding {
    static Logger LOG = LoggerFactory.getLogger(BookieProtoEncoding.class);

    public static class RequestEncoder extends OneToOneEncoder {
        @Override
        public Object encode(ChannelHandlerContext ctx, Channel channel, Object msg)
                throws Exception {
            if (!(msg instanceof BookieProtocol.Request)) {
                return msg;
            }
            BookieProtocol.Request r = (BookieProtocol.Request)msg;
            if (r.getHeader().hasAddRequest()) {
                assert(r.hasData());

                DataFormats.AddRequest add = r.getHeader().getAddRequest();
                int totalHeaderSize = 4 // for the header
                    + BookieProtocol.MASTER_KEY_LENGTH; // for the master key
                ChannelBuffer buf = channel.getConfig().getBufferFactory().getBuffer(totalHeaderSize);
                short flags = BookieProtocol.FLAG_NONE;
                if (add.getIsRecoveryAdd()) {
                    flags |= BookieProtocol.FLAG_RECOVERY_ADD;
                }
                buf.writeByte(r.getProtocolVersion());
                buf.writeBytes(new PacketHeader(BookieProtocol.ADDENTRY, flags)
                        .getBytes(r.getProtocolVersion()));
                buf.writeBytes(add.getMasterKey().toByteArray(), 0,
                               BookieProtocol.MASTER_KEY_LENGTH);

                return ChannelBuffers.wrappedBuffer(buf, r.getData());
            } else if (r.getHeader().hasReadRequest()) {
                int totalHeaderSize = 4 // for request type
                    + 8 // for ledgerId
                    + 8; // for entryId
                DataFormats.ReadRequest read = r.getHeader().getReadRequest();
                if (read.hasMasterKey()) {
                    totalHeaderSize += BookieProtocol.MASTER_KEY_LENGTH;
                }

                ChannelBuffer buf = channel.getConfig().getBufferFactory().getBuffer(totalHeaderSize);
                short flags = BookieProtocol.FLAG_NONE;
                if (read.getIsFencingRequest()) {
                    flags |= BookieProtocol.FLAG_DO_FENCING;
                }
                buf.writeByte(r.getProtocolVersion());
                buf.writeBytes(new PacketHeader(BookieProtocol.READENTRY, flags)
                        .getBytes(r.getProtocolVersion()));
                buf.writeLong(read.getLedgerId());
                buf.writeLong(read.getEntryId());
                if (read.hasMasterKey()) {
                    buf.writeBytes(read.getMasterKey().toByteArray(), 0,
                                   BookieProtocol.MASTER_KEY_LENGTH);
                }

                return buf;
            } else {
                LOG.warn("Unknown message format {}", r);
                assert (false);
                return msg;
            }
        }
    }

    public static class RequestDecoder extends OneToOneDecoder {
        @Override
        public Object decode(ChannelHandlerContext ctx, Channel channel, Object msg)
                throws Exception {
            if (!(msg instanceof ChannelBuffer)) {
                return msg;
            }
            ChannelBuffer packet = (ChannelBuffer)msg;
            byte version = packet.readByte();
            byte[] headerBytes = new byte[3];
            packet.readBytes(headerBytes, 0, 3);
            PacketHeader h = PacketHeader.fromBytes(version, headerBytes);

            // packet format is different between ADDENTRY and READENTRY
            long ledgerId = -1;
            long entryId = BookieProtocol.INVALID_ENTRY_ID;
            byte[] masterKey = null;
            short flags = h.getFlags();

            ServerStats.getInstance().incrementPacketsReceived();

            DataFormats.RequestHeader.Builder builder = DataFormats.RequestHeader.newBuilder();
            switch (h.getOpCode()) {
            case BookieProtocol.ADDENTRY:
                // first read master key
                masterKey = new byte[BookieProtocol.MASTER_KEY_LENGTH];
                packet.readBytes(masterKey, 0, BookieProtocol.MASTER_KEY_LENGTH);

                ChannelBuffer bb = packet.duplicate();
                DataFormats.AddRequest.Builder addRequest = DataFormats.AddRequest.newBuilder()
                    .setLedgerId(bb.readLong()).setEntryId(bb.readLong())
                    .setMasterKey(ByteString.copyFrom(masterKey));
                if ((flags & BookieProtocol.FLAG_RECOVERY_ADD) == BookieProtocol.FLAG_RECOVERY_ADD) {
                    addRequest.setIsRecoveryAdd(true);
                }
                builder.setAddRequest(addRequest.build());
                return new BookieProtocol.Request(version, builder.build(), packet.slice());
            case BookieProtocol.READENTRY:
                DataFormats.ReadRequest.Builder readRequest
                    = DataFormats.ReadRequest.newBuilder()
                    .setLedgerId(packet.readLong()).setEntryId(packet.readLong());

                if ((flags & BookieProtocol.FLAG_DO_FENCING) == BookieProtocol.FLAG_DO_FENCING
                    && version >= 2) {
                    masterKey = new byte[BookieProtocol.MASTER_KEY_LENGTH];
                    packet.readBytes(masterKey, 0, BookieProtocol.MASTER_KEY_LENGTH);
                    readRequest.setMasterKey(ByteString.copyFrom(masterKey))
                        .setIsFencingRequest(true);
                }
                builder.setReadRequest(readRequest.build());
                return new BookieProtocol.Request(version, builder.build());
            }
            return msg;
        }
    }

    public static class ResponseEncoder extends OneToOneEncoder {
        @Override
        public Object encode(ChannelHandlerContext ctx, Channel channel, Object msg)
                throws Exception {
            if (!(msg instanceof BookieProtocol.Response)) {
                return msg;
            }
            BookieProtocol.Response r = (BookieProtocol.Response)msg;
            ChannelBuffer buf = ctx.getChannel().getConfig().getBufferFactory()
                .getBuffer(24);
            buf.writeByte(r.getProtocolVersion());
            byte opCode = 0;
            if (r.getHeader().hasReadResponse()) {
                opCode = BookieProtocol.READENTRY;
            } else {
                assert (r.getHeader().hasAddResponse());
                opCode = BookieProtocol.ADDENTRY;
            }

            buf.writeBytes(new PacketHeader(opCode, (short)0).getBytes(r.getProtocolVersion()));
            buf.writeInt(r.getHeader().getErrorCode());

            ServerStats.getInstance().incrementPacketsSent();
            if (r.getHeader().hasReadResponse()) {
                buf.writeLong(r.getHeader().getReadResponse().getLedgerId());
                buf.writeLong(r.getHeader().getReadResponse().getEntryId());

                if (r.hasData()) {
                    return ChannelBuffers.wrappedBuffer(buf,
                                                        ChannelBuffers.wrappedBuffer(r.getData()));
                } else {
                    return buf;
                }
            } else if (r.getHeader().hasAddResponse()) {
                buf.writeLong(r.getHeader().getAddResponse().getLedgerId());
                buf.writeLong(r.getHeader().getAddResponse().getEntryId());

                return buf;
            } else {
                LOG.error("Cannot encode unknown response type {}", msg.getClass().getName());
                return msg;
            }
        }
    }

    public static class ResponseDecoder extends OneToOneDecoder {
        @Override
        public Object decode(ChannelHandlerContext ctx, Channel channel, Object msg)
                throws Exception {
            if (!(msg instanceof ChannelBuffer)) {
                return msg;
            }

            final ChannelBuffer buffer = (ChannelBuffer)msg;

            byte version = buffer.readByte();
            byte[] headerBytes = new byte[3];
            buffer.readBytes(headerBytes, 0, 3);
            PacketHeader h = PacketHeader.fromBytes(version, headerBytes);
            DataFormats.ResponseHeader.Builder builder = DataFormats.ResponseHeader.newBuilder();
            int rc = buffer.readInt();
            builder.setErrorCode(rc);

            switch (h.getOpCode()) {
            case BookieProtocol.ADDENTRY:
                builder.setAddResponse(DataFormats.AddResponse.newBuilder()
                                       .setLedgerId(buffer.readLong())
                                       .setEntryId(buffer.readLong()).build());
                return new BookieProtocol.Response(version, builder.build());
            case BookieProtocol.READENTRY:
                builder.setReadResponse(DataFormats.ReadResponse.newBuilder()
                                        .setLedgerId(buffer.readLong())
                                        .setEntryId(buffer.readLong()).build());
                if (rc == BookieProtocol.EOK) {
                    return new BookieProtocol.Response(version, builder.build(), buffer.slice());
                } else {
                    return new BookieProtocol.Response(version, builder.build());
                }
            default:
                LOG.error("Unexpected response of type {} received from {}",
                          h.getOpCode(), channel.getRemoteAddress());
                return msg;
            }
        }
    }
}
