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
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;

import org.apache.bookkeeper.proto.BookieProtocolLegacy.PacketHeader;
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
            if (r.getProtocolVersion() >= BookieProtocol.FIRST_PROTOBUF_PROTOCOL_VERSION) {
                // 1 for version, 4 for PB delimiter, then size of header
                int totalHeaderSize = 1 + 4 + r.getHeader().getSerializedSize();
                ChannelBuffer buf = channel.getConfig().getBufferFactory()
                    .getBuffer(totalHeaderSize);
                buf.writeByte(r.getProtocolVersion());
                r.getHeader().writeDelimitedTo(new ChannelBufferOutputStream(buf));
                return r.hasData() ? ChannelBuffers.wrappedBuffer(buf, r.getData()) : buf;
            }

            // Otherwise we do legacy handling
            if (r.getHeader().hasAddRequest()) {
                assert(r.hasData());

                DataFormats.AddRequest add = r.getHeader().getAddRequest();
                int totalHeaderSize = 4 // for the header
                    + BookieProtocol.MASTER_KEY_LENGTH; // for the master key
                ChannelBuffer buf = channel.getConfig().getBufferFactory().getBuffer(totalHeaderSize);
                short flags = BookieProtocolLegacy.FLAG_NONE;
                if (add.getIsRecoveryAdd()) {
                    flags |= BookieProtocolLegacy.FLAG_RECOVERY_ADD;
                }
                buf.writeByte(r.getProtocolVersion());
                buf.writeBytes(new PacketHeader(BookieProtocolLegacy.ADDENTRY, flags)
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
                short flags = BookieProtocolLegacy.FLAG_NONE;
                if (read.getIsFencingRequest()) {
                    flags |= BookieProtocolLegacy.FLAG_DO_FENCING;
                }
                buf.writeByte(r.getProtocolVersion());
                buf.writeBytes(new PacketHeader(BookieProtocolLegacy.READENTRY, flags)
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
            DataFormats.RequestHeader.Builder builder = DataFormats.RequestHeader.newBuilder();
            byte version = packet.readByte();
            if (version >= BookieProtocol.FIRST_PROTOBUF_PROTOCOL_VERSION) {
                builder.mergeDelimitedFrom(new ChannelBufferInputStream(packet));
                DataFormats.RequestHeader h = builder.build();
                if (h.hasAddRequest()) {
                    return new BookieProtocol.Request(version, h, packet.slice());
                } else {
                    return new BookieProtocol.Request(version, h);
                }
            }

            // else do legacy handling
            byte[] headerBytes = new byte[3];
            packet.readBytes(headerBytes, 0, 3);
            PacketHeader h = PacketHeader.fromBytes(version, headerBytes);

            // packet format is different between ADDENTRY and READENTRY
            long ledgerId = -1;
            long entryId = BookieProtocol.INVALID_ENTRY_ID;
            byte[] masterKey = null;
            short flags = h.getFlags();

            ServerStats.getInstance().incrementPacketsReceived();

            switch (h.getOpCode()) {
            case BookieProtocolLegacy.ADDENTRY:
                // first read master key
                masterKey = new byte[BookieProtocol.MASTER_KEY_LENGTH];
                packet.readBytes(masterKey, 0, BookieProtocol.MASTER_KEY_LENGTH);

                ChannelBuffer bb = packet.duplicate();
                DataFormats.AddRequest.Builder addRequest = DataFormats.AddRequest.newBuilder()
                    .setLedgerId(bb.readLong()).setEntryId(bb.readLong())
                    .setMasterKey(ByteString.copyFrom(masterKey));
                if ((flags & BookieProtocolLegacy.FLAG_RECOVERY_ADD)
                    == BookieProtocolLegacy.FLAG_RECOVERY_ADD) {
                    addRequest.setIsRecoveryAdd(true);
                }
                builder.setAddRequest(addRequest.build());
                return new BookieProtocol.Request(version, builder.build(), packet.slice());
            case BookieProtocolLegacy.READENTRY:
                DataFormats.ReadRequest.Builder readRequest
                    = DataFormats.ReadRequest.newBuilder()
                    .setLedgerId(packet.readLong()).setEntryId(packet.readLong());

                if ((flags & BookieProtocolLegacy.FLAG_DO_FENCING)
                    == BookieProtocolLegacy.FLAG_DO_FENCING
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

            if (r.getProtocolVersion() >= BookieProtocol.FIRST_PROTOBUF_PROTOCOL_VERSION) {
                // 1 for version, 4 for PB delimiter, then size of header
                int totalHeaderSize = 1 + 4 + r.getHeader().getSerializedSize();
                ChannelBuffer buf = channel.getConfig().getBufferFactory()
                    .getBuffer(totalHeaderSize);
                buf.writeByte(r.getProtocolVersion());
                r.getHeader().writeDelimitedTo(new ChannelBufferOutputStream(buf));
                return r.hasData() ? ChannelBuffers.wrappedBuffer(buf, r.getData()) : buf;
            }

            // else do legacy handling
            ChannelBuffer buf = ctx.getChannel().getConfig().getBufferFactory()
                .getBuffer(24);
            buf.writeByte(r.getProtocolVersion());
            byte opCode = 0;
            if (r.getHeader().hasReadResponse()) {
                opCode = BookieProtocolLegacy.READENTRY;
            } else {
                assert (r.getHeader().hasAddResponse());
                opCode = BookieProtocolLegacy.ADDENTRY;
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
            DataFormats.ResponseHeader.Builder builder = DataFormats.ResponseHeader.newBuilder();
            byte version = buffer.readByte();

            if (version >= BookieProtocol.FIRST_PROTOBUF_PROTOCOL_VERSION) {
                builder.mergeDelimitedFrom(new ChannelBufferInputStream(buffer));
                DataFormats.ResponseHeader h = builder.build();
                if (h.hasReadResponse()) {
                    return new BookieProtocol.Response(version, h, buffer.slice());
                } else {
                    return new BookieProtocol.Response(version, h);
                }
            }

            // else legacy handling
            byte[] headerBytes = new byte[3];
            buffer.readBytes(headerBytes, 0, 3);
            PacketHeader h = PacketHeader.fromBytes(version, headerBytes);
            int rc = buffer.readInt();
            builder.setErrorCode(rc);

            switch (h.getOpCode()) {
            case BookieProtocolLegacy.ADDENTRY:
                builder.setAddResponse(DataFormats.AddResponse.newBuilder()
                                       .setLedgerId(buffer.readLong())
                                       .setEntryId(buffer.readLong()).build());
                return new BookieProtocol.Response(version, builder.build());
            case BookieProtocolLegacy.READENTRY:
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
