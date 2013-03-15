package org.apache.bookkeeper.proto;

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

import org.jboss.netty.buffer.ChannelBuffer;
import java.nio.ByteBuffer;

import org.apache.bookkeeper.proto.DataFormats.RequestHeader;
import org.apache.bookkeeper.proto.DataFormats.ResponseHeader;
/**
 * The packets of the Bookie protocol all have a 4-byte integer indicating the
 * type of request or response at the very beginning of the packet followed by a
 * payload.
 *
 */
public interface BookieProtocol {

    /**
     * Lowest protocol version which will work with the bookie.
     */
    public static final byte LOWEST_COMPAT_PROTOCOL_VERSION = 0;

    /**
     * Current version of the protocol, which client will use. 
     */
    public static final byte CURRENT_PROTOCOL_VERSION = 3;

    /**
     * First version of the protocol which uses protobufs.
     */
    public static final byte FIRST_PROTOBUF_PROTOCOL_VERSION = 3;

    /**
     * Entry Entry ID. To be used when no valid entry id can be assigned.
     */
    public static final long INVALID_ENTRY_ID = -1;

    /**
     * Entry identifier representing a request to obtain the last add entry confirmed
     */
    public static final long LAST_ADD_CONFIRMED = -1;

    /**
     * The length of the master key in add packets. This
     * is fixed at 20 for historic reasons. This is because it
     * is always generated using the MacDigestManager regardless
     * of whether Mac is being used for the digest or not
     */
    public static final int MASTER_KEY_LENGTH = 20;

    /**
     * The error code that indicates success
     */
    public static final int EOK = 0;
    /**
     * The error code that indicates that the ledger does not exist
     */
    public static final int ENOLEDGER = 1;
    /**
     * The error code that indicates that the requested entry does not exist
     */
    public static final int ENOENTRY = 2;
    /**
     * The error code that indicates an invalid request type
     */
    public static final int EBADREQ = 100;
    /**
     * General error occurred at the server
     */
    public static final int EIO = 101;

    /**
     * Unauthorized access to ledger
     */
    public static final int EUA = 102;

    /**
     * The server version is incompatible with the client
     */
    public static final int EBADVERSION = 103;

    /**
     * Attempt to write to fenced ledger
     */
    public static final int EFENCED = 104;

    /**
     * The server is running as read-only mode
     */
    public static final int EREADONLY = 105;

    static class Request {
        final byte protocolVersion;
        final RequestHeader header;
        final ChannelBuffer data;

        Request(byte protocolVersion, RequestHeader header, ChannelBuffer data) {
            this.protocolVersion = protocolVersion;
            this.header = header;
            this.data = data;
        }

        Request(byte protocolVersion, RequestHeader header) {
            this(protocolVersion, header, null);
        }

        byte getProtocolVersion() {
            return protocolVersion;
        }

        RequestHeader getHeader() {
            return header;
        }

        boolean hasData() {
            return data != null;
        }

        ChannelBuffer getData() {
            return data;
        }

        ByteBuffer getDataAsByteBuffer() {
            return data.toByteBuffer().slice();
        }

        public String toString() {
            return header.toString();
        }
    }

    static class Response {
        final byte protocolVersion;
        final ResponseHeader header;
        final ChannelBuffer data;

        Response(byte protocolVersion, ResponseHeader header, ChannelBuffer data) {
            this.protocolVersion = protocolVersion;
            this.header = header;
            this.data = data;
        }

        Response(byte protocolVersion, ResponseHeader header) {
            this(protocolVersion, header, null);
        }

        byte getProtocolVersion() {
            return protocolVersion;
        }

        ResponseHeader getHeader() {
            return header;
        }

        boolean hasData() {
            return data != null;
        }

        ChannelBuffer getData() {
            return data;
        }

        ByteBuffer getDataAsByteBuffer() {
            return data.toByteBuffer().slice();
        }

        public String toString() {
            return header.toString();
        }
    }
}
