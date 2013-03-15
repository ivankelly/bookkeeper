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

import java.nio.ByteBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

class ResponseBuilder {
    static BookieProtocol.Response buildErrorResponse(int errorCode, BookieProtocol.Request r) {
        return buildErrorResponse(r.getProtocolVersion(), errorCode, r);
    }

    static BookieProtocol.Response buildErrorResponse(byte protocolVersion, int errorCode, BookieProtocol.Request r) {
        DataFormats.ResponseHeader.Builder builder = DataFormats.ResponseHeader.newBuilder();
        builder.setErrorCode(errorCode);
        if (r.getHeader().hasAddRequest()) {
            builder.setAddResponse(DataFormats.AddResponse.newBuilder()
                                   .setLedgerId(r.getHeader().getAddRequest().getLedgerId())
                                   .setEntryId(r.getHeader().getAddRequest().getEntryId())
                                   .build());
        } else {
            assert(r.getHeader().hasReadRequest());
            builder.setReadResponse(DataFormats.ReadResponse.newBuilder()
                                    .setLedgerId(r.getHeader().getReadRequest().getLedgerId())
                                    .setEntryId(r.getHeader().getReadRequest().getEntryId())
                                    .build());
        }
        return new BookieProtocol.Response(protocolVersion, builder.build());
    }

    static BookieProtocol.Response buildAddResponse(BookieProtocol.Request r) {
        DataFormats.ResponseHeader.Builder builder = DataFormats.ResponseHeader.newBuilder();
        builder.setErrorCode(BookieProtocol.EOK);
        builder.setAddResponse(DataFormats.AddResponse.newBuilder()
                               .setLedgerId(r.getHeader().getAddRequest().getLedgerId())
                               .setEntryId(r.getHeader().getAddRequest().getEntryId())
                               .build());
        return new BookieProtocol.Response(r.getProtocolVersion(), builder.build());
    }

    static BookieProtocol.Response buildReadResponse(ByteBuffer data, BookieProtocol.Request r) {
        DataFormats.ResponseHeader.Builder builder = DataFormats.ResponseHeader.newBuilder();
        builder.setErrorCode(BookieProtocol.EOK);
        builder.setReadResponse(DataFormats.ReadResponse.newBuilder()
                                .setLedgerId(r.getHeader().getReadRequest().getLedgerId())
                                .setEntryId(r.getHeader().getReadRequest().getEntryId())
                                .build());
        return new BookieProtocol.Response(r.getProtocolVersion(), builder.build(),
                                           ChannelBuffers.wrappedBuffer(data));
    }
}
