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

import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookieProtocol.VersionedResponse;
import org.apache.bookkeeper.proto.BookieProtocol.VersionedRequest;

import com.google.protobuf.ByteString;

class ResponseBuilder {
    static VersionedResponse buildErrorResponse(int errorCode, VersionedRequest r) {
        Response.Builder builder = Response.newBuilder();
        builder.setErrorCode(errorCode);
        if (r.getRequest().hasAddRequest()) {
            builder.setAddResponse(AddResponse.newBuilder()
                    .setLedgerId(r.getRequest().getAddRequest().getLedgerId())
                    .setEntryId(r.getRequest().getAddRequest().getEntryId())
                    .build());
        } else {
            builder.setReadResponse(ReadResponse.newBuilder()
                    .setLedgerId(r.getRequest().getReadRequest().getLedgerId())
                    .setEntryId(r.getRequest().getReadRequest().getEntryId())
                    .build());
        }
        return new VersionedResponse(r.getProtocolVersion(), builder.build());
    }

    static VersionedResponse buildAddResponse(VersionedRequest r) {
        Response.Builder builder = Response.newBuilder();
        builder.setErrorCode(BookieProtocol.EOK);
        builder.setAddResponse(AddResponse.newBuilder()
                .setLedgerId(r.getRequest().getAddRequest().getLedgerId())
                .setEntryId(r.getRequest().getAddRequest().getEntryId())
                .build());
        return new VersionedResponse(r.getProtocolVersion(), builder.build());
    }

    static VersionedResponse buildReadResponse(VersionedRequest r, ByteBuffer data) {
        Response.Builder builder = Response.newBuilder();
        builder.setErrorCode(BookieProtocol.EOK);
        builder.setReadResponse(ReadResponse.newBuilder()
                .setLedgerId(r.getRequest().getReadRequest().getLedgerId())
                .setEntryId(r.getRequest().getReadRequest().getEntryId())
                .setData(ByteString.copyFrom(data))
                .build());
        return new VersionedResponse(r.getProtocolVersion(), builder.build());
    }
}
