/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie;

import java.nio.ByteBuffer;

/**
 * Journal stream position
 */
class LogMark implements Comparable<LogMark> {
    final long logFileId;
    final long logFileOffset;

    static final public LogMark MAX_VALUE = new LogMark(Long.MAX_VALUE, Long.MAX_VALUE);

    public static LogMark readLogMark(ByteBuffer bb) {
        long logFileId = bb.getLong();
        long logFileOffset = bb.getLong();
        return new LogMark(logFileId, logFileOffset);
    }

    public void writeLogMark(ByteBuffer bb) {
        bb.putLong(logFileId);
        bb.putLong(logFileOffset);
    }

    public LogMark() {
        this(0, 0);
    }

    public LogMark(long logFileId, long logFileOffset) {
        this.logFileId = logFileId;
        this.logFileOffset = logFileOffset;
    }

    public long getLogFileId() {
        return logFileId;
    }

    public long getLogFileOffset() {
        return logFileOffset;
    }

    @Override
    public int compareTo(LogMark other) {
        long ret = this.logFileId - other.logFileId;
        if (ret == 0) {
            ret = this.logFileOffset - other.logFileOffset;
        }
        return (ret < 0)? -1 : ((ret > 0)? 1 : 0);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("LogMark: logFileId - ").append(logFileId)
                .append(" , logFileOffset - ").append(logFileOffset);

        return sb.toString();
    }
}
