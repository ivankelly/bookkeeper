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

public interface BookieProtocolLegacy {
    /** 
     * The first int of a packet is the header.
     * It contains the version, opCode and flags.
     * The initial versions of BK didn't have this structure
     * and just had an int representing the opCode as the 
     * first int. This handles that case also. 
     */
    static class PacketHeader {
        final byte opCode;
        final short flags;

        public PacketHeader(byte opCode, short flags) {
            this.opCode = opCode;
            this.flags = flags;
        }
        
        byte[] getBytes(byte version) {
            if (version == 0) {
                return new byte[] {0,0,opCode};
            } else {
                return new byte[] {opCode, (byte)(flags >> 8 & 0xFF), (byte)(flags & 0xFF)};
            }
        }

        static PacketHeader fromBytes(byte version, byte[] bytes) {
            byte opCode = 0;
            short flags = 0;
            if (version == 0) {
                opCode = bytes[2];
            } else {
                opCode = bytes[0];
                flags = (short)((bytes[1] << 8) | (bytes[2] & 0xFF));
            }
            return new PacketHeader(opCode, flags);
        }

        byte getOpCode() {
            return opCode;
        }

        short getFlags() {
            return flags;
        }
    }

    /**
     * The Add entry request payload will be a ledger entry exactly as it should
     * be logged. The response payload will be a 4-byte integer that has the
     * error code followed by the 8-byte ledger number and 8-byte entry number
     * of the entry written.
     */
    public static final byte ADDENTRY = 1;
    /**
     * The Read entry request payload will be the ledger number and entry number
     * to read. (The ledger number is an 8-byte integer and the entry number is
     * a 8-byte integer.) The response payload will be a 4-byte integer
     * representing an error code and a ledger entry if the error code is EOK,
     * otherwise it will be the 8-byte ledger number and the 4-byte entry number
     * requested. (Note that the first sixteen bytes of the entry happen to be
     * the ledger number and entry number as well.)
     */
    public static final byte READENTRY = 2;

    public static final short FLAG_NONE = 0x0;
    public static final short FLAG_DO_FENCING = 0x0001;
    public static final short FLAG_RECOVERY_ADD = 0x0002;
}
