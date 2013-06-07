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
 * Copyright 2013 Near Infinity Corporation.
 */


package com.nearinfinity.honeycomb.hbase.rowkey;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.nearinfinity.honeycomb.hbase.VarEncoder;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.util.Verify;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.UUID;

/**
 * Representation of the rowkey associated with data row content
 */
public class DataRowKey implements RowKey {
    private static final byte PREFIX = 0x06;
    private final long tableId;
    private final UUID uuid;

    /**
     * Creates a data rowkey for the specified table identifier
     *
     * @param tableId The valid table id that this data row belongs to
     */
    public DataRowKey(final long tableId) {
        this(tableId, null);
    }

    /**
     * Creates a data rowkey for the specified table identifier with the provided
     * universally unique identifier
     *
     * @param tableId The valid table id that this data row belongs to
     * @param uuid The {@link UUID} to associate with this data row
     */
    public DataRowKey(final long tableId, final UUID uuid) {
        Verify.isValidId(tableId);
        this.tableId = tableId;
        this.uuid = uuid;
    }

    @Override
    public byte[] encode() {
        if( uuid != null ) {
            return VarEncoder.appendByteArraysWithPrefix(PREFIX,
                    VarEncoder.encodeULong(tableId),
                    Util.UUIDToBytes(uuid));
        }

        return  VarEncoder.appendByteArraysWithPrefix(PREFIX, VarEncoder.encodeULong(tableId));
    }

    public long getTableId() {
        return tableId;
    }

    public UUID getUuid() {
        return uuid;
    }

    @Override
    public byte getPrefix() {
        return PREFIX;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this.getClass())
            .add("Prefix", String.format("%02X", PREFIX))
            .add("TableId", tableId)
            .add("UUID", uuid == null ? "" : Util.generateHexString(Util.UUIDToBytes(uuid)))
            .toString();
    }

    @Override
    public int compareTo(RowKey o) {
        int typeCompare = getPrefix() - o.getPrefix();
        if (typeCompare != 0) { return typeCompare; }
        DataRowKey row2 = (DataRowKey) o;
        return ComparisonChain.start()
                .compare(getTableId(), row2.getTableId())
                .compare(Util.UUIDToBytes(getUuid()),
                        Util.UUIDToBytes(row2.getUuid()),
                        new Bytes.ByteArrayComparator())
                .result();
    }
}
