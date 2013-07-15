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

import java.util.UUID;

import com.google.common.base.Objects;
import com.gotometrics.orderly.UnsignedLongRowKey;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.util.Verify;

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
        final byte[] serializedTableId = new RowKeyValue(new UnsignedLongRowKey(), tableId).serialize();

        if( uuid != null ) {
            return Util.appendByteArraysWithPrefix(PREFIX, serializedTableId,
                    Util.UUIDToBytes(uuid));
        }

        return Util.appendByteArraysWithPrefix(PREFIX, serializedTableId);
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
}
