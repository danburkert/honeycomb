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
import com.google.common.base.Preconditions;
import com.nearinfinity.honeycomb.hbase.VarEncoder;

/**
 * Super class for rowkeys that occur once per MySQL table
 */
public class TableIDRowKey implements RowKey {
    private final byte prefix;
    private final long tableId;

    public TableIDRowKey(final byte prefix, final long tableId) {
        Preconditions.checkArgument(tableId >= 0, "Table ID must be non-zero.");
        this.prefix = prefix;
        this.tableId = tableId;
    }

    @Override
    public byte[] encode() {
        byte[] table = VarEncoder.encodeULong(tableId);
        return VarEncoder.appendByteArraysWithPrefix(prefix, table);
    }

    @Override
    public byte getPrefix() {
        return prefix;
    }

    public long getTableId() {
        return tableId;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this.getClass())
                .add("Prefix", String.format("%02X", prefix))
                .add("TableId", tableId)
                .toString();
    }
}