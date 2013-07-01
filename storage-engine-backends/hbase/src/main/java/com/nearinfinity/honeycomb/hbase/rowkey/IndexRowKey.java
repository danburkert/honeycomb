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
import com.google.common.collect.Lists;
import com.gotometrics.orderly.FixedByteArrayRowKey;
import com.gotometrics.orderly.LongRowKey;
import com.gotometrics.orderly.Order;
import com.gotometrics.orderly.StructRowKey;
import com.nearinfinity.honeycomb.exceptions.RuntimeIOException;
import com.nearinfinity.honeycomb.hbase.VarEncoder;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.util.Verify;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Super class for index rowkeys
 */
public abstract class IndexRowKey implements RowKey {
    private final byte prefix;
    private final long tableId;
    private final long indexId;
    private final UUID uuid;
    private final List<IndexRowKey.RowKeyValue> records;
    private final SortOrder sortOrder;
    private final byte[] notNullBytes;
    private final byte[] nullBytes;

    protected IndexRowKey(final long tableId,
                          final long indexId,
                          final List<IndexRowKey.RowKeyValue> records,
                          final UUID uuid,
                          final byte prefix,
                          final byte[] notNullBytes,
                          final byte[] nullBytes,
                          final SortOrder sortOrder) {
        Verify.isValidId(tableId);
        checkArgument(indexId >= 0, "Index ID must be non-zero.");
        checkNotNull(prefix, "Prefix cannot be null");
        checkNotNull(notNullBytes, "Not null bytes cannot be null");
        checkNotNull(nullBytes, "Null bytes cannot be null");
        this.uuid = uuid;
        this.tableId = tableId;
        this.indexId = indexId;
        this.prefix = prefix;
        this.records = records;
        this.sortOrder = sortOrder;
        this.notNullBytes = notNullBytes;
        this.nullBytes = nullBytes;
    }

    @Override
    public byte[] encode() {
        final byte[] prefixBytes = {prefix};
        List<RowKeyValue> encodingList = getRowKeyValues();

        com.gotometrics.orderly.RowKey[] fields = new com.gotometrics.orderly.RowKey[encodingList.size()];
        Object[] objects = new Object[encodingList.size()];
        int i = 0;
        for (RowKeyValue rowKeyValue : encodingList) {
            fields[i] = rowKeyValue.rowKey;
            objects[i] = rowKeyValue.value;
            i++;
        }

        StructRowKey rowKey = new StructRowKey(fields);
        rowKey.setOrder(this.sortOrder == SortOrder.Ascending ? Order.ASCENDING : Order.DESCENDING);

        try {
            byte[] serialize = rowKey.serialize(objects);
            return VarEncoder.appendByteArrays(Lists.newArrayList(prefixBytes, serialize));
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public byte getPrefix() {
        return prefix;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this.getClass())
                .add("Prefix", String.format("%02X", prefix))
                .add("TableId", tableId)
                .add("IndexId", indexId)
                .add("Records", records == null ? "" : recordValueStrings())
                .add("UUID", uuid == null ? "" : Util.generateHexString(Util.UUIDToBytes(uuid)))
                .toString();
    }

    private List<RowKeyValue> getRowKeyValues() {
        List<RowKeyValue> encodingList = Lists.newArrayList();
        encodingList.add(new RowKeyValue(new LongRowKey(), tableId));
        encodingList.add(new RowKeyValue(new LongRowKey(), indexId));
        for (RowKeyValue record : records) {
            encodingList.add(new RowKeyValue(new FixedByteArrayRowKey(1), record == null ? nullBytes : notNullBytes));
            if (record != null) {
                encodingList.add(record);
            }
        }

        if (uuid != null) {
            encodingList.add(new RowKeyValue(new FixedByteArrayRowKey(16), Util.UUIDToBytes(uuid)));
        }

        return encodingList;
    }

    private List<String> recordValueStrings() {
        final List<String> strings = Lists.newArrayList();

        for (final RowKeyValue bytes : records) {
            strings.add((bytes == null) ? "null" : Util.generateHexString(bytes.serialize()));
        }

        return strings;
    }

    public static class RowKeyValue {
        private final com.gotometrics.orderly.RowKey rowKey;
        private final Object value;

        public RowKeyValue(com.gotometrics.orderly.RowKey rowKey, Object value) {
            this.rowKey = rowKey;
            this.value = value;
        }

        public byte[] serialize() {
            if (value == null)
                return null;

            try {
                return rowKey.serialize(value);
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        }
    }
}
