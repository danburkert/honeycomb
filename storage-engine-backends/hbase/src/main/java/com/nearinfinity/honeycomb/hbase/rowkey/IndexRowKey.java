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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.gotometrics.orderly.FixedByteArrayRowKey;
import com.gotometrics.orderly.Order;
import com.gotometrics.orderly.StructRowKey;
import com.gotometrics.orderly.Termination;
import com.gotometrics.orderly.UnsignedLongRowKey;
import com.nearinfinity.honeycomb.exceptions.RuntimeIOException;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.util.Verify;

/**
 * Super class for index rowkeys
 */
public abstract class IndexRowKey implements RowKey {
    private final byte prefix;
    private final long tableId;
    private final long indexId;
    private final UUID uuid;
    private final List<RowKeyValue> records;
    private final SortOrder sortOrder;
    private final byte[] notNullBytes;
    private final byte[] nullBytes;

    protected IndexRowKey(final long tableId,
                          final long indexId,
                          final List<RowKeyValue> records,
                          final UUID uuid) {
        Verify.isValidId(tableId);
        checkArgument(indexId >= 0, "Index ID must be non-zero.");
        checkNotNull(records, "Records cannot be null");
        prefix = checkNotNull(getPrefix(), "Prefix cannot be null");
        sortOrder = checkNotNull(getSortOrder(), "Sort order cannot be null");
        notNullBytes = checkNotNull(getNotNullBytes(), "Not null bytes cannot be null");
        nullBytes = checkNotNull(getNullBytes(), "Null bytes cannot be null");

        this.uuid = uuid;
        this.tableId = tableId;
        this.indexId = indexId;
        this.records = records;
    }

    @Override
    public byte[] encode() {
        final byte[] prefixBytes = {prefix};
        List<RowKeyValue> encodingList = getRowKeyValues();

        com.gotometrics.orderly.RowKey[] fields = new com.gotometrics.orderly.RowKey[encodingList.size()];
        Object[] objects = new Object[encodingList.size()];
        int i = 0;
        for (RowKeyValue rowKeyValue : encodingList) {
            fields[i] = rowKeyValue.getRowKey();
            objects[i] = rowKeyValue.getValue();
            i++;
        }

        StructRowKey rowKey = new StructRowKey(fields);
        rowKey.setOrder(sortOrder == SortOrder.Ascending ? Order.ASCENDING : Order.DESCENDING);
        rowKey.setTermination(Termination.MUST);

        try {
            byte[] serialize = rowKey.serialize(objects);
            return Util.appendByteArrays(Lists.newArrayList(prefixBytes, serialize));
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
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

    protected abstract SortOrder getSortOrder();

    private byte[] getNotNullBytes() {
        return new byte[]{0x01};
    }

    private byte[] getNullBytes() {
        return new byte[]{0x00};
    }

    private List<RowKeyValue> getRowKeyValues() {
        List<RowKeyValue> encodingList = Lists.newArrayList();
        encodingList.add(new RowKeyValue(new UnsignedLongRowKey(), tableId));
        encodingList.add(new RowKeyValue(new UnsignedLongRowKey(), indexId));
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
            strings.add(bytes == null ? "null" : Util.generateHexString(bytes.serialize()));
        }

        return strings;
    }
}
