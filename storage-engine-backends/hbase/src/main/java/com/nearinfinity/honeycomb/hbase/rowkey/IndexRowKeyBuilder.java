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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.gotometrics.orderly.*;
import com.nearinfinity.honeycomb.exceptions.RuntimeIOException;
import com.nearinfinity.honeycomb.hbase.VarEncoder;
import com.nearinfinity.honeycomb.mysql.QueryKey;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import com.nearinfinity.honeycomb.util.Verify;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * A builder for creating {@link IndexRowKey} instances.  Builder instances can be reused as it is safe
 * to call {@link #build} multiple times.
 */
public class IndexRowKeyBuilder {


    private final long tableId;
    private final long indexId;
    private SortOrder order;
    private String indexName;
    private TableSchema tableSchema;
    private Map<String, ByteBuffer> fields;
    private UUID uuid;

    private IndexRowKeyBuilder(long tableId, long indexId) {
        this.tableId = tableId;
        this.indexId = indexId;
    }

    /**
     * Creates a builder with the specified table and index identifiers.
     *
     * @param tableId The valid table id that the {@link IndexRowKey} will correspond to
     * @param indexId The valid index id used to identify the {@link IndexRowKey}
     * @return The current builder instance
     */
    public static IndexRowKeyBuilder newBuilder(long tableId,
                                                long indexId) {
        Verify.isValidId(tableId);
        Verify.isValidId(indexId);

        return new IndexRowKeyBuilder(tableId, indexId);
    }

    /**
     * Create a key for a {@link org.apache.hadoop.hbase.client.Scan} with just a table's indices.
     *
     * @param tableId   Table ID
     * @param sortOrder Ascending/Descending index
     * @return Start of index for a table.
     */
    public static byte[] createDropTableIndexKey(long tableId, SortOrder sortOrder) {
        Verify.isValidId(tableId);
        checkNotNull(sortOrder);

        byte[] prefix;
        Order order;
        LongRowKey rowKey = new LongRowKey();
        if (sortOrder == SortOrder.Ascending) {
            prefix = new byte[]{(byte) 0x07};
            order = Order.ASCENDING;
        } else {
            prefix = new byte[]{(byte) 0x08};
            order = Order.DESCENDING;
        }

        rowKey.setOrder(order);
        try {
            byte[] serialize = rowKey.serialize(tableId);
            return VarEncoder.appendByteArrays(Lists.newArrayList(prefix, serialize));
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private static IndexRowKey.RowKeyValue encodeValue(final ByteBuffer value, final ColumnSchema columnSchema) {
        try {
            switch (columnSchema.getType()) {
                case LONG:
                case TIME: {
                    return new IndexRowKey.RowKeyValue(new LongRowKey(), value.getLong());
                }
                case DOUBLE: {
                    return new IndexRowKey.RowKeyValue(new DoubleRowKey(), value.getDouble());
                }
                case BINARY:{
                    int maxLength = columnSchema.getMaxLength();
                    byte[] bytes = Arrays.copyOf(value.array(), maxLength);
                    return new IndexRowKey.RowKeyValue(new FixedByteArrayRowKey(maxLength), bytes);
                }
                case STRING: {
                    UTF8RowKey encoder = new UTF8RowKey();
                    encoder.setTermination(Termination.MUST);
                    return new IndexRowKey.RowKeyValue(encoder, value.array());
                }
                default:
                    byte[] array = value.array();
                    return new IndexRowKey.RowKeyValue(new FixedByteArrayRowKey(array.length), array);
            }
        } finally {
            value.rewind(); // rewind the ByteBuffer's index pointer
        }
    }

    /**
     * Adds the specified {@link SortOrder} to the builder instance being constructed
     *
     * @param order The sort order to use during the build phase, not null
     * @return The current builder instance
     */
    public IndexRowKeyBuilder withSortOrder(SortOrder order) {
        checkNotNull(order, "Order must not be null");
        this.order = order;
        return this;
    }

    /**
     * Set the values of the index row based on a sql row. If an index column is
     * missing from the sql row it is replaced with an explicit null. (This
     * method is intended for insert)
     *
     * @param row         SQL row
     * @param indexName   Columns in the index
     * @param tableSchema Table schema
     * @return The current builder instance
     */
    public IndexRowKeyBuilder withRow(Row row,
                                      String indexName,
                                      TableSchema tableSchema) {
        checkNotNull(row, "row must not be null.");
        Map<String, ByteBuffer> recordCopy = Maps.newHashMap(row.getRecords());
        for (String column : tableSchema.getIndexSchema(indexName).getColumns()) {
            if (!recordCopy.containsKey(column)) {
                recordCopy.put(column, null);
            }
        }

        this.fields = recordCopy;
        this.indexName = indexName;
        this.tableSchema = tableSchema;
        return this;
    }

    /**
     * Set the values of the index row based on a QueryKey.
     *
     * @param queryKey    Query key
     * @param tableSchema Table schema
     * @return The current builder instance
     */
    public IndexRowKeyBuilder withQueryKey(QueryKey queryKey,
                                           TableSchema tableSchema) {
        checkNotNull(queryKey, "queryKey must not be null.");
        checkNotNull(tableSchema, "tableSchema must not be null.");
        this.fields = queryKey.getKeys();
        this.indexName = queryKey.getIndexName();
        this.tableSchema = tableSchema;
        return this;
    }

    /**
     * Adds the specified {@link UUID} to the builder instance being constructed
     *
     * @param uuid The identifier to use during the build phase, not null
     * @return The current builder instance
     */
    public IndexRowKeyBuilder withUUID(UUID uuid) {
        checkNotNull(uuid, "UUID must not be null");
        this.uuid = uuid;
        return this;
    }

    /**
     * Creates an {@link IndexRowKey} instance with the parameters supplied to the builder.
     * Precondition:
     *
     * @return A new row instance constructed by the builder
     */
    public IndexRowKey build() {
        checkState(order != null, "Sort order must be set on IndexRowBuilder.");
        List<IndexRowKey.RowKeyValue> encodedRecords = Lists.newArrayList();
        if (fields != null) {
            for (String column : tableSchema.getIndexSchema(indexName).getColumns()) {
                if (!fields.containsKey(column)) {
                    continue;
                }
                ByteBuffer record = fields.get(column);
                if (record != null) {
                    encodedRecords.add(encodeValue(record, tableSchema.getColumnSchema(column)));
                } else {
                    encodedRecords.add(null);
                }
            }
        }

        if (order == SortOrder.Ascending) {
            return new AscIndexRowKey(tableId, indexId, encodedRecords, uuid);
        }

        return new DescIndexRowKey(tableId, indexId, encodedRecords, uuid);
    }

    /**
     * Representation of the rowkey associated with an index in descending order
     * for data row content
     */
    private static class DescIndexRowKey extends IndexRowKey {

        public DescIndexRowKey(final long tableId, final long indexId,
                               final List<IndexRowKey.RowKeyValue> records, final UUID uuid) {
            super(tableId, indexId, records, uuid);
        }

        @Override
        protected SortOrder getSortOrder() {
            return SortOrder.Descending;
        }

        @Override
        protected byte[] getNotNullBytes() {
            return new byte[]{0x00};
        }

        @Override
        protected byte[] getNullBytes() {
            return new byte[]{0x01};
        }

        @Override
        public byte getPrefix() {
            return (byte) 0x08;
        }
    }

    /**
     * Representation of the rowkey associated with an index in ascending order
     * for data row content
     */
    private static class AscIndexRowKey extends IndexRowKey {

        public AscIndexRowKey(final long tableId, final long indexId,
                              final List<IndexRowKey.RowKeyValue> records, final UUID uuid) {
            super(tableId, indexId, records, uuid);
        }

        @Override
        protected SortOrder getSortOrder() {
            return SortOrder.Ascending;
        }

        @Override
        protected byte[] getNotNullBytes() {
            return new byte[]{0x01};
        }

        @Override
        protected byte[] getNullBytes() {
            return new byte[]{0x00};
        }

        @Override
        public byte getPrefix() {
            return (byte) 0x07;
        }
    }
}
