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
 * Copyright 2013 Altamira Corporation.
 */


package com.nearinfinity.honeycomb.hbase;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.nearinfinity.honeycomb.MockHTable;
import com.nearinfinity.honeycomb.exceptions.TableNotFoundException;
import com.nearinfinity.honeycomb.hbase.rowkey.DataRowKey;
import com.nearinfinity.honeycomb.hbase.rowkey.IndexRowKeyBuilder;
import com.nearinfinity.honeycomb.hbase.rowkey.SortOrder;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MutationFactoryTest {
    private static final String TABLE = "t1";
    private static final String COLUMN1 = "c1";
    private static final String COLUMN2 = "c2";
    private static final String INDEX1 = "i1";
    private static final String INDEX2 = "i2";
    private static final List<ColumnSchema> COLUMNS = new ArrayList<ColumnSchema>() {{
        add(ColumnSchema.builder(COLUMN1, ColumnType.LONG).build());
        add(ColumnSchema.builder(COLUMN2, ColumnType.STRING).setMaxLength(32).build());
    }};
    private static final List<IndexSchema> INDICES = new ArrayList<IndexSchema>() {{
        add(new IndexSchema(INDEX1, Lists.newArrayList(COLUMN1), false));
        add(new IndexSchema(INDEX2, Lists.newArrayList(COLUMN1, COLUMN2), true));
    }};
    private static final Row row = new Row(
            new HashMap<String, ByteBuffer>() {{
                put(COLUMN1, ByteBuffer.wrap(Longs.toByteArray(123)));
                put(COLUMN2, ByteBuffer.wrap("foobar".getBytes()));
            }},
            UUID.randomUUID()
    );
    private static final byte DATA_PREFIX = new DataRowKey(0, null).getPrefix();
    private static final byte ASC_PREFIX = IndexRowKeyBuilder.newBuilder(0, 0)
            .withSortOrder(SortOrder.Ascending).build().getPrefix();
    private static final byte DESC_PREFIX = IndexRowKeyBuilder.newBuilder(0, 0)
            .withSortOrder(SortOrder.Descending).build().getPrefix();
    private MutationFactory factory;
    private long tableId;

    @Before
    public void testSetup() {
        HBaseTableFactory tableFactory = mock(HBaseTableFactory.class);
        HTableProvider provider = mock(HTableProvider.class);

        MockitoAnnotations.initMocks(this);
        MockHTable table = MockHTable.create();
        when(provider.get()).thenReturn(table);

        HBaseMetadata metadata = new HBaseMetadata(provider);
        metadata.setColumnFamily("nic");
        MetadataCache cache = new MetadataCache(metadata);

        HBaseStore store = new HBaseStore(metadata, tableFactory, cache);
        factory = new MutationFactory(store);
        factory.setColumnFamily("nic");

        TableSchema schema = new TableSchema(COLUMNS, INDICES);

        store.createTable(TABLE, schema);
        tableId = store.getTableId(TABLE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInsertRejectsInvalidTableId() throws Exception {
        factory.insert(-1, row);
    }

    @Test(expected = TableNotFoundException.class)
    public void testInsertRejectsUnknownTableId() throws Exception {
        factory.insert(99, row);
    }

    @Test(expected = NullPointerException.class)
    public void testInsertRejectsNullRow() throws Exception {
        factory.insert(1, null);
    }

    @Test
    public void testInsert() throws Exception {
        List<Put> puts = factory.insert(tableId, row);
        byte[] rowCounts = countRowTypes(puts);

        assertEquals("data row count", 1, rowCounts[DATA_PREFIX]);
        assertEquals("ascending index count", 2, rowCounts[ASC_PREFIX]);
        assertEquals("descending index count", 2, rowCounts[DESC_PREFIX]);
        assertEquals("row count", 5, puts.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartialInsertRejectsInvalidTableId() throws Exception {
        factory.insert(-1, row, new ArrayList<IndexSchema>());
    }

    @Test(expected = TableNotFoundException.class)
    public void testPartialInsertRejectsUnknownTableId() throws Exception {
        factory.insert(99, row, ImmutableList.of(INDICES.get(0)));
    }

    @Test(expected = NullPointerException.class)
    public void testPartialInsertRejectsNullRow() throws Exception {
        factory.insert(1, null, new ArrayList<IndexSchema>());
    }

    @Test
    public void testPartialInsert() throws Exception {
        List<Put> puts = factory.insert(tableId, row,
                ImmutableList.of(INDICES.get(0)));
        byte[] rowCounts = countRowTypes(puts);

        assertEquals("data row count", 1, rowCounts[DATA_PREFIX]);
        assertEquals("ascending index count", 1, rowCounts[ASC_PREFIX]);
        assertEquals("descending index count", 1, rowCounts[DESC_PREFIX]);
        assertEquals("row count", 3, puts.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeleteIndicesRejectsInvalidTableId() throws Exception {
        factory.deleteIndices(-1, row);
    }

    @Test(expected = TableNotFoundException.class)
    public void testDeleteIndicesRejectsUnknownTableId() throws Exception {
        factory.deleteIndices(99, row);
    }

    @Test(expected = NullPointerException.class)
    public void testDeleteIndicesRejectsNullRow() throws Exception {
        factory.deleteIndices(1, null);
    }

    @Test
    public void testDeleteIndices() throws Exception {
        List<Delete> deletes = factory.deleteIndices(tableId, row);
        byte[] rowCounts = countRowTypes(deletes);

        assertEquals("ascending index count", 2, rowCounts[ASC_PREFIX]);
        assertEquals("descending index count", 2, rowCounts[DESC_PREFIX]);
        assertEquals("row count", 4, deletes.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPartialDeleteIndicesRejectsInvalidTableId() throws Exception {
        factory.deleteIndices(-1, row, new ArrayList<IndexSchema>());
    }

    @Test(expected = TableNotFoundException.class)
    public void testPartialDeleteIndicesRejectsUnknownTableId() throws Exception {
        factory.deleteIndices(99, row, ImmutableList.of(INDICES.get(0)));
    }

    @Test(expected = NullPointerException.class)
    public void testPartialDeleteIndicesRejectsNullRow() throws Exception {
        factory.deleteIndices(1, null, new ArrayList<IndexSchema>());
    }

    @Test
    public void testPartialDeleteIndices() throws Exception {
        List<Delete> deletes = factory.deleteIndices(tableId, row,
                ImmutableList.of(INDICES.get(0)));
        byte[] rowCounts = countRowTypes(deletes);

        assertEquals("ascending index count", 1, rowCounts[ASC_PREFIX]);
        assertEquals("descending index count", 1, rowCounts[DESC_PREFIX]);
        assertEquals("row count", 2, deletes.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeleteRejectsInvalidTableId() throws Exception {
        factory.delete(-1, row);
    }

    @Test(expected = TableNotFoundException.class)
    public void testDeleteRejectsUnknownTableId() throws Exception {
        factory.delete(99, row);
    }

    @Test(expected = NullPointerException.class)
    public void testDeleteRejectsNullRow() throws Exception {
        factory.delete(1, null);
    }

    @Test
    public void testDelete() throws Exception {
        List<Delete> deletes = factory.delete(tableId, row);
        byte[] rowCounts = countRowTypes(deletes);

        assertEquals("data row count", 1, rowCounts[DATA_PREFIX]);
        assertEquals("ascending index count", 2, rowCounts[ASC_PREFIX]);
        assertEquals("descending index count", 2, rowCounts[DESC_PREFIX]);
        assertEquals("row count", 5, deletes.size());
    }

    private byte[] countRowTypes(List<? extends Mutation> mutations) {
        int numRowTypes = 9;
        byte[] rowCounts = new byte[numRowTypes];

        for (Mutation mutation : mutations) {
            rowCounts[mutation.getRow()[0]]++;
        }
        return rowCounts;
    }
}