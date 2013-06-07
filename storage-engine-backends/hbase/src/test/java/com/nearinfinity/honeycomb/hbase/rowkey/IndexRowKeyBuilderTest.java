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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.nearinfinity.honeycomb.mysql.QueryKey;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.gen.QueryType;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class IndexRowKeyBuilderTest {

    private static final int ASC_INDEX_PREFIX = 0x07;

    private static final long TABLE_ID = 1;
    private static final long INDEX_ID = 5;

    private IndexRowKeyBuilder builder;

    @Before
    public void setupTestCases() {
        builder = IndexRowKeyBuilder.newBuilder(TABLE_ID, INDEX_ID);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewBuilderInvalidTableId() {
        final long invalidTableId = -1;
        IndexRowKeyBuilder.newBuilder(invalidTableId, INDEX_ID);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNewBuilderInvalidIndexId() {
        final long invalidIndexId = -1;
        IndexRowKeyBuilder.newBuilder(TABLE_ID, invalidIndexId);
    }

    @Test
    public void testNewBuilder() {
        assertNotNull(IndexRowKeyBuilder.newBuilder(TABLE_ID, INDEX_ID));
    }


    @Test(expected = NullPointerException.class)
    public void testBuilderNullSortOrder() {
        builder.withSortOrder(null);
    }

    @Test
    public void testBuilderSortOrder() {
        assertNotNull(builder.withSortOrder(SortOrder.Descending));
    }

    @Test(expected = NullPointerException.class)
    public void testBuilderNullUUID() {
        builder.withUUID(null);
    }

    @Test
    public void testBuilderUUID() {
        assertNotNull(builder.withUUID(UUID.randomUUID()));
    }

    @Test(expected = NullPointerException.class)
    public void testBuilderQueryKeyNullQueryKey() {
        builder.withQueryKey(null, getSchema());
    }

    @Test(expected = NullPointerException.class)
    public void testBuilderQueryKeyNullTableSchema() {
        builder.withQueryKey(getQueryKey(), null);
    }

    @Test
    public void testBuilderRecords() {
        builder.withQueryKey(getQueryKey(), getSchema());
    }

    @Test
    public void testBuildAscendingIndex() {
        final IndexRowKey row = builder.withSortOrder(SortOrder.Ascending).build();

        assertEquals(ASC_INDEX_PREFIX, row.getPrefix());
    }

    @Test(expected = IllegalStateException.class)
    public void testBuildWithoutSortOrderFails(){
        builder.build();
    }

    private TableSchema getSchema() {
        return new TableSchema(
                ImmutableList.<ColumnSchema>of(
                        ColumnSchema.builder("c1", ColumnType.BINARY)
                                .setMaxLength(16)
                                .build()),
                ImmutableList.<IndexSchema>of());
    }

    private QueryKey getQueryKey() {
        return new QueryKey("i1", QueryType.AFTER_KEY, ImmutableMap.<String, ByteBuffer>of());
    }
 }
