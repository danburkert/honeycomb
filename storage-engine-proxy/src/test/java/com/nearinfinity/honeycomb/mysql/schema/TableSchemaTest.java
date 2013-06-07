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


package com.nearinfinity.honeycomb.mysql.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class TableSchemaTest {

    private static final String COLUMN_A = "colA";
    private static final String COLUMN_B = "colB";

    private static final String UNKNOWN_INDEX = "foo";
    private static final String INDEX_A = "INDEX_A";
    private static final String INDEX_B = "INDEX_B";

    private static final List<ColumnSchema> COLUMNS = ImmutableList.of(
            ColumnSchema.builder(COLUMN_A, ColumnType.LONG).setIsAutoIncrement(true).build(),
            ColumnSchema.builder(COLUMN_B, ColumnType.LONG).build());

    private static final List<IndexSchema> INDICES = ImmutableList.of(
            new IndexSchema(INDEX_A, ImmutableList.<String>of(COLUMN_A), false),
            new IndexSchema(INDEX_B, ImmutableList.<String>of(COLUMN_B), true));

    private TableSchema tableSchema;

    private static final ColumnSchema FAKE_COL_SCHEMA = ColumnSchema.builder("fakeColumn", ColumnType.ULONG).build();

    @Before
    public void setupTestCase() {
        tableSchema = new TableSchema(COLUMNS, INDICES);
    }


    @Test(expected = IllegalArgumentException.class)
    public void testTableSchemaCreationInvalidNumberColumns() {
        new TableSchema(ImmutableList.<ColumnSchema>of(), ImmutableList.<IndexSchema>of());
    }

    @Test(expected = NullPointerException.class)
    public void testTableSchemaCreationInvalidIndices() {
        new TableSchema(ImmutableList.<ColumnSchema>of(FAKE_COL_SCHEMA), null);
    }

    @Test(expected = NullPointerException.class)
    public void testDeserializeNullSerializedSchema() {
        TableSchema.deserialize(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeserializeEmptySerializedSchema() {
        TableSchema.deserialize(new byte[0]);
    }

    @Test
    public void testDeserializeValidSerializedSchema() {
        final TableSchema actualSchema = TableSchema.deserialize(tableSchema.serialize());

        assertEquals(COLUMNS.size(), tableSchema.getColumns().size());
        assertEquals(INDICES.size(), tableSchema.getIndices().size());
        assertEquals(tableSchema, actualSchema);
    }

    @Test(expected = NullPointerException.class)
    public void testAddIndicesInvalidIndices() {
        tableSchema.addIndices(null);
    }

    @Test
    public void testAddIndicesValidIndex() {
        final List<IndexSchema> indices = Lists.newArrayList();
        final TableSchema schema = new TableSchema(COLUMNS, indices);

        assertEquals(0, schema.getIndices().size());
        schema.addIndices(INDICES);
        assertEquals(INDICES.size(), schema.getIndices().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRemoveIndexEmptyIndexName() {
        tableSchema.removeIndex("");
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveIndexInvalidIndexName() {
        tableSchema.removeIndex(null);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveIndexUnknownIndexName() {
        tableSchema.removeIndex(UNKNOWN_INDEX);
    }

    @Test
    public void testRemoveIndexValidIndexName() {
        final TableSchema schema = new TableSchema(COLUMNS, INDICES);
        assertEquals(INDICES.size(), schema.getIndices().size());

        // Remove all of the indexes from the schema
        for(final IndexSchema indexSchema : INDICES ) {
            schema.removeIndex(indexSchema.getIndexName());
        }

        assertEquals(0, schema.getIndices().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetColumnSchemaEmptyColumnName() {
        tableSchema.getColumnSchema("");
    }

    @Test(expected = NullPointerException.class)
    public void testGetColumnSchemaInvalidColumnName() {
        tableSchema.getColumnSchema(null);
    }

    @Test
    public void testGetColumnSchemaValidColumnName() {
        final TableSchema schema = new TableSchema(ImmutableList.<ColumnSchema>of(FAKE_COL_SCHEMA), INDICES);
        final ColumnSchema actual = schema.getColumnSchema(FAKE_COL_SCHEMA.getColumnName());

        assertEquals(FAKE_COL_SCHEMA.getColumnName(), actual.getColumnName());
        assertEquals(FAKE_COL_SCHEMA, actual);
    }

    @Test
    public void testGetAutoIncColumnNoAutoIncColumns() {
        final TableSchema schema = new TableSchema(ImmutableList.<ColumnSchema>of(FAKE_COL_SCHEMA), INDICES);
        assertEquals(null, schema.getAutoIncrementColumn());
    }

    @Test
    public void testGetAutoIncColumns() {
        assertEquals(COLUMN_A, tableSchema.getAutoIncrementColumn());
    }

    @Test
    public void testHasNoIndices() {
        final TableSchema schema = new TableSchema(COLUMNS, ImmutableList.<IndexSchema>of());
        assertFalse(schema.hasIndices());
    }

    @Test
    public void testHasIndices() {
        assertTrue(tableSchema.hasIndices());
    }


    @Test
    public void testHasNoUniqueIndices() {
        final TableSchema schema = new TableSchema(COLUMNS, ImmutableList.<IndexSchema>of());
        assertFalse(schema.hasUniqueIndices());
    }

    @Test
    public void testHasUniqueIndices() {
        assertTrue(tableSchema.hasUniqueIndices());
    }

    @Test
    public void equalsContract() {
        EqualsVerifier.forClass(TableSchema.class).verify();
    }
}
