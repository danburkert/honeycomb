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


package com.nearinfinity.honeycomb.mysql;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.ImmutableList;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;

/**
 * Provides test cases for the {@link HandlerProxy} class.
 */
@RunWith(PowerMockRunner.class)
public class HandlerProxyTest {

    private static final String TEST_INDEX = "testIdx";

    private static final String TEST_COLUMN = "testCol";

    private static final String TEST_TABLE_NAME = "foo/bar";

    private static final List<ColumnSchema> COLUMNS = ImmutableList.of(
            ColumnSchema.builder(TEST_COLUMN, ColumnType.LONG).setIsAutoIncrement(true).build());

    private static final IndexSchema INDEX_SCHEMA = new IndexSchema(TEST_INDEX, ImmutableList.<String>of(TEST_COLUMN), false);
    private static final TableSchema TABLE_SCHEMA = new TableSchema(COLUMNS, ImmutableList.<IndexSchema>of(INDEX_SCHEMA));

    @Mock
    private Store storageMock;

    @Mock
    private StoreFactory storeFactory;

    @Mock
    private Table tableMock;

    private HandlerProxy proxy;

    @Before
    public void setupTests() {
        MockitoAnnotations.initMocks(this);

        proxy = new HandlerProxy(storeFactory);
    }

    @Test
    public void testRenameTable() throws Exception {
        when(storeFactory.createStore(TEST_TABLE_NAME)).thenReturn(storageMock);

        final String renamedTableName = "bar/baz";

        proxy.renameTable(TEST_TABLE_NAME, renamedTableName);

        verify(storageMock, times(1)).renameTable(eq(TEST_TABLE_NAME), eq(renamedTableName));
    }

    @Test(expected = NullPointerException.class)
    public void testRenameTableNullNewTableName() throws Exception {
        proxy.renameTable("a", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRenameTableEmptyNewTableName() throws Exception {
        proxy.renameTable("a", "");
    }

    @Test(expected = NullPointerException.class)
    public void testRenameTableNullOriginalTableName() throws Exception {
        proxy.renameTable(null, "c");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRenameTableEmptyOriginalTableName() throws Exception {
        proxy.renameTable("", "c");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRenameTableToSameName() throws Exception {
        proxy.renameTable("a", "a");
    }

    @Test(expected = NullPointerException.class)
    public void testCreateTableInvalidTableName() {

        proxy.createTable(null, TABLE_SCHEMA.serialize(), 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateTableEmptyTableName() {
        proxy.createTable("", TABLE_SCHEMA.serialize(), 0);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateTableInvalidTableSchema() {
        proxy.createTable(TEST_TABLE_NAME, null, 0);
    }

    @Test
    public void testCreateTable() {
        when(storeFactory.createStore(anyString())).thenReturn(storageMock);
        final long incrementValue = 42;
        proxy.createTable(TEST_TABLE_NAME, TABLE_SCHEMA.serialize(), incrementValue);

        verify(storeFactory, times(1)).createStore(eq(TEST_TABLE_NAME));
        verify(storageMock, times(1)).createTable(eq(TEST_TABLE_NAME), eq(TABLE_SCHEMA));
        verify(storageMock, times(1)).incrementAutoInc(eq(TEST_TABLE_NAME), eq(incrementValue));
    }

    @Test(expected = NullPointerException.class)
    public void testDropTableInvalidTableName() {
        proxy.dropTable(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDropTableEmptyTableName() {
        proxy.dropTable("");
    }

    @Test
    public void testDropTable() {
        when(storeFactory.createStore(anyString())).thenReturn(storageMock);
        when(storageMock.openTable(anyString())).thenReturn(tableMock);

        proxy.dropTable(TEST_TABLE_NAME);

        verify(storeFactory, times(1)).createStore(eq(TEST_TABLE_NAME));
        verify(storageMock, times(1)).openTable(eq(TEST_TABLE_NAME));
        verify(tableMock, times(1)).deleteAllRows();
        verify(storageMock, times(1)).deleteTable(eq(TEST_TABLE_NAME));
    }

    @Test(expected = NullPointerException.class)
    public void testOpenTableInvalidTableName() {
        proxy.openTable(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOpenTableEmptyTableName() {
        proxy.openTable("");
    }

    @Test
    public void testOpenTable() {
        when(storeFactory.createStore(anyString())).thenReturn(storageMock);
        when(storageMock.openTable(anyString())).thenReturn(tableMock);

        proxy.openTable(TEST_TABLE_NAME);

        verify(storeFactory, times(1)).createStore(eq(TEST_TABLE_NAME));
        verify(storageMock, times(1)).openTable(eq(TEST_TABLE_NAME));
    }

    @Test(expected = NullPointerException.class)
    public void testAddIndexInvalidIndexName() {
        proxy.addIndex(null, INDEX_SCHEMA.serialize());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddIndexEmptyIndexName() {
        proxy.addIndex("", INDEX_SCHEMA.serialize());
    }

    @Test(expected = NullPointerException.class)
    public void testAddIndexInvalidIndexSchema() {
        proxy.addIndex(TEST_INDEX, null);
    }

    @Test
    public void testAddIndex() {
        when(storeFactory.createStore(anyString())).thenReturn(storageMock);
        when(storageMock.openTable(anyString())).thenReturn(tableMock);

        proxy.openTable(TEST_TABLE_NAME);

        verify(storeFactory, times(1)).createStore(eq(TEST_TABLE_NAME));
        verify(storageMock, times(1)).openTable(eq(TEST_TABLE_NAME));

        proxy.addIndex(TEST_INDEX, INDEX_SCHEMA.serialize());

        verify(storageMock, times(1)).addIndex(eq(TEST_TABLE_NAME), eq(INDEX_SCHEMA));
        verify(tableMock, times(1)).insertTableIndex(eq(INDEX_SCHEMA));
        verify(tableMock, times(1)).flush();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddUniqueIndex() {
        when(storeFactory.createStore(anyString())).thenReturn(storageMock);
        when(storageMock.openTable(anyString())).thenReturn(tableMock);

        proxy.openTable(TEST_TABLE_NAME);

        verify(storeFactory, times(1)).createStore(eq(TEST_TABLE_NAME));
        verify(storageMock, times(1)).openTable(eq(TEST_TABLE_NAME));

        final IndexSchema uniqueIndex = new IndexSchema("uniqueIdx", ImmutableList.<String>of(TEST_COLUMN), true);
        proxy.addIndex(TEST_INDEX, uniqueIndex.serialize());

        verify(storageMock, never()).addIndex(eq(TEST_TABLE_NAME), eq(INDEX_SCHEMA));
        verify(tableMock, never()).insertTableIndex(eq(INDEX_SCHEMA));
        verify(tableMock, never()).flush();
    }

    @Test(expected = IllegalStateException.class)
    public void testAddIndexTableNotOpen() {
        when(storeFactory.createStore(anyString())).thenReturn(storageMock);
        when(storageMock.openTable(anyString())).thenReturn(tableMock);

        proxy.addIndex(TEST_INDEX, INDEX_SCHEMA.serialize());

        verify(storageMock, never()).addIndex(eq(TEST_TABLE_NAME), eq(INDEX_SCHEMA));
        verify(tableMock, never()).insertTableIndex(eq(INDEX_SCHEMA));
        verify(tableMock, never()).flush();
    }

    @Test(expected = NullPointerException.class)
    public void testDropIndexInvalidIndexName() {
        proxy.dropIndex(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDropIndexEmptyIndexName() {
        proxy.dropIndex("");
    }

    @Test
    public void testDropIndex() {
        when(storeFactory.createStore(anyString())).thenReturn(storageMock);
        when(storageMock.openTable(anyString())).thenReturn(tableMock);

        proxy.openTable(TEST_TABLE_NAME);

        verify(storeFactory, times(1)).createStore(eq(TEST_TABLE_NAME));
        verify(storageMock, times(1)).openTable(eq(TEST_TABLE_NAME));

        when(storageMock.getSchema(TEST_TABLE_NAME)).thenReturn(TABLE_SCHEMA);

        proxy.dropIndex(TEST_INDEX);

        verify(storageMock, times(1)).getSchema(eq(TEST_TABLE_NAME));
        verify(tableMock, times(1)).deleteTableIndex(eq(INDEX_SCHEMA));
        verify(storageMock, times(1)).dropIndex(eq(TEST_TABLE_NAME), eq(TEST_INDEX));
    }

    @Test(expected = IllegalStateException.class)
    public void testDropIndexTableNotOpen() {
        when(storeFactory.createStore(anyString())).thenReturn(storageMock);
        when(storageMock.openTable(anyString())).thenReturn(tableMock);

        proxy.dropIndex(TEST_INDEX);

        verify(storageMock, never()).getSchema(eq(TEST_TABLE_NAME));
        verify(tableMock, never()).deleteTableIndex(eq(INDEX_SCHEMA));
        verify(storageMock, never()).dropIndex(eq(TEST_TABLE_NAME), eq(TEST_INDEX));
    }
}
