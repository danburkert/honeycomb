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


package com.nearinfinity.honeycomb.mysql;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
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

    private static final String DUMMY_TABLE_NAME = "foo/bar";

    private static final List<ColumnSchema> COLUMNS = ImmutableList.of(
            ColumnSchema.builder("testCol", ColumnType.LONG).setIsAutoIncrement(true).build());

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
        when(storeFactory.createStore(DUMMY_TABLE_NAME)).thenReturn(storageMock);

        final String renamedTableName = "bar/baz";

        proxy.renameTable(DUMMY_TABLE_NAME, renamedTableName);

        verify(storageMock, times(1)).renameTable(eq(DUMMY_TABLE_NAME), eq(renamedTableName));
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
        final TableSchema schema = new TableSchema(COLUMNS, ImmutableList.<IndexSchema>of());
        proxy.createTable(null, schema.serialize(), 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateTableEmptyTableName() {
        final TableSchema schema = new TableSchema(COLUMNS, ImmutableList.<IndexSchema>of());
        proxy.createTable("", schema.serialize(), 0);
    }

    @Test(expected = NullPointerException.class)
    public void testCreateTableInvalidTableSchema() {
        proxy.createTable(DUMMY_TABLE_NAME, null, 0);
    }

    @Test
    public void testCreateTable() {
        when(storeFactory.createStore(anyString())).thenReturn(storageMock);
        final TableSchema schema = new TableSchema(COLUMNS, ImmutableList.<IndexSchema>of());
        final long incrementValue = 42;
        proxy.createTable(DUMMY_TABLE_NAME, schema.serialize(), incrementValue);

        verify(storeFactory, times(1)).createStore(eq(DUMMY_TABLE_NAME));
        verify(storageMock, times(1)).createTable(eq(DUMMY_TABLE_NAME), eq(schema));
        verify(storageMock, times(1)).incrementAutoInc(eq(DUMMY_TABLE_NAME), eq(incrementValue));
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

        proxy.dropTable(DUMMY_TABLE_NAME);

        verify(storeFactory, times(1)).createStore(eq(DUMMY_TABLE_NAME));
        verify(storageMock, times(1)).openTable(eq(DUMMY_TABLE_NAME));
        verify(tableMock, times(1)).deleteAllRows();
        verify(storageMock, times(1)).deleteTable(eq(DUMMY_TABLE_NAME));
    }
}
