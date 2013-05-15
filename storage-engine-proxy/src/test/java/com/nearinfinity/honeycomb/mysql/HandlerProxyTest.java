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

import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Provides test cases for the {@link HandlerProxy} class.
 */
@RunWith(PowerMockRunner.class)
public class HandlerProxyTest {

    private static final String DUMMY_TABLE_NAME = "foo/bar";
    @Mock
    private Store storageMock;
    @Mock
    private StoreFactory storeFactory;
    @Mock
    private Table tableMock;

    @Before
    public void setupTests() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testRenameTable() throws Exception {
        when(storeFactory.createStore(DUMMY_TABLE_NAME)).thenReturn(storageMock);
        when(storageMock.openTable(anyString())).thenReturn(tableMock);

        final String renamedTableName = "bar/baz";

        final HandlerProxy proxy = createProxy();
        proxy.renameTable(DUMMY_TABLE_NAME, renamedTableName);

        verify(storageMock, times(1)).renameTable(eq(DUMMY_TABLE_NAME), eq(renamedTableName));
    }

    @Test(expected = NullPointerException.class)
    public void testRenameTableNullNewTableName() throws Exception {
        createProxy().renameTable("a", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRenameTableEmptyNewTableName() throws Exception {
        createProxy().renameTable("a", "");
    }

    @Test(expected = NullPointerException.class)
    public void testRenameTableNullOriginalTableName() throws Exception {
        createProxy().renameTable(null, "c");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRenameTableEmptyOriginalTableName() throws Exception {
        createProxy().renameTable("", "c");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRenameTableToSameName() throws Exception {
        createProxy().renameTable("a", "a");
    }

    private HandlerProxy createProxy() throws Exception {
        return new HandlerProxy(storeFactory);
    }
}
