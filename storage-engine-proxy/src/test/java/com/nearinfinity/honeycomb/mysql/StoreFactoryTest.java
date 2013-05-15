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

import com.google.common.collect.Maps;
import com.google.inject.Provider;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.config.AdapterType;
import com.nearinfinity.honeycomb.config.HoneycombConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.powermock.api.mockito.PowerMockito.when;

public class StoreFactoryTest {
    @Mock
    Provider<Store> storeProvider;
    Store store;

    String tableName = "foo/bar";

    private static Map<String, String> hbaseConfigs = new HashMap<String, String>() {{
        put("option1", "value1");
        put("option2", "value2");
    }};

    private static Map<String, Map<String, String>> adapterConfigs = new HashMap<String, Map<String, String>>() {{
        put(AdapterType.HBASE.getName(), hbaseConfigs);
    }};

    @Before
    public void testSetup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testDefaultTablespaceIsUsed() {
        StoreFactory factory = createFactory();
        Store returnedStore = factory.createStore(tableName);
        assertEquals(returnedStore, this.store);
    }

    private StoreFactory createFactory() {
        HoneycombConfiguration configurationHolder = new HoneycombConfiguration(adapterConfigs, "hbase");
        Map<AdapterType, Provider<Store>> map = Maps.newHashMap();
        map.put(AdapterType.HBASE, storeProvider);
        when(storeProvider.get()).thenReturn(store);
        return new StoreFactory(map, configurationHolder);
    }
}
