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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.config.AdapterType;
import com.nearinfinity.honeycomb.config.HoneycombConfiguration;

/**
 * Factory class used to construct {@link Store} instances
 */
public class StoreFactory {
    private final Map<AdapterType, Provider<Store>> storeProviders;
    private final HoneycombConfiguration configuration;

    @Inject
    public StoreFactory(Map<AdapterType, Provider<Store>> storeMap,
                        HoneycombConfiguration configuration) {
        checkNotNull(storeMap);
        checkNotNull(configuration);

        storeProviders = storeMap;
        this.configuration = configuration;
    }

    /**
     * Returns a store implementation for a given table name.  Returns a store type
     * for the adapter matching the database name, or if that does not exist,
     * the default adapter.
     * @param tableName
     * @return The store for the specified table name
     */
    public Store createStore(String tableName) {
        try {
            return storeProviders.get(AdapterType.valueOf(databaseName(tableName).toUpperCase())).get();
        } catch (IllegalArgumentException e) {
            return storeProviders.get(configuration.getDefaultAdapter()).get();
        }
    }

    private static String databaseName(String tableName) {
        return tableName.substring(0, tableName.indexOf("/"));
    }
}
