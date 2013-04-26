package com.nearinfinity.honeycomb.mysql;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.config.HoneycombConfiguration;
import com.nearinfinity.honeycomb.config.StoreType;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class StoreFactory {
    private final Map<StoreType, Provider<Store>> storeProviders;
    private final HoneycombConfiguration configuration;

    @Inject
    public StoreFactory(Map<StoreType, Provider<Store>> storeMap, HoneycombConfiguration configuration) {
        checkNotNull(storeMap);
        checkNotNull(configuration);

        this.storeProviders = storeMap;
        this.configuration = configuration;
    }

    public Store createStore(String tableName) {
        if (databaseName(tableName).equals(StoreType.MEMORY.getName())) {
            return storeProviders.get(StoreType.MEMORY).get();
        } else {
            return storeProviders.get(StoreType.HBASE).get();
        }
    }

    private static String databaseName(String tableName) {
        return tableName.substring(0, tableName.indexOf("/"));
    }
}
