package com.nearinfinity.honeycomb.mysql;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.config.AdaptorType;
import com.nearinfinity.honeycomb.config.HoneycombConfiguration;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class StoreFactory {
    private final Map<AdaptorType, Provider<Store>> storeProviders;
    private final HoneycombConfiguration configuration;

    @Inject
    public StoreFactory(Map<AdaptorType, Provider<Store>> storeMap, HoneycombConfiguration configuration) {
        checkNotNull(storeMap);
        checkNotNull(configuration);

        this.storeProviders = storeMap;
        this.configuration = configuration;
    }

    public Store createStore(String tableName) {
        if (databaseName(tableName).equals(AdaptorType.MEMORY.getName())) {
            return storeProviders.get(AdaptorType.MEMORY).get();
        } else {
            return storeProviders.get(AdaptorType.HBASE).get();
        }
    }

    private static String databaseName(String tableName) {
        return tableName.substring(0, tableName.indexOf("/"));
    }
}
