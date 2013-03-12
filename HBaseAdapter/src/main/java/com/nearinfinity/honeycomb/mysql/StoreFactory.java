package com.nearinfinity.honeycomb.mysql;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.nearinfinity.honeycomb.Store;

import java.util.Map;

public class StoreFactory {
    Map<String, Provider<Store>> storeMap;

    @Inject
    public StoreFactory(Map<String, Provider<Store>> storeMap) {
        this.storeMap = storeMap;
    }

    public Store createStore(String database) {
        Provider<Store> storeProvider = this.storeMap.get(database);
        return storeProvider.get();
    }
}
