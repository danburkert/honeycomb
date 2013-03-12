package com.nearinfinity.honeycomb.mysql;

import com.google.inject.Inject;
import com.nearinfinity.honeycomb.Store;

import java.util.Map;

public class StoreFactory {
    Map<String, Store> storeMap;

    @Inject
    public StoreFactory(Map<String, Store> storeMap) {
        this.storeMap = storeMap;
    }

    public Store createStore(String database) {
        return this.storeMap.get(database);
    }
}
