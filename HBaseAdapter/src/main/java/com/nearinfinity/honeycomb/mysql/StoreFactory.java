package com.nearinfinity.honeycomb.mysql;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.StoreNotFoundException;

import java.util.Map;

public class StoreFactory {
    Map<String, Provider<Store>> storeMap;

    @Inject
    public StoreFactory(Map<String, Provider<Store>> storeMap) {
        this.storeMap = storeMap;
    }

    public Store createStore(String database) throws StoreNotFoundException {
        Provider<Store> storeProvider = this.storeMap.get(database);
        if (storeProvider == null) {
            throw new StoreNotFoundException(database);
        }
        return storeProvider.get();
    }
}
