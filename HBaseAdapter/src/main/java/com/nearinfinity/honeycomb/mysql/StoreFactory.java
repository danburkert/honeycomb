package com.nearinfinity.honeycomb.mysql;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.exceptions.StoreNotFoundException;
import com.nearinfinity.honeycomb.config.Constants;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class StoreFactory {
    private final String defaultTableSpace;
    private final Map<String, Provider<Store>> storeMap;

    @Inject
    public StoreFactory(Map<String, Provider<Store>> storeMap, @Named(Constants.DEFAULT_TABLESPACE) String defaultTableSpace) {
        checkNotNull(storeMap);
        Verify.isNotNullOrEmpty(defaultTableSpace);

        this.storeMap = storeMap;
        this.defaultTableSpace = defaultTableSpace;
    }

    public Store createStore(String tablespace) {
        if (tablespace == null) {
            tablespace = defaultTableSpace;
        }

        Provider<Store> storeProvider = this.storeMap.get(tablespace);
        if (storeProvider == null) {
            throw new StoreNotFoundException(tablespace);
        }
        return storeProvider.get();
    }
}
