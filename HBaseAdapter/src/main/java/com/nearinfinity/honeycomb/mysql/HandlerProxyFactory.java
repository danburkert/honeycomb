package com.nearinfinity.honeycomb.mysql;

import com.google.inject.Inject;
import com.nearinfinity.honeycomb.Store;

import java.util.Map;

/**
 * Simple way to decouple the creation of a Store from the user of the store. (May be subject to change)
 */
public class HandlerProxyFactory {

    private final Map<String, Store> storeMap;

    @Inject
    public HandlerProxyFactory(Map<String, Store> storeMap) {
        this.storeMap = storeMap;
    }

    public HandlerProxy createHBaseProxy(String type) throws Exception {
        return new HandlerProxy(storeMap.get(type));
    }
}
