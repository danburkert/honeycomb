package com.nearinfinity.honeycomb.mysql;

import com.google.inject.Inject;

public class HandlerProxyFactory {
    private final StoreFactory storeFactory;

    @Inject
    public HandlerProxyFactory(StoreFactory storeFactory) {
        this.storeFactory = storeFactory;
    }

    public HandlerProxy createHandlerProxy() {
        return new HandlerProxy(storeFactory);
    }
}
