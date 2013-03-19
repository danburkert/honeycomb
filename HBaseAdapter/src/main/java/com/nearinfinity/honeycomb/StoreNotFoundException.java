package com.nearinfinity.honeycomb;

public class StoreNotFoundException extends RuntimeException {
    private final String database;

    public StoreNotFoundException(String database) {
        this.database = database;
    }

    @Override
    public String getMessage() {
        return "Could not find store for " + database;
    }
}
