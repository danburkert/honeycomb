package com.nearinfinity.honeycomb.config;

public enum StoreType {
    HBASE ("hbase"),
    MEMORY ("memory");

    private String name;

    StoreType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
