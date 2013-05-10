package com.nearinfinity.honeycomb.config;

public enum AdapterType {
    HBASE ("hbase"),
    MEMORY ("memory");

    private String name;

    AdapterType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
