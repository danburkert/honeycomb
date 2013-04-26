package com.nearinfinity.honeycomb.config;

public enum AdaptorType {
    HBASE ("hbase"),
    MEMORY ("memory");

    private String name;

    AdaptorType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
