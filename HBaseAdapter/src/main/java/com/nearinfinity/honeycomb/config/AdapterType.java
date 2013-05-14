package com.nearinfinity.honeycomb.config;

public enum AdapterType {
    HBASE ("hbase", "com.nearinfinity.honeycomb.hbase.HBaseModule"),
    MEMORY ("memory", "com.nearinfinity.honeycomb.memory.MemoryModule");

    private String name;
    private String moduleClass;

    AdapterType(String name, String moduleClass) {
        this.name = name;
        this.moduleClass = moduleClass;
    }

    public String getName() {
        return name;
    }

    public String getModuleClass() {
        return moduleClass;
    }
}
