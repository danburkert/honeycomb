package com.nearinfinity.hbaseclient;

public class ColumnInfo {
    private long id;
    private String name;
    private ColumnMetadata metadata;
    public ColumnInfo(long id, String name, ColumnMetadata metadata) {
        this.id = id;
        this.name = name;
        this.metadata = metadata;
    }

    public long getId() {
        return this.id;
    }

    public String getName() {
        return this.name;
    }

    public ColumnMetadata getMetadata() {
        return this.metadata;
    }
}
