package com.nearinfinity.honeycomb;

public class TableNotFoundException extends Exception {
    private String database;
    private String name;

    public TableNotFoundException(String database, String name) {
        this.database = database;
        this.name = name;
    };
}
