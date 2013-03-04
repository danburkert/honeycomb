package com.nearinfinity.honeycomb;

public class TableNotFoundException extends Exception {
    private String name;

    public TableNotFoundException(String name) {
        this.name = name;
    }
}
