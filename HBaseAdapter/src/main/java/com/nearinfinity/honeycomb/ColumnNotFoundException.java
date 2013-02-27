package com.nearinfinity.honeycomb;

public class ColumnNotFoundException extends Exception {
    String column;

    public ColumnNotFoundException(String column) {
        this.column = column;
    };
}
