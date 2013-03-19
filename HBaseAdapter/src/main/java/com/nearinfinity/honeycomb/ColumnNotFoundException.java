package com.nearinfinity.honeycomb;

public class ColumnNotFoundException extends RuntimeException {
    String column;

    public ColumnNotFoundException(String column) {
        this.column = column;
    }
}
