package com.nearinfinity.honeycomb;

public class ColumnNotFoundException extends HoneycombException {
    String column;

    public ColumnNotFoundException(String column) {
        this.column = column;
    }
}
