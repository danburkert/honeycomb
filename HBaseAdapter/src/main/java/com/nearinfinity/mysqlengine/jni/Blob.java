package com.nearinfinity.mysqlengine.jni;


import java.nio.ByteBuffer;

public class Blob {
    private final ByteBuffer data;
    private final String columnName;

    public Blob(ByteBuffer data, String columnName) {
        this.data = data;
        this.columnName = columnName;
    }

    public ByteBuffer getData() {
        return data;
    }

    public String getColumnName() {
        return columnName;
    }
}
