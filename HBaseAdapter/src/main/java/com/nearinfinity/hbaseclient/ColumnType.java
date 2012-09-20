package com.nearinfinity.hbaseclient;

import java.util.Arrays;

public enum ColumnType {
    NONE("None".getBytes()),
    STRING("String".getBytes()),
    BINARY("Binary".getBytes()),
    ULONG("ULong".getBytes()),
    LONG("Long".getBytes()),
    DOUBLE("Double".getBytes()),
    TIME("Time".getBytes()),
    DATE("Date".getBytes()),
    DATETIME("DateTime".getBytes()),
    DECIMAL("Decimal".getBytes());

    private byte[] value;

    ColumnType(byte[] value) {
        this.value = value;
    }

    public byte[] getValue() {
        return this.value;
    }

    public static ColumnType getByValue(byte[] bytes) {
        for (ColumnType columnType : ColumnType.values()) {
            if (Arrays.equals(bytes, columnType.getValue())) {
                return columnType;
            }
        }

        return ColumnType.NONE;
    }
}
