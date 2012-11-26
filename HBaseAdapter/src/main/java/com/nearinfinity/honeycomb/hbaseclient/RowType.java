package com.nearinfinity.honeycomb.hbaseclient;

public enum RowType {
    TABLES(0),
    COLUMNS(1),
    COLUMN_INFO(2),
    TABLE_INFO(3),
    DATA(4),
    PRIMARY_INDEX(5),
    REVERSE_INDEX(6);

    private byte value;

    RowType(int value) {
        this.value = (byte)value;
    }

    public byte getValue() {
        return this.value;
    }
}
