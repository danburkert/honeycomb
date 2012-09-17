package com.nearinfinity.hbaseclient;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 7/25/12
 * Time: 2:07 PM
 * To change this template use File | Settings | File Templates.
 */
public enum RowType {
    TABLES(0),
    COLUMNS(1),
    COLUMN_INFO(2),
    DATA(3),
    PRIMARY_INDEX(4),
    REVERSE_INDEX(5),
    NULL_INDEX(6),
    TABLE_INFO(7);

    private byte value;

    RowType(int value) {
        this.value = (byte)value;
    }

    public byte getValue() {
        return this.value;
    }
}
