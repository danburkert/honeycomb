package com.nearinfinity.mysqlengine;

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
    DATA(2),
    VALUE_INDEX(3),
    SECONDARY_INDEX(4),
    REVERSE_INDEX(5),
    NULL_INDEX(6);

    private byte value;

    RowType(int value) {
        this.value = (byte)value;
    }

    public byte getValue() {
        return this.value;
    }
}
