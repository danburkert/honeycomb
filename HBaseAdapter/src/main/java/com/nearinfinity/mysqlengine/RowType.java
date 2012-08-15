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
    INDEX(3),
    NULL_INDEX(6);

    private byte value;

    RowType(int value) {
        this.value = (byte)value;
    }

    public byte getValue() {
        return this.value;
    }
}
