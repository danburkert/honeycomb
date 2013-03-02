package com.nearinfinity.honeycomb.hbase.rowkey;

import com.nearinfinity.honeycomb.hbase.RowKey;

public class TablesRow implements RowKey {
    private static final byte PREFIX = 0x00;

    private final byte[] rowKey = {PREFIX};

    public TablesRow() {
    }

    public byte[] encode() {
        return rowKey;
    }

    public byte getPrefix() {
        return PREFIX;
    }

    @Override
    public String toString() {
        return "[" + String.format("%02X", PREFIX) + "]";
    }
}