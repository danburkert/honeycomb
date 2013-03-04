package com.nearinfinity.honeycomb.hbase.rowkey;

import com.nearinfinity.honeycomb.hbase.RowKey;

public class PrefixRow implements RowKey {
    private byte[] rowKey;

    public PrefixRow(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    public byte[] encode() {
        return rowKey;
    }

    public byte getPrefix() {
        return rowKey[0];
    }

    @Override
    public String toString() {
        return '[' + String.format("%02X", getPrefix()) + ']';
    }
}