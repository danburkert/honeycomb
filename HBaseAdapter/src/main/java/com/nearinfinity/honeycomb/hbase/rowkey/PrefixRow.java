package com.nearinfinity.honeycomb.hbase.rowkey;

/**
 * Super class for rowkeys that only occur once, that is,
 * rowkeys that are shared across all tables.
 */
public abstract class PrefixRow implements RowKey {
    private final byte[] rowKey;

    public PrefixRow(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    @Override
    public byte[] encode() {
        return rowKey;
    }

    @Override
    public byte getPrefix() {
        return rowKey[0];
    }

    @Override
    public String toString() {
        return '[' + String.format("%02X", getPrefix()) + ']';
    }

    @Override
    public int compareTo(RowKey o) {
        return getPrefix() - o.getPrefix();
    }
}