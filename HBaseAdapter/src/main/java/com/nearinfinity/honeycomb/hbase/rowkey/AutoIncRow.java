package com.nearinfinity.honeycomb.hbase.rowkey;

public class AutoIncRow extends PrefixRow {
    private static final byte[] rowKey = {0x03};

    public AutoIncRow() {
        super(rowKey);
    }
}