package com.nearinfinity.honeycomb.hbase.rowkey;

public class RowsRow extends PrefixRow {
    private static final byte[] rowKey = {0x03};

    public RowsRow() {
        super(rowKey);
    }
}