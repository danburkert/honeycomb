package com.nearinfinity.honeycomb.hbase.rowkey;

public class RowsRow extends PrefixRow {
    private static final byte[] rowKey = {0x02};

    public RowsRow() {
        super(rowKey);
    }
}