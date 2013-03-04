package com.nearinfinity.honeycomb.hbase.rowkey;

public class TablesRow extends PrefixRow {
    private static final byte[] rowKey = {0x00};

    public TablesRow() {
        super(rowKey);
    }
}