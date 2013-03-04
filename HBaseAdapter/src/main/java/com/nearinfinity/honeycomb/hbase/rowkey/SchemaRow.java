package com.nearinfinity.honeycomb.hbase.rowkey;

public class SchemaRow extends PrefixRow {
    private static final byte[] rowKey = {0x04};

    public SchemaRow() {
        super(rowKey);
    }
}