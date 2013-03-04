package com.nearinfinity.honeycomb.hbase.rowkey;

public class ColumnsRow extends MetadataRow {
    private static final byte PREFIX = 0x01;

    public ColumnsRow(long tableId) {
        super(tableId, PREFIX);
    }
}