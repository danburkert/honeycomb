package com.nearinfinity.honeycomb.hbase.rowkey;

public class ColumnsRow extends TableIDRow {
    private static final byte PREFIX = 0x01;

    public ColumnsRow(long tableId) {
        super(PREFIX, tableId);
    }
}
