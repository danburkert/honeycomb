package com.nearinfinity.honeycomb.hbase.rowkey;

/**
 * Rowkey for Columns row type
 */
public class ColumnsRowKey extends TableIDRowKey {
    private static final byte PREFIX = 0x01;

    public ColumnsRowKey(long tableId) {
        super(PREFIX, tableId);
    }
}
