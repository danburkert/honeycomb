package com.nearinfinity.honeycomb.hbase.rowkey;

public class IndicesRow extends TableIDRow {
    private static final byte PREFIX = 0x02;

    public IndicesRow(long tableId) {
        super(PREFIX, tableId);
    }
}
