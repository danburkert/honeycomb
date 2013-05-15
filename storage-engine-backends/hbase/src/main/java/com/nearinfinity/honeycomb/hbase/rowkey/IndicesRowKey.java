package com.nearinfinity.honeycomb.hbase.rowkey;

public class IndicesRowKey extends TableIDRowKey {
    private static final byte PREFIX = 0x02;

    public IndicesRowKey(long tableId) {
        super(PREFIX, tableId);
    }
}
