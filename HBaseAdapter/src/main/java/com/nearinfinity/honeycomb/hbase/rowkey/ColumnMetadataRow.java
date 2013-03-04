package com.nearinfinity.honeycomb.hbase.rowkey;

public class ColumnMetadataRow extends MetadataRow {
    private static final byte PREFIX = 0x02;

    public ColumnMetadataRow(long tableId) {
        super(tableId, PREFIX);
    }
}