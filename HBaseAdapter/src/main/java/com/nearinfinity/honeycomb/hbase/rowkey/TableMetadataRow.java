package com.nearinfinity.honeycomb.hbase.rowkey;

public class TableMetadataRow extends MetadataRow {
    private static final byte PREFIX = 0x03;

    public TableMetadataRow(long tableId) {
        super(tableId, PREFIX);
    }
}