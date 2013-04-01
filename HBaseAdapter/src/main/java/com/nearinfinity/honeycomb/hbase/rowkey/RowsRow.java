package com.nearinfinity.honeycomb.hbase.rowkey;

/**
 * Representation of the rowkey associated with the row count details for the tables being stored
 */
public class RowsRow extends PrefixRow {
    private static final byte[] ROWKEY = {0x03};

    public RowsRow() {
        super(ROWKEY);
    }
}