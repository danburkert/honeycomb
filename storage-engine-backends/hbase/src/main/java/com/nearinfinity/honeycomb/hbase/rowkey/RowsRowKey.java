package com.nearinfinity.honeycomb.hbase.rowkey;

/**
 * Representation of the rowkey associated with the row count details for the tables being stored
 */
public class RowsRowKey extends PrefixRowKey {
    private static final byte[] ROWKEY = {0x03};

    public RowsRowKey() {
        super(ROWKEY);
    }
}