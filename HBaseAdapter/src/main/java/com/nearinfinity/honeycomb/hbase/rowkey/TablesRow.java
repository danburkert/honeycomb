package com.nearinfinity.honeycomb.hbase.rowkey;

/**
 * Representation of the rowkey associated with the metadata details for the tables being stored
 */
public class TablesRow extends PrefixRow {
    private static final byte[] ROWKEY = {0x00};

    public TablesRow() {
        super(ROWKEY);
    }
}