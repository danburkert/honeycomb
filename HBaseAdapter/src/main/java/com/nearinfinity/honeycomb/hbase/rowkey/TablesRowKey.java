package com.nearinfinity.honeycomb.hbase.rowkey;

/**
 * Representation of the rowkey associated with the metadata details for the tables being stored
 */
public class TablesRowKey extends PrefixRowKey {
    private static final byte[] ROWKEY = {0x00};

    public TablesRowKey() {
        super(ROWKEY);
    }
}