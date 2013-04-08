package com.nearinfinity.honeycomb.hbase.rowkey;

/**
 * Representation of the rowkey associated with the table schema details for the tables being stored
 */
public class SchemaRowKey extends PrefixRowKey {
    private static final byte[] ROWKEY = {0x05};

    public SchemaRowKey() {
        super(ROWKEY);
    }
}