package com.nearinfinity.honeycomb.hbase.rowkey;

/**
 * Representation of the rowkey associated with the table schema details for the tables being stored
 */
public class SchemaRow extends PrefixRow {
    private static final byte[] ROWKEY = {0x05};

    public SchemaRow() {
        super(ROWKEY);
    }
}