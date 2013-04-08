package com.nearinfinity.honeycomb.hbase.rowkey;

/**
 * Representation of the rowkey associated with the auto-incremented count of tables being stored
 */
public class AutoIncRowKey extends PrefixRowKey {
    private static final byte[] ROWKEY = {0x04};

    public AutoIncRowKey() {
        super(ROWKEY);
    }
}