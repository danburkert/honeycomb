package com.nearinfinity.honeycomb.hbase.rowkey;

/**
 * Implementing this interface allows an entity to represent a rowkey.  The rowkey
 * must always have an associated prefix for identification.
 */
public interface RowKey extends Comparable<RowKey> {
    /**
     * Performs the necessary encoding operations to generate the byte representation for this rowkey
     *
     * @return A byte array representing the encoded rowkey
     */
    public byte[] encode();

    /**
     * Retrieves the prefix associated with this rowkey for unique type identification
     *
     * @return The unique byte used to identify the rowkey
     */
    public byte getPrefix();
}
