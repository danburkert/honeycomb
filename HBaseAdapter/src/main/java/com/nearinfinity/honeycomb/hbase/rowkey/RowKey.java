package com.nearinfinity.honeycomb.hbase.rowkey;

public interface RowKey extends Comparable<RowKey> {
    public byte[] encode();

    public byte getPrefix();
}