package com.nearinfinity.honeycomb.hbase.rowkey;

public interface RowKey {
    public byte[] encode();

    public byte getPrefix();
}