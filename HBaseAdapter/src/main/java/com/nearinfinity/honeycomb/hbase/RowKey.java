package com.nearinfinity.honeycomb.hbase;

public interface RowKey {
    public byte[] encode();

    public byte getPrefix();
}