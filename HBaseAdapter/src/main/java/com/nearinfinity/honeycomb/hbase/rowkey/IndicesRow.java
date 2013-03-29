package com.nearinfinity.honeycomb.hbase.rowkey;

import com.nearinfinity.honeycomb.hbase.VarEncoder;

public class IndicesRow implements RowKey {
    private static final byte PREFIX = 0x02;
    private final long tableId;

    public IndicesRow(long tableId) {
        this.tableId = tableId;
    }

    @Override
    public byte[] encode() {
        byte[] table = VarEncoder.encodeULong(tableId);
        return VarEncoder.appendByteArraysWithPrefix(PREFIX, table);
    }

    @Override
    public byte getPrefix() {
        return PREFIX;
    }

    public long getTableId() {
        return tableId;
    }

    @Override
    public String toString() {
        return "[" + String.format("%02X", PREFIX) + "\t" + tableId + "]";
    }
}
