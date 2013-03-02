package com.nearinfinity.honeycomb.hbase.rowkey;

import com.google.common.base.Preconditions;
import com.nearinfinity.honeycomb.hbase.RowKey;
import com.nearinfinity.honeycomb.hbase.VarEncoder;

public class TableMetadataRow implements RowKey {
    private static final byte PREFIX = 0x03;
    private long tableId;

    public TableMetadataRow(long tableId) {
        Preconditions.checkArgument(tableId >= 0, "Table ID must be non-zero.");
        this.tableId = tableId;
    }

    public byte[] encode() {
        return VarEncoder.appendByteArraysWithPrefix(PREFIX,
                VarEncoder.encodeULong(tableId));
    }

    public long getTableId() {
        return tableId;
    }

    public byte getPrefix() {
        return PREFIX;
    }

    @Override
    public String toString() {
        return "[" + String.format("%02X", PREFIX)
                + "\t" + tableId + "]";
    }
}