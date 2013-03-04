package com.nearinfinity.honeycomb.hbase.rowkey;

import com.google.common.base.Preconditions;
import com.nearinfinity.honeycomb.hbase.RowKey;
import com.nearinfinity.honeycomb.hbase.VarEncoder;

public class MetadataRow implements RowKey {
    private byte prefix;

    private long tableId;

    public MetadataRow(long tableId, byte prefix) {
        Preconditions.checkArgument(tableId >= 0, "Table ID must be non-zero.");
        this.tableId = tableId;
        this.prefix = prefix;
    }

    public byte[] encode() {
        byte[] table = VarEncoder.encodeULong(tableId);
        byte[] ret = VarEncoder.appendByteArraysWithPrefix(prefix, table);
        return ret;
    }

    public byte getPrefix() {
        return prefix;
    }

    public long getTableId() {
        return tableId;
    }

    @Override
    public String toString() {
        return "[" + String.format("%02X", prefix) + "\t" + tableId + "]";
    }
}
