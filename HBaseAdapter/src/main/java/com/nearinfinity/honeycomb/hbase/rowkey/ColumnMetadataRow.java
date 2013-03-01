package com.nearinfinity.honeycomb.hbase.rowkey;

import com.google.common.base.Preconditions;
import com.nearinfinity.honeycomb.hbase.RowKey;
import com.nearinfinity.honeycomb.hbase.VarEncoder;

public class ColumnMetadataRow implements RowKey {
    private static final byte PREFIX = 0x02;
    private long tableId;
    private long columnId;

    public ColumnMetadataRow(long tableId, long columnId) {
        Preconditions.checkArgument(tableId >= 0, "Table ID must be non-zero.");
        Preconditions.checkArgument(columnId >= 0, "Column ID must be non-zero.");
        this.tableId = tableId;
        this.columnId = columnId;
    }

    public byte[] encode() {
        return VarEncoder.appendByteArraysWithPrefix(PREFIX,
                VarEncoder.encodeULong(tableId),
                VarEncoder.encodeULong(columnId));
    }

    public long getTableId() {
        return tableId;
    }

    public long getColumnId() {
        return columnId;
    }

    public byte getPrefix() {
        return PREFIX;
    }

    @Override
    public String toString() {
        return "[" + String.format("%02X", PREFIX) + "\t"
                + tableId + "\t" + columnId + "]";
    }
}