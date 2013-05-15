package com.nearinfinity.honeycomb.hbase.rowkey;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.nearinfinity.honeycomb.hbase.VarEncoder;

/**
 * Super class for rowkeys that occur once per MySQL table
 */
public class TableIDRowKey implements RowKey {
    private final byte prefix;
    private final long tableId;

    public TableIDRowKey(final byte prefix, final long tableId) {
        Preconditions.checkArgument(tableId >= 0, "Table ID must be non-zero.");
        this.prefix = prefix;
        this.tableId = tableId;
    }

    @Override
    public byte[] encode() {
        byte[] table = VarEncoder.encodeULong(tableId);
        return VarEncoder.appendByteArraysWithPrefix(prefix, table);
    }

    @Override
    public byte getPrefix() {
        return prefix;
    }

    public long getTableId() {
        return tableId;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this.getClass())
                .add("Prefix", String.format("%02X", prefix))
                .add("TableId", tableId)
                .toString();
    }

    @Override
    public int compareTo(RowKey o) {
        int typeCompare = getPrefix() - o.getPrefix();
        if (typeCompare != 0) { return typeCompare; }
        TableIDRowKey row2 = (TableIDRowKey) o;
        return ComparisonChain.start().compare(getTableId(), row2.getTableId()).result();
    }
}