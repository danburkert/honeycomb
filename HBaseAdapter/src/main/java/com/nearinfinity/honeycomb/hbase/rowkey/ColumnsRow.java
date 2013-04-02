package com.nearinfinity.honeycomb.hbase.rowkey;

import com.google.common.base.Objects;
import com.nearinfinity.honeycomb.hbase.VarEncoder;
import com.nearinfinity.honeycomb.util.Verify;

/**
 * Representation of the rowkey associated with the column metadata details for the tables being stored
 */
public class ColumnsRow implements RowKey {
    private static final byte PREFIX = 0x01;
    private final long tableId;

    /**
     * Creates a column metadata rowkey for the specified table identifier
     *
     * @param tableId The valid table id that this column metadata belongs to
     */
    public ColumnsRow(final long tableId) {
        Verify.isValidTableId(tableId);
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
        return Objects.toStringHelper(this.getClass())
                .add("Prefix", String.format("%02X", PREFIX))
                .add("TableId", tableId)
                .toString();
    }
}
