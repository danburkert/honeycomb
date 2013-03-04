package com.nearinfinity.honeycomb.hbase.rowkey;

import com.google.common.base.Preconditions;
import com.nearinfinity.honeycomb.hbase.RowKey;
import com.nearinfinity.honeycomb.hbase.VarEncoder;
import com.nearinfinity.honeycomb.mysql.Util;

import java.util.UUID;

public class DataRow implements RowKey {
    private static final byte PREFIX = 0x04;
    private long tableId;
    private UUID uuid;

    public DataRow(long tableId) {
        Preconditions.checkArgument(tableId >= 0, "Table ID must be non-zero.");
        this.tableId = tableId;
        this.uuid = null;
    }

    public DataRow(long tableId, UUID uuid) {
        this(tableId);
        Preconditions.checkNotNull(uuid, "Data RowKey UUID must not be null.");
        this.uuid = uuid;
    }

    public byte[] encode() {
        if (uuid != null) {
            return VarEncoder.appendByteArraysWithPrefix(PREFIX,
                    VarEncoder.encodeULong(tableId),
                    Util.UUIDToBytes(uuid));
        } else {
            return  VarEncoder.appendByteArraysWithPrefix(PREFIX,
                    VarEncoder.encodeULong(tableId));
        }

    }

    public long getTableId() {
        return tableId;
    }

    public UUID getUuid() {
        return uuid;
    }

    public byte getPrefix() {
        return PREFIX;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        sb.append(String.format("%02X", PREFIX));
        sb.append("\t");
        sb.append(tableId);
        sb.append("\t");
        sb.append(uuid == null ? "" : Util.generateHexString(Util.UUIDToBytes(uuid)));
        sb.append("]");
        return sb.toString();
    }
}
