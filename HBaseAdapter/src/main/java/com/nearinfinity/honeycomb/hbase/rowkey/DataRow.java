package com.nearinfinity.honeycomb.hbase.rowkey;

import java.util.UUID;

import com.google.common.base.Objects;
import com.nearinfinity.honeycomb.hbase.VarEncoder;
import com.nearinfinity.honeycomb.mysql.Util;
import com.nearinfinity.honeycomb.mysql.Verify;

public class DataRow implements RowKey {
    private static final byte PREFIX = 0x06;
    private final long tableId;
    private final UUID uuid;

    public DataRow(final long tableId) {
        this(tableId, null);
    }

    public DataRow(final long tableId, final UUID uuid) {
        Verify.isValidTableId(tableId);
        this.tableId = tableId;
        this.uuid = uuid;
    }

    @Override
    public byte[] encode() {
        if (uuid != null) {
            return VarEncoder.appendByteArraysWithPrefix(PREFIX,
                    VarEncoder.encodeULong(tableId),
                    Util.UUIDToBytes(uuid));
        }

        return  VarEncoder.appendByteArraysWithPrefix(PREFIX,
                VarEncoder.encodeULong(tableId));
    }

    public long getTableId() {
        return tableId;
    }

    public UUID getUuid() {
        return uuid;
    }

    @Override
    public byte getPrefix() {
        return PREFIX;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this.getClass())
            .add("Prefix", String.format("%02X", PREFIX))
            .add("TableId", tableId)
            .add("UUID", uuid == null ? "" : Util.generateHexString(Util.UUIDToBytes(uuid)))
            .toString();
    }
}
