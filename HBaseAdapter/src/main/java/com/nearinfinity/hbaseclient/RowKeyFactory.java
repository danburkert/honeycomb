package com.nearinfinity.hbaseclient;

import java.nio.ByteBuffer;
import java.util.UUID;

public class RowKeyFactory {
    public static final byte[] ROOT = ByteBuffer.allocate(7)
            .put(RowType.TABLES.getValue())
            .put("TABLES".getBytes())
            .array();

    public static byte[] buildColumnsKey(long tableId) {
        return ByteBuffer.allocate(9)
                .put(RowType.COLUMNS.getValue())
                .putLong(tableId)
                .array();
    }

    public static byte[] buildColumnInfoKey(long tableId, long columnId) {
        return ByteBuffer.allocate(17)
                .put(RowType.COLUMN_INFO.getValue())
                .putLong(tableId)
                .putLong(columnId)
                .array();
    }

    public static byte[] buildDataKey(long tableId, UUID uuid) {
        return ByteBuffer.allocate(25)
                .put(RowType.DATA.getValue())
                .putLong(tableId)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    public static byte[] buildValueIndexPrefix(long tableId, byte[] columnId, byte[] value) {
        return ByteBuffer.allocate(9 + value.length + columnId.length)
                .put(RowType.PRIMARY_INDEX.getValue())
                .putLong(tableId)
                .put(columnId)
                .put(value)
                .array();
    }

    public static byte[] buildTableInfoKey(long tableId) {
        return ByteBuffer.allocate(9)
                .put(RowType.TABLE_INFO.getValue())
                .putLong(tableId)
                .array();
    }

    public static byte[] buildIndexRowKey(long tableId, byte[] columnIds, byte[] values, UUID uuid) {
        return ByteBuffer.allocate(25 + columnIds.length + values.length)
                .put(RowType.PRIMARY_INDEX.getValue())
                .putLong(tableId)
                .put(columnIds)
                .put(values)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    public static byte[] buildReverseIndexRowKey(long tableId, byte[] columnIds, byte[] values, UUID uuid) {
        return ByteBuffer.allocate(25 + columnIds.length + values.length)
                .put(RowType.REVERSE_INDEX.getValue())
                .putLong(tableId)
                .put(columnIds)
                .put(values)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }
}
