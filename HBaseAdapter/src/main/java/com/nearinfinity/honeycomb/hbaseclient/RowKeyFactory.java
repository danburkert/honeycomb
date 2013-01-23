package com.nearinfinity.honeycomb.hbaseclient;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.nio.ByteBuffer;
import java.util.UUID;

public class RowKeyFactory {
    public static final byte[] ROOT = ByteBuffer.allocate(7)
            .put(RowType.TABLES.getValue())
            .put("TABLES".getBytes())
            .array();

    public static byte[] buildColumnsKey(final long tableId) {
        checkState(tableId >= 0);
        return ByteBuffer.allocate(9)
                .put(RowType.COLUMNS.getValue())
                .putLong(tableId)
                .array();
    }

    public static byte[] buildColumnInfoKey(final long tableId, final long columnId) {
        checkState(tableId >= 0);
        checkState(columnId >= 0);
        return ByteBuffer.allocate(17)
                .put(RowType.COLUMN_INFO.getValue())
                .putLong(tableId)
                .putLong(columnId)
                .array();
    }

    public static byte[] buildDataKey(final long tableId, final UUID uuid) {
        checkState(tableId >= 0);
        checkNotNull(uuid);
        return ByteBuffer.allocate(25)
                .put(RowType.DATA.getValue())
                .putLong(tableId)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    public static byte[] buildValueIndexPrefix(final long tableId, final byte[] columnId, final byte[] value) {
        checkState(tableId >= 0);
        checkNotNull(columnId);
        checkNotNull(value);
        return ByteBuffer.allocate(9 + value.length + columnId.length)
                .put(RowType.PRIMARY_INDEX.getValue())
                .putLong(tableId)
                .put(columnId)
                .put(value)
                .array();
    }

    public static byte[] buildTableInfoKey(final long tableId) {
        checkState(tableId >= 0);
        return ByteBuffer.allocate(9)
                .put(RowType.TABLE_INFO.getValue())
                .putLong(tableId)
                .array();
    }

    public static byte[] buildIndexRowKey(final long tableId, final byte[] columnIds, final byte[] values, final UUID uuid) {
        checkState(tableId >= 0);
        checkNotNull(columnIds);
        checkNotNull(values);
        checkNotNull(uuid);
        return ByteBuffer.allocate(25 + columnIds.length + values.length)
                .put(RowType.PRIMARY_INDEX.getValue())
                .putLong(tableId)
                .put(columnIds)
                .put(values)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    public static byte[] buildReverseIndexRowKey(final long tableId, final byte[] columnIds, final byte[] values, final UUID uuid) {
        checkState(tableId >= 0);
        checkNotNull(columnIds);
        checkNotNull(values);
        checkNotNull(uuid);
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
