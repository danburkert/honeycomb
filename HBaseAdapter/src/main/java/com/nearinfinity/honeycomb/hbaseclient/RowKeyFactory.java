package com.nearinfinity.honeycomb.hbaseclient;

import java.nio.ByteBuffer;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class RowKeyFactory {
    public static final byte[] ROOT = ByteBuffer.allocate(7)
            .put(RowType.TABLES.getValue())
            .put("TABLES".getBytes())
            .array();

    /**
     * Constructs the row key for the columns entry for a given SQL table
     *
     * @param tableId SQL table ID
     * @return Row key
     */
    public static byte[] buildColumnsKey(final long tableId) {
        checkState(tableId >= 0);
        return ByteBuffer.allocate(9)
                .put(RowType.COLUMNS.getValue())
                .putLong(tableId)
                .array();
    }

    /**
     * Construct the row key for the column metadata entry for a given SQL table and column.
     *
     * @param tableId  SQL table ID
     * @param columnId SQL column ID
     * @return Row key
     */
    public static byte[] buildColumnInfoKey(final long tableId, final long columnId) {
        checkState(tableId >= 0);
        checkState(columnId >= 0);
        return ByteBuffer.allocate(17)
                .put(RowType.COLUMN_INFO.getValue())
                .putLong(tableId)
                .putLong(columnId)
                .array();
    }

    /**
     * Constructs the row key for a data row with a unique identifier
     *
     * @param tableId SQL table ID
     * @param uuid    Unique identifier
     * @return Row key
     */
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

    /**
     * Constructs the prefix row key for an ascending index scan.
     *
     * @param tableId  SQL table ID
     * @param columnId SQL column ID
     * @param value SQL row
     * @return Row key
     */
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

    /**
     * Constructs the row key for the table metadata entry for a SQL table
     *
     * @param tableId SQL table ID
     * @return Row key
     */
    public static byte[] buildTableInfoKey(final long tableId) {
        checkState(tableId >= 0);
        return ByteBuffer.allocate(9)
                .put(RowType.TABLE_INFO.getValue())
                .putLong(tableId)
                .array();
    }

    /**
     * Constructs the row key for an entry in the ascending index.
     *
     * @param tableId   SQL table ID
     * @param columnIds SQL column IDs
     * @param values    SQL row
     * @param uuid      Unique identifier
     * @return Row key
     */
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

    /**
     * Constructs the row key for an entry in the descending index.
     *
     * @param tableId   SQL table ID
     * @param columnIds SQL column IDs
     * @param values    SQL row
     * @param uuid      Unique identifier
     * @return Row key
     */
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
