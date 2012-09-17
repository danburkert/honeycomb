package com.nearinfinity.hbaseclient;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/9/12
 * Time: 9:43 AM
 * To change this template use File | Settings | File Templates.
 */
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

    public static byte[] buildValueIndexKey(long tableId, long columnId, byte[] value, UUID uuid, ColumnType columnType, int padLength) {
        byte[] encodedValue = ValueEncoder.encodeValue(value, columnType);
        byte[] paddedValue = ValueEncoder.padValueAscending(encodedValue, padLength);
        return ByteBuffer.allocate(33 + paddedValue.length)
                .put(RowType.PRIMARY_INDEX.getValue())
                .putLong(tableId)
                .putLong(columnId)
                .put(paddedValue)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    public static byte[] buildReverseIndexKey(long tableId, long columnId, byte[] value, ColumnType columnType, UUID rowId, int padLength) {
        return buildReverseIndexKey(tableId, columnId, value, columnType, rowId, padLength, false);
    }

    public static byte[] buildEmptyValueReverseIndexKey(long tableId, long columnId, byte[] value, ColumnType columnType, UUID rowId, int padLength) {
        return buildReverseIndexKey(tableId, columnId, value, columnType, rowId, padLength, true);
    }

    public static byte[] buildNullIndexKey(long tableId, long columnId, UUID uuid) {
        return ByteBuffer.allocate(33)
                .put(RowType.NULL_INDEX.getValue())
                .putLong(tableId)
                .putLong(columnId)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    public static byte[] buildValueIndexPrefix(long tableId, long columnId, byte[] value, ColumnType columnType) {
        byte[] encodedValue = ValueEncoder.encodeValue(value, columnType);
        return ByteBuffer.allocate(17 + encodedValue.length)
                .put(RowType.PRIMARY_INDEX.getValue())
                .putLong(tableId)
                .putLong(columnId)
                .put(encodedValue)
                .array();
    }

    private static byte[] buildReverseIndexKey(long tableId, long columnId, byte[] value, ColumnType columnType, UUID rowId, int padLength, boolean emptyValue) {
        byte[] encodedValue = ValueEncoder.encodeValue(value, columnType);
        byte[] reversedValue = ValueEncoder.reverseValue(encodedValue);
        byte[] paddedValue = ValueEncoder.padValueDescending(reversedValue, padLength);
        byte[] putValue = emptyValue ? new byte[paddedValue.length] : paddedValue;

        return ByteBuffer.allocate(33 + paddedValue.length)
                .put(RowType.REVERSE_INDEX.getValue())
                .putLong(tableId)
                .putLong(columnId)
                .put(putValue)
                .putLong(rowId.getMostSignificantBits())
                .putLong(rowId.getLeastSignificantBits())
                .array();
    }
}
