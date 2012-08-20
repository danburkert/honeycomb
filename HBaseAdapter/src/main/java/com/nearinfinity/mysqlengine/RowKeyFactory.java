package com.nearinfinity.mysqlengine;

import org.apache.log4j.Logger;

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

    public static byte[] buildValueIndexKey(long tableId, long columnId, byte[] value, UUID uuid) {
        return ByteBuffer.allocate(33 + value.length)
                .put(RowType.VALUE_INDEX.getValue())
                .putLong(tableId)
                .putLong(columnId)
                .put(value)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    public static byte[] buildSecondaryIndexKey(long tableId, long columnId, byte[] value, ColumnMetadata columnType) {
        byte[] encodedValue = ValueEncoder.encodeValue(value, columnType);
        return ByteBuffer.allocate(17 + encodedValue.length)
                .put(RowType.SECONDARY_INDEX.getValue())
                .putLong(tableId)
                .putLong(columnId)
                .put(encodedValue)
                .array();
    }

    public static byte[] buildReverseIndexKey(long tableId, long columnId, byte[] value, ColumnMetadata columnType) {
        byte[] reversedValue = ValueEncoder.reverseValue(value);
        byte[] encodedValue = ValueEncoder.encodeValue(reversedValue, columnType);
        return ByteBuffer.allocate(17 + encodedValue.length)
                .put(RowType.REVERSE_INDEX.getValue())
                .putLong(tableId)
                .putLong(columnId)
                .put(encodedValue)
                .array();
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
}
