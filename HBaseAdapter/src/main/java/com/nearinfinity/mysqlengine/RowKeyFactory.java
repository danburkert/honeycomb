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

    private static final byte BYTE_MASK = (byte) 0x000000ff;

    private static final long INVERT_SIGN_MASK = 0x8000000000000000L;

    private static final long INVERT_ALL_BITS_MASK = 0xFFFFFFFFFFFFFFFFL;

    private static final Logger logger = Logger.getLogger(RowKeyFactory.class);

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
        byte[] encodedValue = encodeValue(value, columnType);
        return ByteBuffer.allocate(17 + encodedValue.length)
                .put(RowType.SECONDARY_INDEX.getValue())
                .putLong(tableId)
                .putLong(columnId)
                .put(encodedValue)
                .array();
    }

    public static byte[] buildReverseIndexKey(long tableId, long columnId, byte[] value, ColumnMetadata columnType) {
        byte[] reversedValue = reverseValue(value);
        byte[] encodedValue = encodeValue(reversedValue, columnType);
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

    public static byte[] parseValueFromReverseIndexKey(byte[] reverseIndexKey, ColumnMetadata columnType) {
        byte[] reversedValue = wrapAndGet(reverseIndexKey, 17, reverseIndexKey.length - 17);
        byte[] encodedValue = reverseValue(reversedValue);
        return encodeValue(encodedValue, columnType);
    }

    public static byte[] parseValueFromSecondaryIndexKey(byte[] secondaryIndexKey, ColumnMetadata columnType) {
        byte[] encodedValue = wrapAndGet(secondaryIndexKey, 17, secondaryIndexKey.length - 17);
        return encodeValue(encodedValue, columnType);
    }

    private static byte[] reverseValue(byte[] value) {
        ByteBuffer buffer = ByteBuffer.allocate(value.length);

        for (int i = 0 ; i < value.length ; i++) {
            buffer.put((byte) (BYTE_MASK ^ value[i]));
        }

        return buffer.array();
    }

    private static byte[] encodeValue(byte[] value, ColumnMetadata columnType) {
        if (value == null || value.length == 0) {
            return new byte[0];
        }
        byte[] encodedValue;
        switch (columnType) {
            case LONG: {
                long longValue = ByteBuffer.wrap(value).getLong();
                encodedValue = positionOfLong(longValue);
            } break;
            case DOUBLE: {
                double doubleValue = ByteBuffer.wrap(value).getDouble();
                encodedValue = positionOfDouble(doubleValue);
            } break;
            default:
                encodedValue = value;
        }
        return encodedValue;
    }

    private static byte[] positionOfLong(long value) {
        return ByteBuffer.allocate(8).putLong(value ^ INVERT_SIGN_MASK).array();
    }

    private static byte[] positionOfDouble(double value) {
        long longValue = Double.doubleToLongBits(value);
        if (value < 0.0) {
            return ByteBuffer.allocate(8).putLong(longValue ^ INVERT_ALL_BITS_MASK).array();
        }
        return ByteBuffer.allocate(8).putLong(longValue ^ INVERT_SIGN_MASK).array();
    }

    private static byte[] wrapAndGet(byte[] array, int offset, int length) {
        ByteBuffer buffer = ByteBuffer.wrap(array);
        buffer.position(offset);
        byte[] ans = new byte[length];
        buffer.get(ans);
        return ans;
    }
}
