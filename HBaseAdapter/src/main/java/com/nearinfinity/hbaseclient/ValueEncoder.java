package com.nearinfinity.hbaseclient;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ValueEncoder {
    private static final byte BYTE_MASK = (byte) 0x000000ff;

    private static final byte ASC_BYTE_MASK = (byte) 0x00000000;

    private static final byte NEGATIVE_MASK = (byte) 0x00000080;

    private static final long INVERT_SIGN_MASK = 0x8000000000000000L;

    private static final long INVERT_ALL_BITS_MASK = 0xFFFFFFFFFFFFFFFFL;

    public static byte[] descendingEncode(final byte[] value, final ColumnType columnType, final int padLength) {
        final byte[] encodedValue = ValueEncoder.encodeValue(value, columnType);
        final byte[] reversedValue = ValueEncoder.reverseValue(encodedValue);
        final byte[] paddedValue = ValueEncoder.padValueDescending(reversedValue, padLength);
        return paddedValue;
    }

    public static byte[] ascendingEncode(final byte[] value, final ColumnType columnType, final int padLength) {
        final byte[] encodedValue = ValueEncoder.encodeValue(value, columnType);
        return ValueEncoder.padValueAscending(encodedValue, padLength);
    }

    private static byte[] encodeValue(byte[] value, ColumnType columnType) {
        if (value == null || value.length == 0) {
            return new byte[0];
        }

        byte[] encodedValue;
        switch (columnType) {
            case LONG: {
                long longValue = ByteBuffer.wrap(value).getLong();
                encodedValue = positionOfLong(longValue);
            }
            break;
            case DOUBLE: {
                double doubleValue = ByteBuffer.wrap(value).getDouble();
                encodedValue = positionOfDouble(doubleValue);
            }
            break;
            default:
                encodedValue = value;
        }

        return encodedValue;
    }

    private static byte[] padValueDescending(byte[] value, int padLength) {
        return padValue(value, padLength, BYTE_MASK);
    }

    private static byte[] padValueAscending(byte[] value, int padLength) {
        return padValue(value, padLength, ASC_BYTE_MASK);
    }

    private static byte[] reverseValue(byte[] value) {
        ByteBuffer buffer = ByteBuffer.allocate(value.length);

        for (byte aValue : value) {
            buffer.put((byte) (~aValue));
        }

        return buffer.array();
    }

    private static byte[] padValue(byte[] value, int padLength, byte mask) {
        byte[] paddedValue = new byte[value.length + padLength];
        Arrays.fill(paddedValue, mask);
        System.arraycopy(value, 0, paddedValue, 0, value.length);

        return paddedValue;
    }

    private static byte[] positionOfLong(long value) {
        return ByteBuffer.allocate(8).putLong(value ^ INVERT_SIGN_MASK).array();
    }

    private static byte[] positionOfDouble(double value) {
        long longValue = Double.doubleToLongBits(value);
        if (isNegative(value)) {
            return ByteBuffer.allocate(8).putLong(longValue ^ INVERT_ALL_BITS_MASK).array();
        }
        return ByteBuffer.allocate(8).putLong(longValue ^ INVERT_SIGN_MASK).array();
    }

    private static boolean isNegative(double value) {
        byte[] bytes = ByteBuffer.allocate(8).putDouble(value).array();
        return (bytes[0] & NEGATIVE_MASK) != 0;
    }
}
