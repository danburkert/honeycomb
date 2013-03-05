package com.nearinfinity.honeycomb.hbase;

import static com.google.common.base.Preconditions.checkArgument;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Helper functions for variable length encoding data in a binary-sort safe
 * manner.
 */
public class VarEncoder {

    public static byte[] encodeULong(long value) {
        checkArgument(value >= 0, "Encoded long must be non-negative");
        int size = varLongSize(value);
        byte[] encodedValue = new byte[1 + size];
        encodedValue[0] = (byte) size;
        for (int i = size; i > 0; i--) {
            encodedValue[i] = (byte) value;
            value >>>= 8;
        }
        return encodedValue;
    }

    private static int varLongSize(final long value) {
        if ((value & (0xffffffffffffffffL << 8)) == 0) return 1;
        if ((value & (0xffffffffffffffffL << 16)) == 0) return 2;
        if ((value & (0xffffffffffffffffL << 24)) == 0) return 3;
        if ((value & (0xffffffffffffffffL << 32)) == 0) return 4;
        if ((value & (0xffffffffffffffffL << 40)) == 0) return 5;
        if ((value & (0xffffffffffffffffL << 48)) == 0) return 6;
        if ((value & (0xffffffffffffffffL << 56)) == 0) return 7;
        return 8;
    }

    public static long decodeULong(byte[] encodedValue) {
        long value = 0;
        long mask;
        for (int i = 1; i < encodedValue.length; i++) {
            mask = 0xFF & encodedValue[i];
            value = value << 8;
            value |= mask;
        }
        return value;
    }

    public static byte[] encodeBytes(final byte[] value) {
        byte[] length = encodeULong(value.length);
        return ByteBuffer
                .allocate(length.length + value.length)
                .put(length)
                .put(value)
                .array();
    }

    public static byte[] decodeBytes(final byte[] bytes) {
        int start = bytes[0] + 1;
        int length = bytes.length - start;
        byte[] decodedBytes = new byte[length];
        for (int i = 0; i < length; i++) {
            decodedBytes[i] = bytes[start + i];
        }
        return decodedBytes;
    }

    public static byte[] appendByteArrays(List<byte[]> arrays) {
        int size = 0;
        for (byte[] array : arrays) {
            size += array.length;
        }
        ByteBuffer bb = ByteBuffer.allocate(size);
        for (byte[] array : arrays) {
            bb.put(array);
        }
        return bb.array();
    }

    public static byte[] appendByteArraysWithPrefix(byte prefix, byte[]... arrays) {
        List<byte[]> elements = new ArrayList<byte[]>();
        byte[] prefixBytes = {prefix};
        elements.add(prefixBytes);
        elements.addAll(Arrays.asList(arrays));
        return appendByteArrays(elements);
    }
}