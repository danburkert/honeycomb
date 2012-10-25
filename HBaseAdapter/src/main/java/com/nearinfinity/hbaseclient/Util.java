package com.nearinfinity.hbaseclient;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public final class Util {
    private static Type mapType = new TypeToken<Map<String, byte[]>>() {
    }.getType();
    private static Type listType = new TypeToken<List<List<String>>>() {
    }.getType();
    private static Gson gson = new Gson();

    public static byte[] mergeByteArrays(final Iterable<byte[]> pieces, final int size) {
        if (size < 0) {
            throw new IllegalArgumentException("size must be positive.");
        }

        int offset = 0;
        final byte[] mergedArray = new byte[size];
        for (final byte[] piece : pieces) {
            int totalLength = offset + piece.length;
            if (totalLength > size) {
                throw new IllegalStateException("Merging byte arrays would exceed allocated size.");
            }

            System.arraycopy(piece, 0, mergedArray, offset, piece.length);
            offset += piece.length;
        }

        return mergedArray;
    }

    public static byte[] incrementColumn(final byte[] columnIds, final int offset) {
        if (columnIds == null) {
            throw new IllegalArgumentException("columnIds cannot be null");
        }

        if (offset < 0) {
            throw new IllegalArgumentException("offset must be positive");
        }

        if (offset > (columnIds.length - Bytes.SIZEOF_LONG)) {
            throw new IllegalArgumentException("offset must be less than the length of columnIds");
        }

        final byte[] nextColumn = new byte[columnIds.length];
        final long nextColumnId = Bytes.toLong(columnIds, offset) + 1;
        final int finalOffset = offset + Bytes.SIZEOF_LONG;

        System.arraycopy(columnIds, 0, nextColumn, 0, offset);
        System.arraycopy(Bytes.toBytes(nextColumnId), 0, nextColumn, offset, Bytes.SIZEOF_LONG);
        System.arraycopy(columnIds, finalOffset, nextColumn, finalOffset, columnIds.length - finalOffset);

        return nextColumn;
    }

    public static byte[] serializeMap(final Map<String, byte[]> values) {
        if (values == null) {
            throw new IllegalArgumentException("values cannot be null");
        }

        return gson.toJson(values, mapType).getBytes();
    }

    public static Map<String, byte[]> deserializeMap(final byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes cannot be null");
        }

        return gson.fromJson(new String(bytes), mapType);
    }

    public static byte[] serializeList(final List<List<String>> list) {
        if (list == null) {
            throw new IllegalArgumentException("list cannot be null");
        }

        return gson.toJson(list, listType).getBytes();
    }

    public static List<List<String>> deserializeList(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes cannot be null");
        }

        return gson.fromJson(new String(bytes), listType);
    }
}
