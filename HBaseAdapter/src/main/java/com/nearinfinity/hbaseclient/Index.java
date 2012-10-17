package com.nearinfinity.hbaseclient;

import com.google.common.base.Function;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Type;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public class Index {
    public static LinkedList<LinkedList<String>> indexForTable(final Map<byte[], byte[]> tableMetadata) {
        Type type = new TypeToken<LinkedList<LinkedList<String>>>() {
        }.getType();
        byte[] jsonBytes = tableMetadata.get(Constants.INDEXES);
        if (jsonBytes == null) {
            return new LinkedList<LinkedList<String>>();
        }

        return new Gson().fromJson(new String(jsonBytes), type);
    }

    public static byte[] createColumnIds(final List<String> columns, final Map<String, Long> columnNameToId) {
        return correctColumnIdSize(convertToByteArray(columns, new Function<String, byte[]>() {
            @Override
            public byte[] apply(String column) {
                return Bytes.toBytes(columnNameToId.get(column));
            }
        }));
    }

    public static byte[] createValues(final List<String> columns, final Map<String, byte[]> values) {
        return convertToByteArray(columns, new Function<String, byte[]>() {
            @Override
            public byte[] apply(String column) {
                return values.get(column);
            }
        });
    }

    private static byte[] correctColumnIdSize(byte[] columnIds) {
        int expectedSize = 4 * Bytes.SIZEOF_LONG;
        if (columnIds.length > expectedSize) {
            throw new IllegalStateException(format("There should never be more than %d columns indexed. Found %d columns.", expectedSize / Bytes.SIZEOF_LONG, columnIds.length / Bytes.SIZEOF_LONG));
        }

        if (columnIds.length == expectedSize) {
            return columnIds;
        }

        byte[] expandedColumnIds = new byte[expectedSize];
        System.arraycopy(columnIds, 0, expandedColumnIds, 0, columnIds.length);
        return expandedColumnIds;
    }

    private static byte[] convertToByteArray(final List<String> columns,
                                             final Function<String, byte[]> conversion) {
        List<byte[]> pieces = new LinkedList<byte[]>();
        int size = 0;
        for (final String column : columns) {
            byte[] bytes = conversion.apply(column);
            size += bytes.length;
            pieces.add(bytes);
        }

        return mergeByteArrayList(pieces, size);
    }

    private static byte[] mergeByteArrayList(final List<byte[]> pieces, final int size) {
        int offset = 0;
        final byte[] mergedArray = new byte[size];
        for (final byte[] piece : pieces) {
            System.arraycopy(piece, 0, mergedArray, offset, piece.length);
            offset += piece.length;
        }

        return mergedArray;
    }
}
