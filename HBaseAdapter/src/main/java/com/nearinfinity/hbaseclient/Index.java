package com.nearinfinity.hbaseclient;

import com.google.common.base.Function;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

public class Index {
    public static LinkedList<LinkedList<String>> indexForTable(final Map<byte[], byte[]> tableMetadata) {
        Type type = new TypeToken<LinkedList<LinkedList<String>>>() {
        }.getType();
        byte[] jsonBytes = null;
        for (Map.Entry<byte[], byte[]> entry : tableMetadata.entrySet()) {
            if (Arrays.equals(entry.getKey(), Constants.INDEXES)) {
                jsonBytes = entry.getValue();
            }
        }

        if (jsonBytes == null) {
            return new LinkedList<LinkedList<String>>();
        }

        return new Gson().fromJson(new String(jsonBytes), type);
    }

    public static byte[] createColumnIds(final Iterable<String> columns, final Map<String, Long> columnNameToId) {
        return correctColumnIdSize(convertToByteArray(columns, new Function<String, byte[]>() {
            @Override
            public byte[] apply(String column) {
                return Bytes.toBytes(columnNameToId.get(column));
            }
        }));
    }

    public static byte[] createValues(final Iterable<String> columns, final Map<String, byte[]> values) {
        return convertToByteArray(columns, new Function<String, byte[]>() {
            @Override
            public byte[] apply(String column) {
                return values.get(column);
            }
        });
    }

    public static int calculateIndexValuesFullLength(final Iterable<String> columns, final TableInfo info) {
        int size = 0;
        for (String column : columns) {
            size += info.getColumnMetadata(column).getMaxLength();
        }

        return size;
    }

    public static byte[] mergeByteArrayList(final Iterable<byte[]> pieces, final int size) {
        int offset = 0;
        final byte[] mergedArray = new byte[size];
        for (final byte[] piece : pieces) {
            System.arraycopy(piece, 0, mergedArray, offset, piece.length);
            offset += piece.length;
        }

        return mergedArray;
    }

    private static byte[] correctColumnIdSize(final byte[] columnIds) {
        int expectedSize = Constants.KEY_PART_COUNT * Bytes.SIZEOF_LONG;
        if (columnIds.length > expectedSize) {
            throw new IllegalStateException(format("There should never be more than %d columns indexed. Found %d columns.", expectedSize / Bytes.SIZEOF_LONG, columnIds.length / Bytes.SIZEOF_LONG));
        }

        if (columnIds.length == expectedSize) {
            return columnIds;
        }

        return Bytes.padTail(columnIds, expectedSize - columnIds.length);
    }

    private static byte[] convertToByteArray(final Iterable<String> columns,
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
}
