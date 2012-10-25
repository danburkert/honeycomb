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

        return Util.mergeByteArrays(pieces, size);
    }

    public static List<List<String>> indexForTable(final Map<byte[], byte[]> tableMetadata) {
        byte[] jsonBytes = null;
        for (Map.Entry<byte[], byte[]> entry : tableMetadata.entrySet()) {
            if (Arrays.equals(entry.getKey(), Constants.INDEXES)) {
                jsonBytes = entry.getValue();
            }
        }

        if (jsonBytes == null) {
            return new LinkedList<List<String>>();
        }

        return Util.deserializeList(jsonBytes);
    }
}
