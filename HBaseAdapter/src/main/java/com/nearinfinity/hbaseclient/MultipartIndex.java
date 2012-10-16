package com.nearinfinity.hbaseclient;

import com.google.common.base.Function;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Type;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class MultipartIndex {
    public static LinkedList<LinkedList<String>> indexForTable(Map<byte[], byte[]> tableMetadata) {
        Type type = new TypeToken<LinkedList<LinkedList<String>>>() {
        }.getType();
        byte[] jsonBytes = tableMetadata.get(Constants.MULTIPART_KEY);
        if (jsonBytes == null) {
            return new LinkedList<LinkedList<String>>();
        }

        return new Gson().fromJson(new String(jsonBytes), type);
    }

    public static Put createPut(Map<String, byte[]> values, long tableId, Map<String, Long> columnNameToId, UUID rowId, byte[] rowByteArray, List<String> columns) {
        byte[] columnIds = createMultipartColumnIds(columns, columnNameToId);
        byte[] indexValues = createMultipartValues(columns, values);
        final byte[] multipartIndexKey = RowKeyFactory.buildMultipartIndexKey(tableId, columnIds, indexValues, rowId);
        return new Put(multipartIndexKey).add(Constants.NIC, Constants.VALUE_MAP, rowByteArray);
    }

    private static byte[] createMultipartColumnIds(final List<String> columns, final Map<String, Long> columnNameToId) {
        return convertToByteArray(columns, new Function<String, byte[]>() {
            @Override
            public byte[] apply(String column) {
                return Bytes.toBytes(columnNameToId.get(column));
            }
        });
    }

    private static byte[] createMultipartValues(final List<String> columns, final Map<String, byte[]> values) {
        return convertToByteArray(columns, new Function<String, byte[]>() {
            @Override
            public byte[] apply(String column) {
                return values.get(column);
            }
        });
    }

    private static byte[] convertToByteArray(List<String> columns, Function<String, byte[]> conversion) {
        List<byte[]> pieces = new LinkedList<byte[]>();
        int size = 0;
        for (final String column : columns) {
            byte[] bytes = conversion.apply(column);
            size += bytes.length;
            pieces.add(bytes);
        }

        return mergeByteArrayList(pieces, size);
    }

    private static byte[] mergeByteArrayList(List<byte[]> pieces, int size) {
        int offset = 0;
        final byte[] mergedArray = new byte[size];
        for (final byte[] piece : pieces) {
            System.arraycopy(piece, 0, mergedArray, offset, piece.length);
            offset += piece.length;
        }

        return mergedArray;
    }
}
