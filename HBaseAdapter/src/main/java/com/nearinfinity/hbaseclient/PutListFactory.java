package com.nearinfinity.hbaseclient;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Type;
import java.util.*;

public class PutListFactory {
    public static List<Put> createPutList(final Map<String, byte[]> values, final TableInfo info, final LinkedList<LinkedList<String>> indexedKeys) {
        final long tableId = info.getId();
        final List<Put> putList = new LinkedList<Put>();
        final Map<String, Long> columnNameToId = info.columnNameToIdMap();
        final UUID rowId = UUID.randomUUID();
        final byte[] dataKey = RowKeyFactory.buildDataKey(tableId, rowId);
        final Put dataRow = createDataRows(dataKey, values, columnNameToId);

        if (dataRow.getFamilyMap().size() == 0) {
            // Add special []->[] data row to signify a row of all null values
            putList.add(dataRow.add(Constants.NIC, new byte[0], new byte[0]));
        } else {
            putList.add(dataRow);
        }

        putList.addAll(createIndexForColumns(values, info, indexedKeys, rowId));

        return putList;
    }

    public static List<Put> createIndexForColumns(Map<String, byte[]> values, TableInfo info, LinkedList<LinkedList<String>> indexedKeys, UUID rowId) {
        final long tableId = info.getId();
        final Map<String, Long> columnNameToId = info.columnNameToIdMap();
        List<Put> putList = new LinkedList<Put>();
        final byte[] rowByteArray = createRowFromMap(values);
        final Map<String, byte[]> ascendingValues = correctAscendingValuePadding(info, values);
        final Map<String, byte[]> descendingValues = correctDescendingValuePadding(info, values);

        for (List<String> columns : indexedKeys) {
            final byte[] columnIds = Index.createColumnIds(columns, columnNameToId);

            byte[] ascendingIndexKey = createPrimaryIndex(tableId, rowId, ascendingValues, columns, columnIds);
            byte[] descendingIndexKey = createReverseIndex(tableId, rowId, descendingValues, columns, columnIds);

            putList.add(createIndexPut(ascendingIndexKey, rowByteArray));
            putList.add(createIndexPut(descendingIndexKey, rowByteArray));
        }

        return putList;
    }

    public static byte[] createReverseIndex(long tableId, UUID rowId, Map<String, byte[]> descendingValues, List<String> columns, byte[] columnIds) {
        final byte[] descendingIndexValues = Index.createValues(columns, descendingValues);
        return RowKeyFactory.buildReverseIndexRowKey(tableId, columnIds, descendingIndexValues, rowId);
    }

    public static byte[] createPrimaryIndex(long tableId, UUID rowId, Map<String, byte[]> ascendingValues, List<String> columns, byte[] columnIds) {
        final byte[] ascendingIndexValues = Index.createValues(columns, ascendingValues);
        return RowKeyFactory.buildIndexRowKey(tableId, columnIds, ascendingIndexValues, rowId);
    }

    private static Put createIndexPut(byte[] key, byte[] rowByteArray) {
        return new Put(key).add(Constants.NIC, Constants.VALUE_MAP, rowByteArray);
    }

    private static Put createDataRows(final byte[] dataKey, final Map<String, byte[]> values, final Map<String, Long> columnNameToId) {
        final Put dataRow = new Put(dataKey);
        for (String columnName : values.keySet()) {
            final long columnId = columnNameToId.get(columnName);
            final byte[] value = values.get(columnName);
            if (value != null) {
                dataRow.add(Constants.NIC, Bytes.toBytes(columnId), value);
            }
        }

        return dataRow;
    }

    public static Map<String, byte[]> correctAscendingValuePadding(final TableInfo info, final Map<String, byte[]> values) {
        return correctAscendingValuePadding(info, values, new HashSet<String>());
    }

    public static Map<String, byte[]> correctDescendingValuePadding(final TableInfo info, final Map<String, byte[]> values) {
        return correctDescendingValuePadding(info, values, new HashSet<String>());
    }

    public static Map<String, byte[]> correctAscendingValuePadding(final TableInfo info, final Map<String, byte[]> values, final Set<String> nullSearchColumns) {
        return convertToCorrectOrder(info, values, nullSearchColumns, new Function<byte[], ColumnType, Integer, byte[]>() {
            @Override
            public byte[] apply(byte[] value, ColumnType columnType, Integer padLength) {
                return ValueEncoder.ascendingEncode(value, columnType, padLength);
            }
        });
    }

    public static Map<String, byte[]> correctDescendingValuePadding(final TableInfo info, final Map<String, byte[]> values, final Set<String> nullSearchColumns) {
        return convertToCorrectOrder(info, values, nullSearchColumns, new Function<byte[], ColumnType, Integer, byte[]>() {
            @Override
            public byte[] apply(byte[] value, ColumnType columnType, Integer padLength) {
                return ValueEncoder.descendingEncode(value, columnType, padLength);
            }
        });
    }

    private static Map<String, byte[]> convertToCorrectOrder(final TableInfo info, final Map<String, byte[]> values, final Set<String> nullSearchColumns, Function<byte[], ColumnType, Integer, byte[]> convert) {
        ImmutableMap.Builder<String, byte[]> result = ImmutableMap.builder();
        for (String columnName : values.keySet()) {
            final ColumnMetadata metadata = info.getColumnMetadata(columnName);
            final ColumnType columnType = info.getColumnTypeByName(columnName);
            byte[] value = values.get(columnName);
            boolean isNull = value == null || nullSearchColumns.contains(columnName);
            if (isNull) {
                value = new byte[metadata.getMaxLength()];
            }

            int padLength = 0;
            if (columnType == ColumnType.STRING || columnType == ColumnType.BINARY) {
                final long maxLength = metadata.getMaxLength();
                padLength = (int) maxLength - value.length;
            }

            byte[] paddedValue = convert.apply(value, columnType, padLength);
            if (metadata.isNullable()) {
                byte[] nullPadValue = Bytes.padHead(paddedValue, 1);
                nullPadValue[0] = isNull ? (byte) 1 : 0;
                result.put(columnName, nullPadValue);
            } else {
                result.put(columnName, paddedValue);
            }
        }

        return result.build();
    }

    private static byte[] createRowFromMap(final Map<String, byte[]> values) {
        Gson gson = new Gson();
        Type type = new TypeToken<TreeMap<String, byte[]>>() {
        }.getType();
        return gson.toJson(values, type).getBytes();
    }

    private interface Function<F1, F2, F3, T> {
        T apply(F1 f1, F2 f2, F3 f3);
    }
}
