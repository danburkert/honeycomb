package com.nearinfinity.hbaseclient;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.*;

public class PutListFactory {
    public static List<Put> createPutList(final Map<String, byte[]> values, final TableInfo info, final LinkedList<LinkedList<String>> indexedKeys) {
        final long tableId = info.getId();
        final List<Put> putList = new LinkedList<Put>();
        final UUID rowId = UUID.randomUUID();

        final Map<String, Long> columnNameToId = info.columnNameToIdMap();
        final byte[] dataKey = RowKeyFactory.buildDataKey(tableId, rowId);
        final Put dataRow = createDataRows(dataKey, values, columnNameToId);

        if (values.size() == 0) {
            // Add special []->[] data row to signify a row of all null values
            putList.add(dataRow.add(Constants.NIC, new byte[0], new byte[0]));
        } else {
            putList.add(dataRow);
        }

        final byte[] rowByteArray = createRowFromMap(values);
        final Map<String, byte[]> ascendingValues = correctAscendingValuePadding(info, values);
        final Map<String, byte[]> descendingValues = correctDescendingValuePadding(info, values);

        for (List<String> columns : indexedKeys) {
            final byte[] columnIds = Index.createColumnIds(columns, columnNameToId);

            final byte[] ascendingIndexValues = Index.createValues(columns, ascendingValues);
            final byte[] descendingIndexValues = Index.createValues(columns, descendingValues);

            final byte[] ascendingIndexKey = RowKeyFactory.buildIndexRowKey(tableId, columnIds, ascendingIndexValues, rowId);
            final byte[] descendingIndexKey = RowKeyFactory.buildReverseIndexRowKey(tableId, columnIds, descendingIndexValues, rowId);

            putList.add(new Put(ascendingIndexKey).add(Constants.NIC, Constants.VALUE_MAP, rowByteArray));
            putList.add(new Put(descendingIndexKey).add(Constants.NIC, Constants.VALUE_MAP, rowByteArray));
        }

        return putList;
    }

    private static Put createDataRows(byte[] dataKey, Map<String, byte[]> values, Map<String, Long> columnNameToId) {
        final Put dataRow = new Put(dataKey);
        for (String columnName : values.keySet()) {
            final long columnId = columnNameToId.get(columnName);
            final byte[] value = values.get(columnName);
            dataRow.add(Constants.NIC, Bytes.toBytes(columnId), value);
        }

        return dataRow;
    }

    public static Map<String, byte[]> correctAscendingValuePadding(TableInfo info, Map<String, byte[]> values) {
        return correctAscendingValuePadding(info, values, new HashSet<String>());
    }

    public static Map<String, byte[]> correctDescendingValuePadding(TableInfo info, Map<String, byte[]> values) {
        return correctDescendingValuePadding(info, values, new HashSet<String>());
    }

    public static Map<String, byte[]> correctAscendingValuePadding(TableInfo info, Map<String, byte[]> values, Set<String> nullSearchColumns) {
        return convertToCorrectOrder(info, values, nullSearchColumns, new Function<byte[], ColumnType, Integer, byte[]>() {
            @Override
            public byte[] apply(byte[] value, ColumnType columnType, Integer padLength) {
                return ValueEncoder.ascendingEncode(value, columnType, padLength);
            }
        });
    }

    public static Map<String, byte[]> correctDescendingValuePadding(TableInfo info, Map<String, byte[]> values, Set<String> nullSearchColumns) {
        return convertToCorrectOrder(info, values, nullSearchColumns, new Function<byte[], ColumnType, Integer, byte[]>() {
            @Override
            public byte[] apply(byte[] value, ColumnType columnType, Integer padLength) {
                return ValueEncoder.descendingEncode(value, columnType, padLength);
            }
        });
    }

    private static Map<String, byte[]> convertToCorrectOrder(TableInfo info, Map<String, byte[]> values, Set<String> nullSearchColumns, Function<byte[], ColumnType, Integer, byte[]> convert) {
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

    private static byte[] createRowFromMap(Map<String, byte[]> values) {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            final ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(values);
            out.close();
        } catch (IOException e) {
            return new byte[0];
        }
        return bos.toByteArray();
    }

    private interface Function<F1, F2, F3, T> {
        T apply(F1 f1, F2 f2, F3 f3);
    }
}
