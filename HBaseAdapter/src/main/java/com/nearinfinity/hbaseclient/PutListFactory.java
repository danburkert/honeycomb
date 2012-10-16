package com.nearinfinity.hbaseclient;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.*;

public class PutListFactory {

    public static List<Put> createPutList(final Map<String, byte[]> values, final TableInfo info) {
        return createPutList(values, info, new LinkedList<LinkedList<String>>());
    }

    public static List<Put> createPutList(final Map<String, byte[]> values, final TableInfo info, final LinkedList<LinkedList<String>> multipartKeys) {
        //Get table id
        final long tableId = info.getId();
        Map<String, Long> columnNameToId = info.columnNameToIdMap();

        //Get UUID for new entry
        final UUID rowId = UUID.randomUUID();

        //Build data row key
        final byte[] dataKey = RowKeyFactory.buildDataKey(tableId, rowId);

        //Create put list
        final List<Put> putList = new LinkedList<Put>();

        final Put dataRow = new Put(dataKey);

        final byte[] rowByteArray = createRowFromMap(values);

        boolean allRowsNull = true;

        for (String columnName : values.keySet()) {

            //Get column id and value
            final long columnId = columnNameToId.get(columnName);
            final ColumnType columnType = info.getColumnTypeByName(columnName);
            final byte[] value = values.get(columnName);

            if (value == null) {
                // Build null index
                final byte[] nullIndexRow = RowKeyFactory.buildNullIndexKey(tableId, columnId, rowId);
                putList.add(new Put(nullIndexRow).add(Constants.NIC, Constants.VALUE_MAP, rowByteArray));
            } else {
                int padLength = 0;
                if (columnType == ColumnType.STRING || columnType == ColumnType.BINARY) {
                    final long maxLength = info.getColumnMetadata(columnName).getMaxLength();
                    padLength = (int) maxLength - value.length;
                }

                allRowsNull = false;
                // Add data column to put
                dataRow.add(Constants.NIC, Bytes.toBytes(columnId), value);

                // Build value index key
                final byte[] indexRow = RowKeyFactory.buildValueIndexKey(tableId, columnId, value, rowId, columnType, padLength);
                putList.add(new Put(indexRow).add(Constants.NIC, Constants.VALUE_MAP, rowByteArray));

                // Build reverse index key
                final byte[] reverseIndexRow = RowKeyFactory.buildReverseIndexKey(tableId, columnId, value, columnType, rowId, padLength);
                putList.add(new Put(reverseIndexRow).add(Constants.NIC, Constants.VALUE_MAP, rowByteArray));
            }
        }

        for (List<String> columns : multipartKeys) {
            putList.add(MultipartIndex.createPut(values, tableId, columnNameToId, rowId, rowByteArray, columns));
        }

        if (allRowsNull) {
            // Add special []->[] data row to signify a row of all null values
            putList.add(dataRow.add(Constants.NIC, new byte[0], new byte[0]));
        }

        //Add the row to put list
        putList.add(dataRow);
        return putList;
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
}
