package com.nearinfinity.honeycomb.hbaseclient;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PutListFactory {
    /**
     * Takes a MySQL row and turns it into a collection of HBase rows for insert/update.
     *
     * @param values      A MySQL row
     * @param info        Metadata about the HBase sql table
     * @param indexedKeys Columns with an index
     * @return HBase table rows
     */
    public static List<Put> createDataInsertPutList(final Map<String, byte[]> values, final TableInfo info, final List<List<String>> indexedKeys) {
        UUID rowId = UUID.randomUUID();
        final long tableId = info.getId();
        final List<Put> putList = new LinkedList<Put>();
        final Map<String, Long> columnNameToId = info.columnNameToIdMap();
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

    /**
     * Creates HBase index rows for a single SQL index.
     *
     * @param values      A MySQL row
     * @param info        Metadata about the HBase sql table
     * @param indexedKeys A column with an index
     * @param rowId       The unique identifier for a MySQL row
     * @return HBase index rows
     */
    public static List<Put> createIndexForColumns(Map<String, byte[]> values, TableInfo info, UUID rowId, List<String> indexedKeys) {
        List<List<String>> newIndexColumns = new LinkedList<List<String>>();
        newIndexColumns.add(new LinkedList<String>(indexedKeys));
        return createIndexForColumns(values, info, newIndexColumns, rowId);
    }

    /**
     * Creates HBase index rows for all SQL indexes for insert/update.
     *
     * @param values      A MySQL row
     * @param info        Metadata about the HBase sql table
     * @param indexedKeys Columns with an index
     * @param rowId       The unique identifier for a MySQL row
     * @return HBase index rows
     */
    public static List<Put> createIndexForColumns(Map<String, byte[]> values, TableInfo info, List<List<String>> indexedKeys, UUID rowId) {
        final long tableId = info.getId();
        final Map<String, Long> columnNameToId = info.columnNameToIdMap();
        final List<Put> putList = new LinkedList<Put>();
        final byte[] rowByteArray = Util.serializeMap(values);
        final Map<String, byte[]> ascendingValues = ValueEncoder.correctAscendingValuePadding(info, values);
        final Map<String, byte[]> descendingValues = ValueEncoder.correctDescendingValuePadding(info, values);

        for (List<String> columns : indexedKeys) {
            final byte[] columnIds = Index.createColumnIds(columns, columnNameToId);

            byte[] ascendingIndexKey = Index.createPrimaryIndex(tableId, rowId, ascendingValues, columns, columnIds);
            byte[] descendingIndexKey = Index.createReverseIndex(tableId, rowId, descendingValues, columns, columnIds);

            putList.add(createIndexPut(ascendingIndexKey, rowByteArray));
            putList.add(createIndexPut(descendingIndexKey, rowByteArray));
        }

        return putList;
    }

    private static Put createIndexPut(byte[] key, byte[] rowByteArray) {
        return new Put(key).add(Constants.NIC, Constants.VALUE_MAP, rowByteArray);
    }

    private static Put createDataRows(final byte[] dataKey, final Map<String, byte[]> values, final Map<String, Long> columnNameToId) {
        final Put dataRow = new Put(dataKey);
        for (Map.Entry<String, byte[]> entry : values.entrySet()) {
            final long columnId = columnNameToId.get(entry.getKey());
            final byte[] value = entry.getValue();
            if (value != null) {
                dataRow.add(Constants.NIC, Bytes.toBytes(columnId), value);
            }
        }

        return dataRow;
    }

}
