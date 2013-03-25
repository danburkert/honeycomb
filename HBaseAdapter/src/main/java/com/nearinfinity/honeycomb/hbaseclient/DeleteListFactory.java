package com.nearinfinity.honeycomb.hbaseclient;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;

import java.util.*;

public class DeleteListFactory {
    /**
     * Creates the required HBase {@code Delete's} to remove a SQL row from HBase.
     *
     * @param uuid        Unique identifier of the SQL row
     * @param info        Table metadata
     * @param result      The SQL row from the data section
     * @param dataRowKey  The HBase row key of the SQL row in the data section
     * @param indexedKeys All indexed columns in the SQL table
     * @return HBase deletes
     */
    public static List<Delete> createDeleteRowList(UUID uuid, TableInfo info, Result result, byte[] dataRowKey, final List<List<String>> indexedKeys) {
        List<Delete> deleteList = new LinkedList<Delete>();
        deleteList.add(new Delete(dataRowKey));

        Map<String, byte[]> values = ResultReader.readDataRow(result, info).getRecords();
        Set<String> columnNames = info.getColumnNames();
        for (String columnName : columnNames) {
            if (!values.containsKey(columnName)) {
                values.put(columnName, null);
            }
        }

        deleteList.addAll(createDeleteForIndex(values, info, indexedKeys, uuid));

        return deleteList;
    }

    /**
     * Creates the required HBase {@code Delete's} to remove an index for a SQL row from HBase.
     *
     * @param values      SQL row
     * @param info        Table metadata
     * @param indexedKeys One index in the SQL table
     * @param uuid        Unique identifier of the SQL row
     * @return HBase deletes
     */
    public static List<Delete> createDeleteForIndex(Map<String, byte[]> values, TableInfo info, UUID uuid, List<String> indexedKeys) {
        List<List<String>> newIndexColumns = new LinkedList<List<String>>();
        newIndexColumns.add(new LinkedList<String>(indexedKeys));
        return createDeleteForIndex(values, info, newIndexColumns, uuid);
    }

    /**
     * Creates the required HBase {@code Delete's} to remove all indexes for a SQL row from HBase.
     *
     * @param values      SQL row
     * @param info        Table metadata
     * @param indexedKeys All indexed columns in the SQL table
     * @param uuid        Unique identifier of the SQL row
     * @return HBase deletes
     */
    public static List<Delete> createDeleteForIndex(Map<String, byte[]> values, TableInfo info, List<List<String>> indexedKeys, UUID uuid) {
        final long tableId = info.getId();
        List<Delete> deleteList = new LinkedList<Delete>();
        final Map<String, Long> columnNameToId = info.columnNameToIdMap();

        final Map<String, byte[]> ascendingValues = ValueEncoder.correctAscendingValuePadding(info, values);
        final Map<String, byte[]> descendingValues = ValueEncoder.correctDescendingValuePadding(info, values);

        for (List<String> columns : indexedKeys) {
            final byte[] columnIds = Index.createColumnIds(columns, columnNameToId);

            final byte[] ascendingIndexKey = Index.createPrimaryIndex(tableId, uuid, ascendingValues, columns, columnIds);
            final byte[] descendingIndexKey = Index.createReverseIndex(tableId, uuid, descendingValues, columns, columnIds);

            deleteList.add(new Delete(ascendingIndexKey));
            deleteList.add(new Delete(descendingIndexKey));
        }

        return deleteList;
    }
}
