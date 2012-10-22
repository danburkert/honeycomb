package com.nearinfinity.hbaseclient;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class DeleteListFactory {
    public static List<Delete> createDeleteRowList(UUID uuid, TableInfo info, Result result, byte[] dataRowKey, final LinkedList<LinkedList<String>> indexedKeys) {
        long tableId = info.getId();
        List<Delete> deleteList = new LinkedList<Delete>();
        deleteList.add(new Delete(dataRowKey));

        Map<String, byte[]> values = ResultParser.parseDataRow(result, info);
        for (String columnName : info.getColumnNames()) {
            ColumnMetadata metadata = info.getColumnMetadata(columnName);
            if (!values.containsKey(columnName)) {
                values.put(columnName, new byte[metadata.getMaxLength()]);
            }
        }

        final Map<String, Long> columnNameToId = info.columnNameToIdMap();

        final Map<String, byte[]> ascendingValues = PutListFactory.correctAscendingValuePadding(info, values);
        final Map<String, byte[]> descendingValues = PutListFactory.correctDescendingValuePadding(info, values);

        for (List<String> columns : indexedKeys) {
            final byte[] columnIds = Index.createColumnIds(columns, columnNameToId);

            final byte[] ascendingIndexValues = Index.createValues(columns, ascendingValues);
            final byte[] descendingIndexValues = Index.createValues(columns, descendingValues);

            final byte[] ascendingIndexKey = RowKeyFactory.buildIndexRowKey(tableId, columnIds, ascendingIndexValues, uuid);
            final byte[] descendingIndexKey = RowKeyFactory.buildReverseIndexRowKey(tableId, columnIds, descendingIndexValues, uuid);

            deleteList.add(new Delete(ascendingIndexKey));
            deleteList.add(new Delete(descendingIndexKey));
        }

        return deleteList;
    }
}
