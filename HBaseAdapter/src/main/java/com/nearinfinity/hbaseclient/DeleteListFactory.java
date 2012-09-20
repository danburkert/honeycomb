package com.nearinfinity.hbaseclient;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class DeleteListFactory {
    public static List<Delete> createDeleteList(UUID uuid, TableInfo info, Result result, byte[] dataRowKey) {
        long tableId = info.getId();
        List<Delete> deleteList = new LinkedList<Delete>();
        Map<String, byte[]> valueMap = ResultParser.parseDataRow(result, info);
        deleteList.add(new Delete(dataRowKey));

        //Loop through ALL columns to determine which should be NULL
        for (String columnName : info.getColumnNames()) {
            long columnId = info.getColumnIdByName(columnName);
            byte[] value = valueMap.get(columnName);
            ColumnMetadata metadata = info.getColumnMetadata(columnName);
            ColumnType columnType = metadata.getType();

            if (value == null) {
                byte[] nullIndexKey = RowKeyFactory.buildNullIndexKey(tableId, columnId, uuid);
                deleteList.add(new Delete(nullIndexKey));
                continue;
            }

            //Determine pad length
            int padLength = 0;
            if (columnType == ColumnType.STRING || columnType == ColumnType.BINARY) {
                long maxLength = metadata.getMaxLength();
                padLength = (int) maxLength - value.length;
            }

            byte[] indexKey = RowKeyFactory.buildValueIndexKey(tableId, columnId, value, uuid, columnType, padLength);
            byte[] reverseKey = RowKeyFactory.buildReverseIndexKey(tableId, columnId, value, columnType, uuid, padLength);

            deleteList.add(new Delete(indexKey));
            deleteList.add(new Delete(reverseKey));
        }

        return deleteList;
    }
}
