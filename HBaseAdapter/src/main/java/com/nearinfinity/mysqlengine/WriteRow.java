package com.nearinfinity.mysqlengine;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class WriteRow {
    private final HTableInterface table;
    private final TableCache tableCache;

    public WriteRow(HTableInterface table, TableCache tableCache) {
        this.table = table;
        this.tableCache = tableCache;
    }

    public void writeRow(String tableName, Map<String, byte[]> values) throws IOException {
        writeRow(tableName, values, null);
    }

    public void writeRow(String tableName, Map<String, byte[]> values, byte[] unireg) throws IOException {
        //Get table id
        TableInfo info = tableCache.getTableInfo(tableName);
        long tableId = info.getId();

        //Get UUID for new entry
        UUID rowId = UUID.randomUUID();

        //Build data row key
        byte[] dataKey = RowKeyFactory.buildDataKey(tableId, rowId);

        //Create put list
        List<Put> putList = new LinkedList<Put>();

        Put dataRow = new Put(dataKey);

        byte[] indexQualifier = new byte[0];
        byte[] indexValue = new byte[0];
        if (unireg != null) {
            indexQualifier = Constants.UNIREG;
            indexValue = unireg;
        }

        boolean allRowsNull = true;

        for (String columnName : values.keySet()) {

            //Get column id and value
            long columnId = info.getColumnIdByName(columnName);
            ColumnMetadata columnType = info.getColumnTypeByName(columnName);
            byte[] value = values.get(columnName);


            if (value == null) {
                // Build null index
                byte[] nullIndexRow = RowKeyFactory.buildNullIndexKey(tableId, columnId, rowId);
                putList.add(new Put(nullIndexRow).add(Constants.NIC, new byte[0], new byte[0]));
            } else {
                allRowsNull = false;
                // Add data column to put
                dataRow.add(Constants.NIC, Bytes.toBytes(columnId), value);

                // Build value index key
                byte[] indexRow = RowKeyFactory.buildValueIndexKey(tableId, columnId, value, rowId);
                putList.add(new Put(indexRow).add(Constants.NIC, indexQualifier, indexValue));

                // Build secondary index key
                byte[] secondaryIndexRow = RowKeyFactory.buildSecondaryIndexKey(tableId, columnId, value, columnType);
                putList.add(new Put(secondaryIndexRow).add(Constants.NIC, new byte[0], new byte[0]));

                // Build reverse index key
                byte[] reverseIndexRow = RowKeyFactory.buildReverseIndexKey(tableId, columnId, value, columnType);
                putList.add(new Put(reverseIndexRow).add(Constants.NIC, Constants.VALUE_COLUMN, value));
            }
        }

        if (allRowsNull) {
            // Add special []->[] data row to signify a row of all null values
            putList.add(dataRow.add(Constants.NIC, new byte[0], new byte[0]));
        }

        //Add the row to put list
        putList.add(dataRow);

        //Final put
        table.put(putList);
    }
}
