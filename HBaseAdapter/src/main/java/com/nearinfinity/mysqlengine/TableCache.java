package com.nearinfinity.mysqlengine;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TableCache {
    private final ConcurrentHashMap<String, TableInfo> tableCache = new ConcurrentHashMap<String, TableInfo>();
    private final HTable table;

    public TableCache(HTable table) {
        this.table = table;
    }

    public TableInfo getTableInfo(String tableName) throws IOException {
        if (tableCache.containsKey(tableName)) {
            return tableCache.get(tableName);
        }

        //Get the table id from HBase
        Get tableIdGet = new Get(RowKeyFactory.ROOT);
        Result result = table.get(tableIdGet);
        long tableId = ByteBuffer.wrap(result.getValue(Constants.NIC, tableName.getBytes())).getLong();

        TableInfo info = new TableInfo(tableName, tableId);

        byte[] rowKey = RowKeyFactory.buildColumnsKey(tableId);

        Get columnsGet = new Get(rowKey);
        Result columnsResult = table.get(columnsGet);
        Map<byte[], byte[]> columns = columnsResult.getFamilyMap(Constants.NIC);
        for (byte[] qualifier : columns.keySet()) {
            String columnName = new String(qualifier);
            long columnId = ByteBuffer.wrap(columns.get(qualifier)).getLong();
            info.addColumn(columnName, columnId, getMetadataForColumn(tableId, columnId));
        }

        return info;
    }

    private List<ColumnMetadata> getMetadataForColumn(long tableId, long columnId) throws IOException {
        ArrayList<ColumnMetadata> metadataList = new ArrayList<ColumnMetadata>();

        Get metadataGet = new Get(RowKeyFactory.buildColumnInfoKey(tableId, columnId));
        Result result = table.get(metadataGet);

        Map<byte[], byte[]> metadata = result.getFamilyMap(Constants.NIC);
        for (byte[] qualifier : metadata.keySet()) {
            // Only the qualifier matters for column metadata - value is not important
            String metadataString = new String(qualifier).toUpperCase();
            ColumnMetadata metaDataItem;

            try {
                metaDataItem = ColumnMetadata.valueOf(metadataString);
                metadataList.add(metaDataItem);
            } catch (IllegalArgumentException e) {

            }
        }

        return metadataList;
    }
}
