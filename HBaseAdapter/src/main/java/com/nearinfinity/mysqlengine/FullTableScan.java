package com.nearinfinity.mysqlengine;

import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class FullTableScan {
    private static final Logger logger = Logger.getLogger(FullTableScan.class);
    private final ScanFactory scanFactory;
    private final RowKeyFactory rowKeyFactory;
    private final TableCache tableCache;
    private final HTableInterface table;

    public FullTableScan(ScanFactory scanFactory, RowKeyFactory rowKeyFactory, TableCache tableCache, HTableInterface table)
    {
        this.scanFactory = scanFactory;
        this.rowKeyFactory = rowKeyFactory;
        this.tableCache = tableCache;
        this.table = table;
    }

   public List<Map<String, byte[]>> fullTableScan(String tableName) throws IOException {
        logger.info("HBaseClient.fullTableScan");

        //Get table id
        TableInfo info = tableCache.getTableInfo(tableName);
        long tableId = info.getId();

        //Build row keys
        byte[] startRow = this.rowKeyFactory.buildDataKey(tableId, Constants.ZERO_UUID);
        byte[] endRow = this.rowKeyFactory.buildDataKey(tableId + 1, Constants.ZERO_UUID);

        Scan scan = this.scanFactory.buildScan(startRow, endRow);

        //Scan all rows in HBase
        List<Map<String, byte[]>> rows = new LinkedList<Map<String, byte[]>>();
        ResultScanner results = table.getScanner(scan);

        for (Result result : results) {
            Map<String, byte[]> columns = new HashMap<String, byte[]>();
            Map<byte[], byte[]> returnedColumns = result.getNoVersionMap().get(Constants.NIC);
            for (byte[] qualifier : returnedColumns.keySet()) {
                long columnId = ByteBuffer.wrap(qualifier).getLong();
                String columnName = info.getColumnNameById(columnId);
                columns.put(columnName, returnedColumns.get(qualifier));
            }
            rows.add(columns);
        }

        return rows;
    }}
