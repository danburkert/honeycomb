package com.nearinfinity.mysqlengine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 7/25/12
 * Time: 2:09 PM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseClient {
    private HTable table;

    private static final byte[] ROOT = ByteBuffer.allocate(7)
            .put(RowType.TABLES.getValue())
            .put("TABLES".getBytes()).array();

    private static final byte[] NIC = "nic".getBytes();

    private final ConcurrentHashMap<String, TableInfo> tableCache = new ConcurrentHashMap<String, TableInfo>();

    public HBaseClient(String tableName, String zkQuorum) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zkQuorum);

        try {
            this.table = new HTable(configuration, tableName);
        }
        catch(IOException e) {
            //TODO: handle this better
            e.printStackTrace();
        }
    }

    private void createTable(String tableName, List<Put> puts) throws IOException {
        //Get and increment the table counter (assumes it exists)
        long tableId = table.incrementColumnValue(ROOT, NIC, new byte[0], 1);

        //Add a row with the table name
        puts.add(new Put(ROOT).add(NIC, tableName.getBytes(), Bytes.toBytes(tableId)));

        //Cache the table
        tableCache.put(tableName, new TableInfo(tableName, tableId));
    }

    private void addColumns(String tableName, List<String> columns, List<Put> puts) throws IOException {
        //Get table id from cache
        long tableId = tableCache.get(tableName).getId();

        //Build the column row key
        byte[] columnBytes = ByteBuffer.allocate(9).put(RowType.COLUMNS.getValue()).putLong(tableId).array();

        //Allocate ids and compute start id
        long numColumns = columns.size();
        long lastColumnId = table.incrementColumnValue(columnBytes, NIC, new byte[0], numColumns);
        long startColumn = lastColumnId - numColumns;

        for (String columnName : columns) {
            long columnId = ++startColumn;

            //Add put
            Put columnPut = new Put(columnBytes).add(NIC, columnName.getBytes(), Bytes.toBytes(columnId));
            puts.add(columnPut);

            //Add to cache
            tableCache.get(tableName).addColumn(columnName, columnId);
        }
    }

    public void createTableFull(String tableName, List<String> columns) throws IOException {
        //Batch put list
        List<Put> putList = new LinkedList<Put>();

        //Create table and add to put list
        createTable(tableName, putList);

        //Create columns and add to put list
        addColumns(tableName, columns, putList);

        //Perform all puts
        table.put(putList);
    }

    public void writeRow(String tableName, Map<String, byte[]> values) throws IOException {
        //Get table id
        long tableId = getTableInfo(tableName).getId();

        //Get UUID for new entry
        UUID rowId = UUID.randomUUID();

        //Build data row key
        byte [] rowKey = ByteBuffer.allocate(25)
                .put(RowType.DATA.getValue())
                .putLong(tableId)
                .putLong(rowId.getLeastSignificantBits())
                .putLong(rowId.getMostSignificantBits())
                .array();

        //Create put list
        List<Put> putList = new LinkedList<Put>();

        Put rowPut = new Put(rowKey);

        for (String columnName : values.keySet()) {
            //Get column id and value
            long columnId = getTableInfo(tableName).getColumnIdByName(columnName);
            byte[] value = values.get(columnName);

            //Add column to put
            rowPut.add(NIC, Bytes.toBytes(columnId), value);

            //Build index key
            byte [] indexRow = ByteBuffer.allocate(33 + value.length)
                    .put(RowType.INDEX.getValue())
                    .putLong(tableId)
                    .putLong(columnId)
                    .put(value)
                    .putLong(rowId.getLeastSignificantBits())
                    .putLong(rowId.getMostSignificantBits())
                    .array();

            //Add the corresponding index
            putList.add(new Put(indexRow).add(NIC, new byte[0], new byte[0]));
        }

        //Add the row to put list
        putList.add(rowPut);

        //Final put
        table.put(putList);
    }

    public List<Map<String, byte[]>> fullTableScan(String tableName) throws IOException {
        //Get table id
        TableInfo info = getTableInfo(tableName);
        long tableId = info.getId();

        //Build row key
        byte[] startRow = ByteBuffer.allocate(25)
                .put(RowType.DATA.getValue())
                .putLong(tableId)
                .putLong(0L) /* UUID pt 1 */
                .putLong(0L) /* UUID pt 2 */
                .array();

        byte[] endRow = ByteBuffer.allocate(25)
                .put(RowType.DATA.getValue())
                .putLong(tableId+1)
                .putLong(0L) /* UUID pt 1 */
                .putLong(0L) /* UUID pt 2 */
                .array();

        Scan scan = new Scan(startRow, endRow);

        //Scan all rows in HBase
        List<Map<String, byte[]>> rows = new LinkedList<Map<String, byte[]>>();
        ResultScanner results = table.getScanner(scan);

        for (Result result : results) {
            Map<String, byte[]> columns = new HashMap<String, byte[]>();
            Map<byte[], byte[]> returnedColumns = result.getNoVersionMap().get(NIC);
            for (byte[] qualifier : returnedColumns.keySet()) {
                long columnId = ByteBuffer.wrap(qualifier).getLong();
                String columnName = info.getColumnNameById(columnId);
                columns.put(columnName, returnedColumns.get(qualifier));
            }
            rows.add(columns);
        }

        return rows;
    }

    public ResultScanner getTableScanner(String tableName) throws IOException {
        //Get table id
        TableInfo info = getTableInfo(tableName);
        long tableId = info.getId();

        //Build row keys
        byte[] startRow = ByteBuffer.allocate(25)
                .put(RowType.DATA.getValue())
                .putLong(tableId)
                .putLong(0L) /* UUID pt 1 */
                .putLong(0L) /* UUID pt 2 */
                .array();

        byte[] endRow = ByteBuffer.allocate(25)
                .put(RowType.DATA.getValue())
                .putLong(tableId+1)
                .putLong(0L) /* UUID pt 1 */
                .putLong(0L) /* UUID pt 2 */
                .array();

        Scan scan = new Scan(startRow, endRow);

        return table.getScanner(scan);
    }

    private TableInfo getTableInfo(String tableName) throws IOException {
        if (tableCache.containsKey(tableName)) {
            return tableCache.get(tableName);
        }

        //Get the table id from HBase
        Get tableIdGet = new Get(ROOT);
        Result result = table.get(tableIdGet);
        long tableId = ByteBuffer.wrap(result.getValue(NIC, tableName.getBytes())).getLong();

        TableInfo info = new TableInfo(tableName, tableId);

        byte[] rowKey = ByteBuffer.allocate(9)
                .put(RowType.COLUMNS.getValue())
                .putLong(tableId)
                .array();

        Get columnsGet = new Get(rowKey);
        Result columnsResult = table.get(columnsGet);
        Map<byte[], byte[]> columns = columnsResult.getFamilyMap(NIC);
        for (byte[] qualifier : columns.keySet()) {
            String columnName = new String(qualifier);
            long columnId = ByteBuffer.wrap(columns.get(qualifier)).getLong();
            info.addColumn(columnName, columnId);
        }

        return info;
    }

    public Map<String, byte[]> parseRow(Result result, String tableName) throws IOException {
        TableInfo info = getTableInfo(tableName);

        Map<String, byte[]> columns = new HashMap<String, byte[]>();
        Map<byte[], byte[]> returnedColumns = result.getNoVersionMap().get(NIC);

        for (byte[] qualifier : returnedColumns.keySet()) {
            long columnId = ByteBuffer.wrap(qualifier).getLong();
            String columnName = info.getColumnNameById(columnId);
            columns.put(columnName, returnedColumns.get(qualifier));
        }

        return columns;
    }
}