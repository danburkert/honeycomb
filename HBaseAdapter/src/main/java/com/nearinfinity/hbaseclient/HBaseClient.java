package com.nearinfinity.hbaseclient;

import com.nearinfinity.hbaseclient.strategy.ScanStrategy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class HBaseClient {
    private HTable table;

    private HBaseAdmin admin;

    private final ConcurrentHashMap<String, TableInfo> tableCache = new ConcurrentHashMap<String, TableInfo>();

    private static final Logger logger = Logger.getLogger(HBaseClient.class);

    public HBaseClient(String tableName, String zkQuorum) {
        logger.info("HBaseClient: Constructing with HBase table name: " + tableName);
        logger.info("HBaseClient: Constructing with ZK Quorum: " + zkQuorum);

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zkQuorum);

        try {
            this.admin = new HBaseAdmin(configuration);
            this.initializeSqlTable();

            this.table = new HTable(configuration, tableName);

        } catch (MasterNotRunningException e) {
            logger.error("MasterNotRunningException thrown", e);
        } catch (ZooKeeperConnectionException e) {
            logger.error("ZooKeeperConnectionException thrown", e);
        } catch (IOException e) {
            logger.error("IOException thrown", e);
        } catch (InterruptedException e) {
            logger.error("InterruptedException thrown", e);
        }
    }

    private void initializeSqlTable() throws IOException, InterruptedException {
        HTableDescriptor sqlTableDescriptor;
        HColumnDescriptor nicColumn = new HColumnDescriptor(Constants.NIC);

        if (!this.admin.tableExists(Constants.SQL)) {
            logger.info("Creating sql table");
            sqlTableDescriptor = new HTableDescriptor(Constants.SQL);
            sqlTableDescriptor.addFamily(nicColumn);

            this.admin.createTable(sqlTableDescriptor);
        }

        sqlTableDescriptor = this.admin.getTableDescriptor(Constants.SQL);
        if (!sqlTableDescriptor.hasFamily(Constants.NIC)) {
            logger.info("Adding nic column family to sql table");

            if (!this.admin.isTableDisabled(Constants.SQL)) {
                logger.info("Disabling sql table");
                this.admin.disableTable(Constants.SQL);
            }

            this.admin.addColumn(Constants.SQL, nicColumn);
        }

        if (this.admin.isTableDisabled(Constants.SQL)) {
            logger.info("Enabling sql table");
            this.admin.enableTable(Constants.SQL);
        }

        this.admin.flush(Constants.SQL);
    }

    private void createTable(String tableName, List<Put> puts) throws IOException {
        long tableId = table.incrementColumnValue(RowKeyFactory.ROOT, Constants.NIC, new byte[0], 1);
        tableCache.put(tableName, new TableInfo(tableName, tableId));

        puts.add(new Put(RowKeyFactory.ROOT).add(Constants.NIC, tableName.getBytes(), Bytes.toBytes(tableId)));
        puts.add(new Put(RowKeyFactory.buildTableInfoKey(tableId)).add(Constants.NIC, Constants.ROW_COUNT, Bytes.toBytes(0l)));
    }

    private void addColumns(String tableName, Map<String, ColumnMetadata> columns, List<Put> puts) throws IOException {
        //Get table id from cache
        long tableId = tableCache.get(tableName).getId();

        //Build the column row key
        byte[] columnBytes = ByteBuffer.allocate(9).put(RowType.COLUMNS.getValue()).putLong(tableId).array();

        //Allocate ids and compute start id
        long numColumns = columns.size();
        long lastColumnId = table.incrementColumnValue(columnBytes, Constants.NIC, new byte[0], numColumns);
        long startColumn = lastColumnId - numColumns;

        for (String columnName : columns.keySet()) {
            long columnId = ++startColumn;

            //Add column
            Put columnPut = new Put(columnBytes).add(Constants.NIC, columnName.getBytes(), Bytes.toBytes(columnId));
            puts.add(columnPut);

            // Add column metadata
            byte[] columnInfoBytes = RowKeyFactory.buildColumnInfoKey(tableId, columnId);
            Put columnInfoPut = new Put(columnInfoBytes);

            ColumnMetadata metadata = columns.get(columnName);

            columnInfoPut.add(Constants.NIC, Constants.METADATA, metadata.toJson());

            puts.add(columnInfoPut);

            //Add to cache
            tableCache.get(tableName).addColumn(columnName, columnId, columns.get(columnName));
        }
    }

    public void createTableFull(String tableName, Map<String, ColumnMetadata> columns) throws IOException {
        //Batch put list
        List<Put> putList = new LinkedList<Put>();

        createTable(tableName, putList);

        addColumns(tableName, columns, putList);

        this.table.put(putList);

        this.table.flushCommits();
    }

    public void writeRow(String tableName, Map<String, byte[]> values) throws IOException {
        TableInfo info = getTableInfo(tableName);
        List<Put> putList = PutListFactory.createPutList(values, info);

        //Final put
        this.table.put(putList);
    }

    public Result getDataRow(UUID uuid, String tableName) throws IOException {
        TableInfo info = getTableInfo(tableName);
        long tableId = info.getId();

        byte[] rowKey = RowKeyFactory.buildDataKey(tableId, uuid);

        Get get = new Get(rowKey);
        return table.get(get);
    }

    public TableInfo getTableInfo(String tableName) throws IOException {
        if (tableCache.containsKey(tableName)) {
            return tableCache.get(tableName);
        }

        //Get the table id from HBase
        Get tableIdGet = new Get(RowKeyFactory.ROOT);
        Result result = table.get(tableIdGet);
        if (result.isEmpty()) {
            throw new TableNotFoundException(tableName + " was not found.");
        }

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

        tableCache.put(tableName, info);

        return info;
    }

    public void renameTable(String from, String to) throws IOException {
        logger.info("Renaming table " + from + " to " + to);

        TableInfo info = tableCache.get(from);

        byte[] rowKey = RowKeyFactory.ROOT;

        Delete oldNameDelete = new Delete(rowKey);

        oldNameDelete.deleteColumn(Constants.NIC, from.getBytes());

        this.table.delete(oldNameDelete);

        Put nameChangePut = new Put(rowKey);
        nameChangePut.add(Constants.NIC, to.getBytes(), Bytes.toBytes(info.getId()));

        this.table.put(nameChangePut);

        info.setName(to);

        tableCache.remove(from);
        tableCache.put(to, info);

        logger.info("Rename complete!");
    }

    public ColumnMetadata getMetadataForColumn(long tableId, long columnId) throws IOException {
        Get metadataGet = new Get(RowKeyFactory.buildColumnInfoKey(tableId, columnId));
        Result result = table.get(metadataGet);

        byte[] jsonBytes = result.getValue(Constants.NIC, Constants.METADATA);
        return new ColumnMetadata(jsonBytes);
    }

    public boolean deleteRow(String tableName, UUID uuid) throws IOException {
        if (uuid == null) {
            return false;
        }

        TableInfo info = getTableInfo(tableName);
        long tableId = info.getId();

        byte[] dataRowKey = RowKeyFactory.buildDataKey(tableId, uuid);
        Get get = new Get(dataRowKey);
        Result result = table.get(get);

        List<Delete> deleteList = DeleteListFactory.createDeleteRowList(uuid, info, result, dataRowKey);

        table.delete(deleteList);

        return true;
    }

    public boolean dropTable(String tableName) throws IOException {
        logger.info("Preparing to drop table " + tableName);
        TableInfo info = getTableInfo(tableName);
        long tableId = info.getId();

        deleteIndexRows(tableId);
        deleteDataRows(tableId);
        deleteColumnInfoRows(info);
        deleteColumns(tableId);
        deleteTableInfoRows(tableId);
        deleteTableFromRoot(tableName);

        logger.info("Table " + tableName + " is no more!");

        return true;
    }

    public int deleteAllRows(String tableName) throws IOException {
        long tableId = getTableInfo(tableName).getId();

        logger.info("Deleting all rows from table " + tableName + " with tableId " + tableId);

        deleteIndexRows(tableId);

        return deleteDataRows(tableId);
    }

    private int deleteTableInfoRows(long tableId) throws IOException {
        byte[] prefix = ByteBuffer.allocate(9).put(RowType.TABLE_INFO.getValue()).putLong(tableId).array();
        return deleteRowsWithPrefix(prefix);
    }

    private int deleteDataRows(long tableId) throws IOException {
        logger.info("Deleting all data rows");
        byte[] prefix = ByteBuffer.allocate(9).put(RowType.DATA.getValue()).putLong(tableId).array();
        return deleteRowsWithPrefix(prefix);
    }

    private int deleteColumns(long tableId) throws IOException {
        logger.info("Deleting all columns");
        byte[] prefix = ByteBuffer.allocate(9).put(RowType.COLUMNS.getValue()).putLong(tableId).array();
        return deleteRowsWithPrefix(prefix);
    }

    private int deleteIndexRows(long tableId) throws IOException {
        logger.info("Deleting all index rows");

        int affectedRows = 0;

        byte[] valuePrefix = ByteBuffer.allocate(9).put(RowType.PRIMARY_INDEX.getValue()).putLong(tableId).array();
        byte[] reversePrefix = ByteBuffer.allocate(9).put(RowType.REVERSE_INDEX.getValue()).putLong(tableId).array();
        byte[] nullPrefix = ByteBuffer.allocate(9).put(RowType.NULL_INDEX.getValue()).putLong(tableId).array();

        affectedRows += deleteRowsWithPrefix(valuePrefix);
        affectedRows += deleteRowsWithPrefix(reversePrefix);
        affectedRows += deleteRowsWithPrefix(nullPrefix);

        return affectedRows;
    }

    private int deleteColumnInfoRows(TableInfo info) throws IOException {
        logger.info("Deleting all column metadata rows");

        long tableId = info.getId();
        int affectedRows = 0;

        for (Long columnId : info.getColumnIds()) {
            byte[] metadataKey = RowKeyFactory.buildColumnInfoKey(tableId, columnId);
            affectedRows += deleteRowsWithPrefix(metadataKey);
        }

        return affectedRows;
    }

    private int deleteRowsWithPrefix(byte[] prefix) throws IOException {
        Scan scan = ScanFactory.buildScan();
        PrefixFilter filter = new PrefixFilter(prefix);
        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);
        List<Delete> deleteList = new LinkedList<Delete>();
        int count = 0;

        for (Result result : scanner) {
            //Delete the data row key
            byte[] rowKey = result.getRow();
            Delete rowDelete = new Delete(rowKey);
            deleteList.add(rowDelete);

            ++count;
        }

        table.delete(deleteList);

        return count;
    }

    public void incrementRowCount(String tableName, long delta) throws IOException {
        long tableId = getTableInfo(tableName).getId();
        byte[] rowKey = RowKeyFactory.buildTableInfoKey(tableId);
        table.incrementColumnValue(rowKey, Constants.NIC, Constants.ROW_COUNT, delta);
    }

    public void setRowCount(String tableName, long value) throws IOException {
        long tableId = getTableInfo(tableName).getId();
        Put put = new Put(RowKeyFactory.buildTableInfoKey(tableId)).add(Constants.NIC, Constants.ROW_COUNT, Bytes.toBytes(value));
        table.put(put);
    }

    public long getRowCount(String tableName) throws IOException {
        long tableId = getTableInfo(tableName).getId();
        byte[] rowKey = RowKeyFactory.buildTableInfoKey(tableId);
        return table.incrementColumnValue(rowKey, Constants.NIC, Constants.ROW_COUNT, 0);
    }

    public void deleteTableFromRoot(String tableName) throws IOException {
        Delete delete = new Delete((RowKeyFactory.ROOT));
        delete.deleteColumns(Constants.NIC, tableName.getBytes());

        table.delete(delete);
    }

    public void setCacheSize(int cacheSize) {
        logger.info("Setting table scan row cache to " + cacheSize);
        ScanFactory.setCacheAmount(cacheSize);
    }

    public void setAutoFlushTables(boolean shouldFlushChangesImmediately) {
        this.table.setAutoFlush(shouldFlushChangesImmediately);

        logger.info(shouldFlushChangesImmediately
                ? "Changes to tables will be written to HBase immediately"
                : "Changes to tables will be written to HBase when the write buffer has become full");
    }

    public void setWriteBufferSize(long numBytes) {
        try {
            this.table.setWriteBufferSize(numBytes);
        } catch (IOException e) {
            logger.error("Encountered an error setting write buffer size", e);
        }

        logger.info("Size of HBase write buffer set to " + numBytes + " bytes (" + (numBytes / 1024 / 1024) + " megabytes)");
    }

    public void flushWrites() {
        try {
            table.flushCommits();
        } catch (IOException e) {
            logger.error("Encountered an exception while flushing commits : ", e);
        }
    }

    public ResultScanner getScanner(ScanStrategy strategy) throws IOException {
        TableInfo info = getTableInfo(strategy.getTableName());

        Scan scan = strategy.getScan(info);

        return table.getScanner(scan);
    }
}
