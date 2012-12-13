package com.nearinfinity.honeycomb.hbaseclient;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.nearinfinity.honeycomb.hbaseclient.strategy.PrefixScanStrategy;
import com.nearinfinity.honeycomb.hbaseclient.strategy.ScanStrategy;
import com.nearinfinity.honeycomb.hbaseclient.strategy.ScanStrategyInfo;
import com.nearinfinity.honeycomb.mysqlengine.HBaseResultScanner;
import com.nearinfinity.honeycomb.mysqlengine.SingleResultScanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class HBaseClient {
    private HTable table;

    private HBaseAdmin admin;

    private static final ConcurrentHashMap<String, TableInfo> tableCache = new ConcurrentHashMap<String, TableInfo>();

    private static final Logger logger = Logger.getLogger(HBaseClient.class);

    private static final ReadWriteLock cacheLock = new ReentrantReadWriteLock();

    public HBaseClient(String tableName, String zkQuorum) throws IOException {
        checkNotNull(tableName);
        checkNotNull(zkQuorum);
        logger.info("Constructing with HBase table name: " + tableName);
        logger.info("Constructing with ZK Quorum: " + zkQuorum);

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zkQuorum);

        this.admin = new HBaseAdmin(configuration);
        this.initializeSqlTable();
        logger.info("Sql table successfully initialized.");

        this.table = new HTable(configuration, tableName);
        logger.info("HTable successfully created.");
    }

    private void initializeSqlTable() throws IOException {
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

        try {
            this.admin.flush(Constants.SQL);
        } catch (InterruptedException e) {
            logger.warn("HBaseAdmin flush was interrupted. Retrying.");
            try {
                this.admin.flush(Constants.SQL);
            } catch (InterruptedException e1) {
                throw new RuntimeException(e1);
            }
        }
    }

    private void createTable(String tableName, List<Put> puts,
                             TableMultipartKeys multipartKeys)
            throws IOException {
        long tableId = table.incrementColumnValue(RowKeyFactory.ROOT, Constants.NIC, new byte[0], 1);
        tableCache.put(tableName, new TableInfo(tableName, tableId));

        puts.add(new Put(RowKeyFactory.ROOT).add(Constants.NIC, tableName.getBytes(), Bytes.toBytes(tableId)));
        Put put = new Put(RowKeyFactory.buildTableInfoKey(tableId));
        put.add(Constants.NIC, Constants.ROW_COUNT, Bytes.toBytes(0l));
        final byte[] indexBytes = multipartKeys.toJson();
        final byte[] uniqueKeyBytes = multipartKeys.uniqueKeysToJson();
        updateTableCacheIndex(tableName, new HashMap<String, byte[]>() {{
            put(Constants.INDEXES_STRING, indexBytes);
            put(Constants.UNIQUE_STRING, uniqueKeyBytes);
        }});
        put.add(Constants.NIC, Constants.INDEXES, indexBytes);
        put.add(Constants.NIC, Constants.UNIQUES, uniqueKeyBytes);
        puts.add(put);
    }

    private void updateTableCacheIndex(String tableName, Map<String, byte[]> map) throws IOException {
        getTableInfo(tableName).setTableMetadata(map);
    }

    private void addColumns(String tableName, Map<String, ColumnMetadata> columns, List<Put> puts) throws IOException {
        TableInfo tableInfo = getTableInfo(tableName);
        long tableId = tableInfo.getId();

        byte[] columnBytes = ByteBuffer.allocate(9).put(RowType.COLUMNS.getValue()).putLong(tableId).array();

        long numColumns = columns.size();
        long lastColumnId = table.incrementColumnValue(columnBytes, Constants.NIC, new byte[0], numColumns);
        long startColumn = lastColumnId - numColumns;

        for (String columnName : columns.keySet()) {
            long columnId = ++startColumn;

            Put columnPut = new Put(columnBytes).add(Constants.NIC, columnName.getBytes(), Bytes.toBytes(columnId));
            puts.add(columnPut);

            byte[] columnInfoBytes = RowKeyFactory.buildColumnInfoKey(tableId, columnId);
            Put columnInfoPut = new Put(columnInfoBytes);

            ColumnMetadata metadata = columns.get(columnName);

            columnInfoPut.add(Constants.NIC, Constants.METADATA, metadata.toJson());
            if (metadata.isAutoincrement()) {
                columnInfoPut.add(Constants.NIC, new byte[0], Bytes.toBytes(0L));
            }

            puts.add(columnInfoPut);

            tableInfo.addColumn(columnName, columnId, columns.get(columnName));
        }
    }

    public void createTableFull(String tableName, Map<String,
            ColumnMetadata> columns, TableMultipartKeys multipartKeys)
            throws IOException {
        List<Put> putList = new LinkedList<Put>();

        createTable(tableName, putList, multipartKeys);

        addColumns(tableName, columns, putList);

        this.table.put(putList);

        this.table.flushCommits();
    }

    public long getAutoincrementValue(String tableName, String fieldName) throws IOException {
        TableInfo tableInfo = getTableInfo(tableName);
        ColumnMetadata columnMetadata = tableInfo.getColumnMetadata(fieldName);

        logger.info("isAutoincrement:" + columnMetadata.isAutoincrement() + ", autoincrementValue:" + columnMetadata.getAutoincrementValue());

        if (columnMetadata.isAutoincrement() == false)
            return -1L;
        else
            return columnMetadata.getAutoincrementValue();
    }

    public boolean alterAutoincrementValue(String tableName, String fieldName, long autoincrementValue, boolean isTruncate) throws IOException {
        TableInfo info = getTableInfo(tableName);
        long columnId = info.getColumnIdByName(fieldName);
        long tableId = info.getId();
        byte[] columnInfoBytes = RowKeyFactory.buildColumnInfoKey(tableId, columnId);

        Get get = new Get(columnInfoBytes);
        Result result = table.get(get);

        long currentValue = Bytes.toLong(result.getValue(Constants.NIC, new byte[0]));
        ColumnMetadata metadata = new ColumnMetadata(result.getValue(Constants.NIC, Constants.METADATA));

        // only set the new autoincrement value if it is greater than the
        // current autoincrement value or if the autoincrement value is being reset in a truncate command
        if (autoincrementValue > currentValue || isTruncate) {
            metadata.setAutoincrement(true);
            metadata.setAutoincrementValue(autoincrementValue);
        } else {
            logger.info(format("The new auto_increment value of %d is less than the current count of %d, so this command will be ignored.", autoincrementValue, currentValue));
            return false;
        }

        Put columnInfoPut = new Put(columnInfoBytes);
        columnInfoPut.add(Constants.NIC, Constants.METADATA, metadata.toJson());
        columnInfoPut.add(Constants.NIC, new byte[0], Bytes.toBytes(autoincrementValue));

        table.put(columnInfoPut);
        table.flushCommits();

        tableCache.get(tableName).setColumnMetadata(fieldName, metadata);

        return true;
    }

    public void writeRow(String tableName, Map<String, byte[]> values) throws IOException {
        TableInfo info = getTableInfo(tableName);
        List<List<String>> multipartIndex = Index.indexForTable(info.tableMetadata());
        List<Put> putList = PutListFactory.createDataInsertPutList(values, info, multipartIndex);
        this.table.put(putList);

        // Very special case for alter table with data in it. MySQL creates a temp table in the form #sql-XXXX_X
        // and starts to put data in it. When a unique index is on the new table duplicates appear because the data is
        // not flushed to HBase.
        if (info.getTableName().startsWith("#sql-")) {
            this.table.flushCommits();
        }
    }

    public Result getDataRow(UUID uuid, String tableName) throws IOException {
        TableInfo info = getTableInfo(tableName);
        long tableId = info.getId();

        byte[] rowKey = RowKeyFactory.buildDataKey(tableId, uuid);

        Get get = new Get(rowKey);
        return table.get(get);
    }

    public TableInfo getTableInfo(String tableName) throws IOException {
        checkNotNull(tableName);
        cacheLock.readLock().lock();
        try {
            TableInfo tableInfo = tableCache.get(tableName);
            if (tableInfo != null) {
                return tableInfo;
            }
        } finally {
            cacheLock.readLock().unlock();
        }

        cacheLock.writeLock().lock();
        try {
            return TableCache.refreshCache(tableName, table, tableCache);
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    public void renameTable(String from, String to) throws IOException {
        checkNotNull(from);
        checkNotNull(to);
        logger.info("Renaming table " + from + " to " + to);

        TableInfo info = getTableInfo(from);

        byte[] rowKey = RowKeyFactory.ROOT;

        Delete oldNameDelete = new Delete(rowKey);

        oldNameDelete.deleteColumn(Constants.NIC, from.getBytes());

        this.table.delete(oldNameDelete);

        Put nameChangePut = new Put(rowKey);
        nameChangePut.add(Constants.NIC, to.getBytes(), Bytes.toBytes(info.getId()));

        this.table.put(nameChangePut);
        this.table.flushCommits();

        info.setName(to);

        cacheLock.writeLock().lock();
        try {
            tableCache.remove(from);
            tableCache.put(to, info);
        } finally {
            cacheLock.writeLock().unlock();
        }

        logger.info("Rename complete!");
    }

    public void updateRow(UUID uuid, List<String> changedFields, String tableName, Map<String, byte[]> newValues) throws IOException {
        Map<String, byte[]> oldRow = retrieveRowAndDelete(tableName, uuid);

        for (String changedField : changedFields) {
            oldRow.put(changedField, newValues.get(changedField)); // Hack around MySQL setting field->is_null when actually not.
        }

        TableInfo info = getTableInfo(tableName);
        Set<String> columnNames = info.getColumnNames();
        for (String columnName : columnNames) {
            if (!oldRow.containsKey(columnName)) {
                oldRow.put(columnName, null); // Nulls need to be transferred otherwise writeRow loses them.
            }
        }
        writeRow(tableName, oldRow);
    }

    public boolean deleteRow(String tableName, UUID uuid) throws IOException {
        if (uuid == null) {
            return false;
        }

        retrieveRowAndDelete(tableName, uuid);

        return true;
    }

    private Map<String, byte[]> retrieveRowAndDelete(String tableName, UUID uuid) throws IOException {
        TableInfo info = getTableInfo(tableName);
        long tableId = info.getId();

        byte[] dataRowKey = RowKeyFactory.buildDataKey(tableId, uuid);
        Get get = new Get(dataRowKey);
        Result result = table.get(get);
        Map<String, byte[]> oldRow = ResultParser.parseDataRow(result, info);

        List<Delete> deleteList = DeleteListFactory.createDeleteRowList(uuid, info, result, dataRowKey, Index.indexForTable(info.tableMetadata()));

        table.delete(deleteList);
        incrementRowCount(tableName, -1);

        return oldRow;
    }

    public boolean dropTable(String tableName) throws IOException {
        logger.info("Preparing to drop table " + tableName);
        TableInfo info = getTableInfo(tableName);
        long tableId = info.getId();
        deleteAllRowsInTable(info);

        deleteColumnInfoRows(info);
        deleteColumns(tableId);
        deleteTableInfoRows(tableId);
        deleteTableFromRoot(tableName);

        logger.info("Table " + tableName + " is no more!");

        return true;
    }

    public int deleteAllRowsInTable(String tableName) throws IOException {
        TableInfo info = getTableInfo(tableName);
        long tableId = info.getId();

        logger.info("Deleting all rows from table " + tableName + " with tableId " + tableId);

        deleteAllRowsInTable(info);
        return 0;
    }

    private void deleteAllRowsInTable(TableInfo info) throws IOException {
        long tableId = info.getId();
        byte[] prefix = ByteBuffer.allocate(9).put(RowType.DATA.getValue()).putLong(tableId).array();
        Scan scan = ScanFactory.buildScan();
        PrefixFilter filter = new PrefixFilter(prefix);
        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);
        List<List<String>> indexedKeys = Index.indexForTable(info.tableMetadata());
        List<Delete> deleteList = new LinkedList<Delete>();
        for (Result result : scanner) {
            UUID uuid = ResultParser.parseUUID(result);
            byte[] rowKey = result.getRow();
            deleteList.addAll(DeleteListFactory.createDeleteRowList(uuid, info, result, rowKey, indexedKeys));
        }
        table.delete(deleteList);
    }

    private int deleteTableInfoRows(long tableId) throws IOException {
        byte[] prefix = ByteBuffer.allocate(9).put(RowType.TABLE_INFO.getValue()).putLong(tableId).array();
        return deleteRowsWithPrefix(prefix);
    }

    private int deleteColumns(long tableId) throws IOException {
        logger.info("Deleting all columns");
        byte[] prefix = ByteBuffer.allocate(9).put(RowType.COLUMNS.getValue()).putLong(tableId).array();
        return deleteRowsWithPrefix(prefix);
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
        TableInfo tableInfo = getTableInfo(tableName);
        long tableId = tableInfo.getId();
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

    private <K, V> Map<K, V> selectKeys(Map<K, V> map, Iterable<K> keys) {
        Map<K, V> subset = new HashMap<K, V>();
        for (K key : keys) {
            V value = map.get(key);
            if (map.containsKey(key) && value != null) {
                subset.put(key, value);
            }
        }

        return subset;
    }

    public String findDuplicateKeyOnUpdate(final String tableName, final Map<String, byte[]> values, List<String> changedColumns) throws IOException {
        if (values.isEmpty()) {
            return null;
        }

        Map<String, byte[]> subMap = selectKeys(values, changedColumns);
        return findDuplicateKey(tableName, subMap);
    }

    public String findDuplicateKey(final String tableName, final Map<String, byte[]> values) throws IOException {
        TableInfo info = getTableInfo(tableName);
        List<List<String>> allUniqueKeys = Index.uniqueKeysForTable(info.tableMetadata());
        for (List<String> uniqueKeySet : allUniqueKeys) {
            Map<String, byte[]> subKeyMap = selectKeys(values, uniqueKeySet);
            if (subKeyMap.size() == 0) {
                continue;
            }

            String duplicateFound = doFindDuplicateKey(tableName, subKeyMap);
            if (duplicateFound != null) {
                return duplicateFound;
            }
        }

        return null;
    }

    private String doFindDuplicateKey(String tableName, Map<String, byte[]> values) throws IOException {
        ScanStrategyInfo scanInfo = new ScanStrategyInfo(tableName, values.keySet(), valueMapToKeyValues(tableName, values));
        PrefixScanStrategy strategy = new PrefixScanStrategy(scanInfo);

        HBaseResultScanner scanner = new SingleResultScanner(getScanner(strategy));

        if (scanner.next(null) != null) {
            return Joiner.on(",").join(scanInfo.columnNames());
        }

        return null;
    }

    public byte[] findDuplicateValue(String tableName, String columnNameStrings) throws IOException {
        TableInfo info = getTableInfo(tableName);
        List<String> columnNames = Arrays.asList(columnNameStrings.split(","));

        Scan scan = ScanFactory.buildScan();
        byte[] prefix = ByteBuffer.allocate(9).put(RowType.DATA.getValue()).putLong(info.getId()).array();
        PrefixFilter prefixFilter = new PrefixFilter(prefix);

        List<byte[]> columnIds = new LinkedList<byte[]>();
        for (String columnName : columnNames) {
            byte[] columnIdBytes = Bytes.toBytes(info.getColumnIdByName(columnName));
            columnIds.add(columnIdBytes);
            scan.addColumn(Constants.NIC, columnIdBytes);
        }

        scan.setFilter(prefixFilter);

        Set<ByteBuffer> columnValues = new HashSet<ByteBuffer>();

        ResultScanner scanner = this.table.getScanner(scan);
        Result result;

        while ((result = scanner.next()) != null) {
            List<byte[]> values = new LinkedList<byte[]>();
            int size = 0;
            for (byte[] columnIdBytes : columnIds) {
                byte[] value = result.getValue(Constants.NIC, columnIdBytes);
                values.add(value);
                size += value.length;
            }
            ByteBuffer value = ByteBuffer.wrap(Util.mergeByteArrays(values, size));

            if (columnValues.contains(value)) {
                return value.array();
            }

            columnValues.add(value);
        }

        return null;
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

    public long getNextAutoincrementValue(String tableName, String columnName) throws IOException {
        long nextAutoincrementValue = getTableInfo(tableName).getColumnMetadata(columnName).getAutoincrementValue();
        alterAutoincrementValue(tableName, columnName, nextAutoincrementValue + 1, false);

        return nextAutoincrementValue;
    }

    public void setupKeyValues(String tableName, List<String> columnName, List<KeyValue> keyValues, byte fill) throws IOException {
        TableInfo info = getTableInfo(tableName);
        for (String column : columnName) {
            ColumnMetadata metadata = info.getColumnMetadata(column);
            byte[] value = new byte[metadata.getMaxLength()];
            Arrays.fill(value, fill);
            keyValues.add(new KeyValue(column, value, metadata.isNullable(), false));
        }
    }

    public boolean isNullable(String tableName, String columnName) throws IOException {
        TableInfo info = getTableInfo(tableName);
        return info.getColumnMetadata(columnName).isNullable();
    }

    private List<KeyValue> valueMapToKeyValues(String tableName, Map<String, byte[]> valueMap) throws IOException {
        TableInfo info = getTableInfo(tableName);
        List<KeyValue> keyValues = new LinkedList<KeyValue>();
        for (Map.Entry<String, byte[]> entry : valueMap.entrySet()) {
            String key = entry.getKey();
            ColumnMetadata metadata = info.getColumnMetadata(key);
            byte[] value = entry.getValue();
            keyValues.add(new KeyValue(key, value, metadata.isNullable(), value == null));
        }

        return keyValues;
    }

    public void addIndex(String tableName, TableMultipartKeys columnString) throws IOException {
        final List<String> columnsToIndex = columnString.indexKeys().get(0);
        final List<List<String>> uniqueColumns = columnString.uniqueKeys();
        final TableInfo info = getTableInfo(tableName);
        updateIndexEntryToMetadata(info, new IndexFunction<List<List<String>>, Boolean, Void>() {
            @Override
            public Void apply(List<List<String>> index, Boolean isIndex) {
                if (!isIndex) {
                    if (uniqueColumns.size() > 0) {
                        index.add(uniqueColumns.get(0));
                    }

                    return null;
                }

                index.add(columnsToIndex);
                return null;
            }
        });

        changeIndex(info, new IndexFunction<Map<String, byte[]>, UUID, Void>() {
            @Override
            public Void apply(Map<String, byte[]> values, UUID uuid) {
                List<Put> puts = PutListFactory.createIndexForColumns(values, info, uuid, columnsToIndex);
                try {
                    table.put(puts);
                } catch (IOException e) {
                    e.printStackTrace();    // TODO: Bad way to handle an exception.
                }

                return null;
            }
        });
    }

    public void dropIndex(String tableName, String indexToDrop) throws IOException {
        final List<String> indexColumns = Arrays.asList(indexToDrop.split(","));
        final TableInfo info = getTableInfo(tableName);
        updateIndexEntryToMetadata(info, new IndexFunction<List<List<String>>, Boolean, Void>() {
            @Override
            public Void apply(List<List<String>> index, Boolean isIndex) {
                index.remove(indexColumns);
                return null;
            }
        });

        changeIndex(info, new IndexFunction<Map<String, byte[]>, UUID, Void>() {
            @Override
            public Void apply(Map<String, byte[]> values, UUID rowId) {
                List<Delete> deletes = DeleteListFactory.createDeleteForIndex(values, info, rowId, indexColumns);
                try {
                    table.delete(deletes);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return null;
            }
        });
    }

    private void updateIndexEntryToMetadata(TableInfo info, IndexFunction<List<List<String>>, Boolean, Void> updateFunc) throws IOException {
        final String tableName = info.getName();
        final long tableId = info.getId();
        Put indexUpdate = new Put(RowKeyFactory.buildTableInfoKey(tableId));
        HashMap<String, byte[]> map = new HashMap<String, byte[]>();

        List<List<String>> index = Index.indexForTable(info.tableMetadata());
        updateFunc.apply(index, true);
        final byte[] bytes = TableMultipartKeys.indexJson(index);
        indexUpdate.add(Constants.NIC, Constants.INDEXES, bytes);
        map.put(Constants.INDEXES_STRING, bytes);

        List<List<String>> uniqueKeys = Index.uniqueKeysForTable(info.tableMetadata());
        updateFunc.apply(uniqueKeys, false);
        final byte[] uniqueKeyBytes = TableMultipartKeys.indexJson(uniqueKeys);
        indexUpdate.add(Constants.NIC, Constants.UNIQUES, uniqueKeyBytes);
        map.put(Constants.UNIQUE_STRING, uniqueKeyBytes);

        updateTableCacheIndex(tableName, map);

        this.table.put(indexUpdate);
        this.table.flushCommits();
    }

    private void changeIndex(TableInfo info, IndexFunction<Map<String, byte[]>, UUID, Void> function) throws IOException {
        final long tableId = info.getId();
        byte[] startKey = RowKeyFactory.buildDataKey(tableId, Constants.ZERO_UUID);
        byte[] endKey = RowKeyFactory.buildDataKey(tableId, Constants.FULL_UUID);
        Scan scan = ScanFactory.buildScan(startKey, endKey);
        ResultScanner scanner = this.table.getScanner(scan);
        Result result;
        while ((result = scanner.next()) != null) {
            Map<String, byte[]> values = ResultParser.parseDataRow(result, info);
            UUID rowId = ResultParser.parseUUID(result);
            function.apply(values, rowId);
        }

        table.flushCommits();
    }

    private interface IndexFunction<F1, F2, T> {
        T apply(F1 f1, F2 f2);
    }
}
