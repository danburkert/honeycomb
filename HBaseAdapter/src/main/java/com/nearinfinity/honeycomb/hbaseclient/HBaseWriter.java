package com.nearinfinity.honeycomb.hbaseclient;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.hbase.ResultReader;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class HBaseWriter implements Closeable {
    private static final Logger logger = Logger.getLogger(HBaseWriter.class);
    private final HTableInterface table;
    private long writeBufferSize;

    public HBaseWriter(HTableInterface table) {
        this.table = table;
        this.writeBufferSize = 0;
    }

    public void setWriteBufferSize(long writeBufferSize){
        this.writeBufferSize = writeBufferSize;
    }

    @Override
    public void close() throws IOException {
        table.close();
    }

    /**
     * Writes a SQL row into HBase "SQL" table.
     *
     * @param tableName Name of SQL table
     * @param values    SQL row values
     * @throws IOException
     */
    public void writeRow(String tableName, Map<String, byte[]> values) throws IOException {
        if (logger.isDebugEnabled()) {
            logger.debug(format("Writing row for %s: %s", tableName, values));
        }

        TableInfo info = TableCache.getTableInfo(tableName, table);
        List<List<String>> multipartIndex = Index.indexForTable(info.tableMetadata());
        List<Put> putList = PutListFactory.createDataInsertPutList(values, info, multipartIndex);
        table.put(putList);

        // Very special case for alter table with data in it. MySQL creates a temp table in the form #sql-XXXX_X
        // and starts to put data in it. When a unique index is on the new table duplicates appear because the data is
        // not flushed to HBase.
        if (info.getTableName().startsWith("#sql-")) {
            table.flushCommits();
        }
    }

    /**
     * Updates a SQL row with new values.
     *
     * @param changedFields Columns that will change
     * @param tableName     Name of SQL table
     * @param newValues     SQL row values
     * @throws IOException
     */
    public void updateRow(UUID uuid, List<String> changedFields, String tableName, Map<String, byte[]> newValues) throws IOException {
        if (logger.isDebugEnabled()) {
            logger.debug(format("Updating fields %s on %s", Joiner.on(",").join(changedFields), tableName));
        }

        Map<String, byte[]> oldRow = retrieveRowAndDelete(tableName, uuid);

        for (String changedField : changedFields) {
            oldRow.put(changedField, newValues.get(changedField)); // Hack around MySQL setting field->is_null when actually not.
        }

        TableInfo info = getTableInfo(tableName);
        Set<String> columnNames = info.getColumnNames();
        for (String columnName : columnNames) {
            if (!oldRow.containsKey(columnName)) {
                if (logger.isDebugEnabled()) {
                    logger.debug(format("On update %s was set to null.", columnName));
                }
                oldRow.put(columnName, null); // Nulls need to be transferred otherwise writeRow loses them.
            }
        }

        writeRow(tableName, oldRow);
    }

    /**
     * Renames a SQL table in HBase
     *
     * @param from Old SQL table name
     * @param to   New SQL table name
     * @throws IOException
     */
    public void renameTable(String from, String to) throws IOException {
        checkNotNull(from);
        checkNotNull(to);
        logger.debug(String.format("Renaming table %s to %s", from, to));

        TableInfo info = getTableInfo(from);

        byte[] rowKey = RowKeyFactory.ROOT;

        Delete oldNameDelete = new Delete(rowKey);

        oldNameDelete.deleteColumn(Constants.NIC, from.getBytes());

        table.delete(oldNameDelete);

        Put nameChangePut = new Put(rowKey);
        nameChangePut.add(Constants.NIC, to.getBytes(), Bytes.toBytes(info.getId()));

        table.put(nameChangePut);
        table.flushCommits();

        info.setName(to);

        TableCache.swap(from, to, info);

        logger.debug("Rename complete!");
    }

    /**
     * Deletes a SQL row out of HBase based on the SQL row's unique identifier
     *
     * @param tableName SQL table name
     * @param uuid      Unique identifier of the SQL row
     * @return success
     * @throws IOException
     */
    public boolean deleteRow(String tableName, UUID uuid) throws IOException {
        if (uuid == null) {
            return false;
        }

        logger.debug("Removing row in " + tableName);
        retrieveRowAndDelete(tableName, uuid);

        return true;
    }

    /**
     * Drops a SQL table from HBase
     *
     * @param tableName SQL table name
     * @return success
     * @throws IOException
     */
    public boolean dropTable(String tableName) throws IOException {
        logger.debug("Preparing to drop table " + tableName);
        TableInfo info = getTableInfo(tableName);

        deleteAllRowsInTable(info);
        dropTableMetadata(info, tableName);

        logger.debug("Table " + tableName + " is no more!");

        return true;
    }

    /**
     * Remove all SQL rows from a SQL table
     *
     * @param tableName SQL table name
     * @return Number of rows removed
     * @throws IOException
     */
    public int deleteAllRowsInTable(String tableName) throws IOException {
        TableInfo info = getTableInfo(tableName);
        long tableId = info.getId();

        logger.debug(String.format("Deleting all rows from table %s with tableId %d", tableName, tableId));

        deleteAllRowsInTable(info);
        return 0;
    }

    /**
     * Set the SQL row count in HBase.
     *
     * @param tableName SQL table name
     * @param value     Value to set the count to
     * @throws IOException
     */
    public void setRowCount(String tableName, long value) throws IOException {
        long tableId = getTableInfo(tableName).getId();
        Put put = new Put(RowKeyFactory.buildTableInfoKey(tableId)).add(Constants.NIC, Constants.ROW_COUNT, Bytes.toBytes(value));
        table.put(put);
    }

    public void incrementRowCount(String tableName, long delta) throws IOException {
        long tableId = getTableInfo(tableName).getId();
        byte[] rowKey = RowKeyFactory.buildTableInfoKey(tableId);
        table.incrementColumnValue(rowKey, Constants.NIC, Constants.ROW_COUNT, delta);
    }

    /**
     * Adds an index to a SQL table
     *
     * @param tableName    SQL table name
     * @param columnString SQL table columns used in the index
     * @throws IOException
     */
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
                try {
                    List<Put> puts = PutListFactory.createIndexForColumns(values, info, uuid, columnsToIndex);
                    table.put(puts);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                return null;
            }
        });
    }

    /**
     * Remove an index from a SQL table
     *
     * @param tableName   SQL table name
     * @param indexToDrop SQL table columns used in the index
     * @throws IOException
     */
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

    /**
     * Sets the autoincrement value of a SQL column.
     *
     * @param tableName          SQL table name
     * @param fieldName          SQL column
     * @param autoincrementValue New autoincrement value
     * @param isTruncate         Is SQL truncate command
     * @return Success
     * @throws IOException
     */
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
            logger.debug(format("The new auto_increment value of %d is less than the current count of %d, so this command will be ignored.", autoincrementValue, currentValue));
            return false;
        }

        Put columnInfoPut = new Put(columnInfoBytes);
        columnInfoPut.add(Constants.NIC, Constants.METADATA, metadata.toJson());
        columnInfoPut.add(Constants.NIC, new byte[0], Bytes.toBytes(autoincrementValue));

        table.put(columnInfoPut);
        table.flushCommits();

        info.setColumnMetadata(fieldName, metadata);

        return true;
    }

    /**
     * Retrieves the next autoincrement value and increments the value.
     *
     * @param tableName  SQL table name
     * @param columnName SQL column
     * @return Next autoincrement value
     * @throws IOException
     */
    public long getNextAutoincrementValue(String tableName, String columnName) throws IOException {
        long nextAutoincrementValue = getTableInfo(tableName).getColumnMetadata(columnName).getAutoincrementValue();
        alterAutoincrementValue(tableName, columnName, nextAutoincrementValue + 1, false);

        return nextAutoincrementValue;
    }

    /**
     * Creates a sql table in HBase with columns and indexes.
     * Called when a "create table XXX()" statement is executed.
     *
     * @param tableName      Name of sql table
     * @param columns        Column name and metadata
     * @param indexedColumns Indexed columns
     * @throws IOException
     */
    public void createTableFull(String tableName, Map<String,
            ColumnMetadata> columns, TableMultipartKeys indexedColumns)
            throws IOException {
        List<Put> putList = new LinkedList<Put>();

        createTable(tableName, putList, indexedColumns);

        addColumns(tableName, columns, putList);

        table.put(putList);

        table.flushCommits();
    }

    /**
     * Flush data stored in cache out to HBase
     */
    public void flushWrites() {
        try {
            table.flushCommits();
        } catch (IOException e) {
            logger.error("Encountered an exception while flushing commits : ", e);
        }
    }

    private TableInfo getTableInfo(String tableName) throws IOException {
        return TableCache.getTableInfo(tableName, table);
    }

    private void dropTableMetadata(TableInfo info, String fullTableName) throws IOException {
        long tableId = info.getId();
        List<Delete> deletes = Lists.newArrayList();
        deletes.add(new Delete(RowKeyFactory.buildTableInfoKey(tableId)));
        deletes.add(new Delete(RowKeyFactory.buildColumnsKey(tableId)));

        for (Long columnId : info.getColumnIds()) {
            deletes.add(new Delete(RowKeyFactory.buildColumnInfoKey(tableId, columnId)));
        }

        Delete delete = new Delete(RowKeyFactory.ROOT);
        delete.deleteColumns(Constants.NIC, fullTableName.getBytes());
        deletes.add(delete);

        table.delete(deletes);
    }

    private void deleteAllRowsInTable(TableInfo info) throws IOException {
        long totalDeleteSize = 0;
        long tableId = info.getId();
        byte[] startKey = RowKeyFactory.buildDataKey(tableId, Constants.ZERO_UUID);
        byte[] endKey = RowKeyFactory.buildDataKey(tableId, Constants.FULL_UUID);
        Scan scan = ScanFactory.buildScan(startKey, endKey);

        ResultScanner scanner = table.getScanner(scan);
        List<List<String>> indexedKeys = Index.indexForTable(info.tableMetadata());
        List<Delete> deleteList = new LinkedList<Delete>();
        for (Result result : scanner) {
            UUID uuid = ResultParser.parseUUID(result);
            byte[] rowKey = result.getRow();
            List<Delete> deletes = DeleteListFactory.createDeleteRowList(uuid, info, result, rowKey, indexedKeys);
            totalDeleteSize += deletes.size() * result.getWritableSize();
            deleteList.addAll(deletes);
            if (writeBufferSize > 0 && totalDeleteSize > writeBufferSize) {
                table.delete(deleteList);
                totalDeleteSize = 0;
            }
        }

        table.delete(deleteList);
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

        table.put(indexUpdate);
        table.flushCommits();
    }

    private void changeIndex(TableInfo info, IndexFunction<Map<String, byte[]>, UUID, Void> function) throws IOException {
        final long tableId = info.getId();
        byte[] startKey = RowKeyFactory.buildDataKey(tableId, Constants.ZERO_UUID);
        byte[] endKey = RowKeyFactory.buildDataKey(tableId, Constants.FULL_UUID);
        Scan scan = ScanFactory.buildScan(startKey, endKey);
        ResultScanner scanner = table.getScanner(scan);
        Result result;
        while ((result = scanner.next()) != null) {
            Map<String, byte[]> values = ResultReader.readDataRow(result, info).getRecords();
            UUID rowId = ResultParser.parseUUID(result);
            function.apply(values, rowId);
        }

        table.flushCommits();
    }

    private Map<String, byte[]> retrieveRowAndDelete(String tableName, UUID uuid) throws IOException {
        checkNotNull(tableName, "tableName may not be null.");
        checkNotNull(uuid, "uuid may not be null.");
        TableInfo info = getTableInfo(tableName);
        long tableId = info.getId();

        byte[] dataRowKey = RowKeyFactory.buildDataKey(tableId, uuid);
        Get get = new Get(dataRowKey);
        Result result = table.get(get);
        Map<String, byte[]> oldRow = ResultReader.readDataRow(result, info).getRecords();
        if (logger.isDebugEnabled()) {
            logger.debug(format("Deleting row in table %s / Table ID %d", tableName, tableId));
            logger.debug(format("Old row %s", oldRow));
        }

        List<Delete> deleteList = DeleteListFactory.createDeleteRowList(uuid, info, result, dataRowKey, Index.indexForTable(info.tableMetadata()));

        table.delete(deleteList);
        incrementRowCount(tableName, -1);

        return oldRow;
    }

    private void createTable(String tableName, List<Put> puts,
                             TableMultipartKeys multipartKeys)
            throws IOException {
        long tableId = table.incrementColumnValue(RowKeyFactory.ROOT, Constants.NIC, new byte[0], 1);
        TableCache.put(tableName, new TableInfo(tableName, tableId));

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

        for (Map.Entry<String, ColumnMetadata> entry : columns.entrySet()) {
            String columnName = entry.getKey();
            ColumnMetadata metadata = entry.getValue();
            long columnId = ++startColumn;

            Put columnPut = new Put(columnBytes).add(Constants.NIC, columnName.getBytes(), Bytes.toBytes(columnId));
            puts.add(columnPut);

            byte[] columnInfoBytes = RowKeyFactory.buildColumnInfoKey(tableId, columnId);
            Put columnInfoPut = new Put(columnInfoBytes);

            columnInfoPut.add(Constants.NIC, Constants.METADATA, metadata.toJson());
            if (metadata.isAutoincrement()) {
                columnInfoPut.add(Constants.NIC, new byte[0], Bytes.toBytes(0L));
            }

            puts.add(columnInfoPut);

            tableInfo.addColumn(columnName, columnId, columns.get(columnName));
        }
    }

    private interface IndexFunction<F1, F2, T> {
        T apply(F1 f1, F2 f2);
    }
}
