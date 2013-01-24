package com.nearinfinity.honeycomb.hbaseclient;

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class TableCache {
    private static final ConcurrentHashMap<String, TableInfo> tableCache = new ConcurrentHashMap<String, TableInfo>();
    private static final ReadWriteLock cacheLock = new ReentrantReadWriteLock();
    private static final Logger logger = Logger.getLogger(TableCache.class);

    /**
     * Retrieves SQL table metadata from a cache or HBase.
     *
     * @param tableName SQL table name
     * @param table     HTable containing the metadata
     * @return SQL table metadata
     * @throws IOException
     */
    public static TableInfo getTableInfo(String tableName, HTableInterface table) throws IOException {
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
            TableInfo tableInfo = tableCache.get(tableName); // Did the table cache get updated before entering the write lock?
            if (tableInfo != null) {
                return tableInfo;
            }

            logger.info(String.format("Table cache miss for %s. Going out to HBase.", tableName));
            return refreshCache(tableName, table, tableCache);
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    /**
     * Put table metadata into the cache for a SQL table.
     *
     * @param tableName SQL table name
     * @param info      Table metadata
     */
    public static void put(String tableName, TableInfo info) {
        tableCache.put(tableName, info);
    }

    /**
     * Atomically swap an old table name for a new one.
     *
     * @param from Old table name
     * @param to   New table name
     * @param info New table metadata
     */
    public static void swap(String from, String to, TableInfo info) {
        cacheLock.writeLock().lock();
        try {
            tableCache.remove(from);
            tableCache.put(to, info);
        } finally {
            cacheLock.writeLock().unlock();
        }
    }

    private static TableInfo refreshCache(String tableName, HTableInterface table,
                                          ConcurrentHashMap<String, TableInfo> tableCache)
            throws IOException {
        if (table == null) {
            throw new IllegalStateException(format("Table %s is null." +
                    " Cannot get table information from null table.", tableName));
        }

        long tableId = retrieveTableId(tableName, table);

        TableInfo info = new TableInfo(tableName, tableId);

        addColumns(table, info);

        addTableMetadata(info, table, tableName);

        tableCache.put(tableName, info);

        return info;
    }

    private static long retrieveTableId(String tableName, HTableInterface table) throws IOException {
        String htableName = format("HTable used \"%s\"",
                Bytes.toString(table.getTableName()));
        Get tableIdGet = new Get(RowKeyFactory.ROOT);
        String tableNotFoundMessage = format("SQL table \"%s\" was not found. %s",
                tableName, htableName);
        Result result = table.get(tableIdGet);
        if (result.isEmpty()) {
            throw new TableNotFoundException(tableNotFoundMessage);
        }

        byte[] sqlTableBytes = result.getValue(Constants.NIC, tableName.getBytes());
        if (sqlTableBytes == null) {
            throw new TableNotFoundException(tableNotFoundMessage);
        }

        long tableId = ByteBuffer.wrap(sqlTableBytes).getLong();
        if (tableId < 0) {
            throw new IllegalStateException(format("Table id %d retrieved from HBase was not valid.", tableId));
        }
        return tableId;
    }

    private static void addTableMetadata(TableInfo info, HTableInterface table, String sqlTableName) throws IOException {
        long tableId = info.getId();
        byte[] rowKey = RowKeyFactory.buildTableInfoKey(tableId);
        Result tableMetadata = table.get(new Get(rowKey));

        NavigableMap<byte[], byte[]> familyMap = tableMetadata.getFamilyMap(Constants.NIC);
        if (familyMap.isEmpty()) {
            throw new IllegalStateException(format("SQL Table \"%s\" is missing metadata.", sqlTableName));
        }
        Map<String, byte[]> stringFamily = Maps.newHashMap();
        for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
            stringFamily.put(new String(entry.getKey()), entry.getValue());
        }
        info.setTableMetadata(stringFamily);
    }

    private static void addColumns(HTableInterface table, TableInfo info) throws IOException {
        long tableId = info.getId();
        byte[] rowKey = RowKeyFactory.buildColumnsKey(tableId);
        Get columnsGet = new Get(rowKey);
        Result columnsResult = table.get(columnsGet);
        if (columnsResult == null || columnsResult.isEmpty()) {
            throw new IllegalStateException("Column result from the get was null/empty for row key " + Bytes.toStringBinary(rowKey));
        }

        Map<byte[], byte[]> columns = columnsResult.getFamilyMap(Constants.NIC);
        if (columns == null) {
            throw new NullPointerException("Columns were null after getting family map.");
        }

        for (Map.Entry<byte[], byte[]> entry : columns.entrySet()) {
            String columnName = new String(entry.getKey());
            long columnId = ByteBuffer.wrap(entry.getValue()).getLong();
            info.addColumn(columnName, columnId, getMetadataForColumn(tableId, columnId, table));
        }
    }

    private static ColumnMetadata getMetadataForColumn(long tableId, long columnId, HTableInterface table) throws IOException {
        Get metadataGet = new Get(RowKeyFactory.buildColumnInfoKey(tableId, columnId));
        Result result = table.get(metadataGet);

        byte[] jsonBytes = result.getValue(Constants.NIC, Constants.METADATA);
        return new ColumnMetadata(jsonBytes);
    }
}
