package com.nearinfinity.honeycomb.hbase;

import com.google.inject.Inject;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.mysql.Verify;
import com.nearinfinity.honeycomb.mysql.gen.IndexSchema;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import org.apache.log4j.Logger;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class HBaseStore implements Store {
    private static final Logger logger = Logger.getLogger(HBaseStore.class);
    private final HBaseMetadata metadata;
    private final HBaseTableFactory tableFactory;
    private final MetadataCache cache;

    @Inject
    public HBaseStore(HBaseMetadata metadata, HBaseTableFactory tableFactory, MetadataCache cache) {
        this.metadata = checkNotNull(metadata);
        this.tableFactory = checkNotNull(tableFactory);
        this.cache = checkNotNull(cache);
    }

    public Map<String, Long> getIndices(long tableId) {
        Verify.isValidTableId(tableId);
        return cache.indicesCacheGet(tableId);
    }

    @Override
    public Table openTable(String tableName) {
        Verify.isNotNullOrEmpty(tableName);
        logger.debug(String.format("Trying to open the table (%s)", tableName));
        long tableId = cache.tableCacheGet(tableName);
        return tableFactory.createTable(tableId, cache.schemaCacheGet(tableId));
    }

    @Override
    public TableSchema getSchema(String tableName) {
        Verify.isNotNullOrEmpty(tableName);
        return cache.schemaCacheGet(cache.tableCacheGet(tableName));
    }

    @Override
    public void createTable(String tableName, TableSchema schema) {
        Verify.isNotNullOrEmpty(tableName);
        logger.debug(String.format("Trying to create the table (%s)", tableName));
        metadata.createTable(tableName, schema);
    }

    @Override
    public void deleteTable(String tableName) {
        Verify.isNotNullOrEmpty(tableName);
        logger.debug(String.format("Trying to delete the table (%s)", tableName));
        long tableId = cache.tableCacheGet(tableName);
        cache.invalidateCache(tableName, tableId);
        metadata.deleteSchema(tableName);
    }

    @Override
    public void addIndex(String tableName, String indexName, IndexSchema schema) {
    }

    @Override
    public void dropIndex(String tableName, String indexName) {
    }

    @Override
    public void renameTable(String curTableName, String newTableName) {
        Verify.isNotNullOrEmpty(curTableName);
        Verify.isNotNullOrEmpty(newTableName);
        logger.debug(String.format("Trying to rename the table (%s) -> %s", curTableName, newTableName));
        long tableId = cache.tableCacheGet(curTableName);
        metadata.renameExistingTable(curTableName, newTableName);
        cache.invalidateCache(curTableName, tableId);
    }

    @Override
    public long getAutoInc(String tableName) {
        Verify.isNotNullOrEmpty(tableName);
        return cache.autoIncCacheGet(cache.tableCacheGet(tableName));
    }

    @Override
    public long incrementAutoInc(String tableName, long amount) {
        Verify.isNotNullOrEmpty(tableName);
        long tableId = cache.tableCacheGet(tableName);
        long value = metadata.incrementAutoInc(tableId, amount);
        cache.updateAutoIncCache(tableId, value);
        return value;
    }

    @Override
    public void truncateAutoInc(String tableName) {
        Verify.isNotNullOrEmpty(tableName);
        logger.debug(String.format("Trying to truncate auto increment for tables (%s)", tableName));
        long tableId = cache.tableCacheGet(tableName);
        metadata.truncateAutoInc(tableId);
        cache.invalidateAutoIncCache(tableId);
    }

    @Override
    public long getRowCount(String tableName) {
        Verify.isNotNullOrEmpty(tableName);
        return cache.rowsCacheGet(cache.tableCacheGet(tableName));
    }

    @Override
    public long incrementRowCount(String tableName, long amount) {
        Verify.isNotNullOrEmpty(tableName);
        long tableId = cache.tableCacheGet(tableName);
        long value = metadata.incrementRowCount(tableId, amount);
        cache.updateRowCache(tableId, value);
        return value;
    }

    @Override
    public void truncateRowCount(String tableName) {
        Verify.isNotNullOrEmpty(tableName);
        long tableId = cache.tableCacheGet(tableName);
        metadata.truncateRowCount(tableId);
        cache.invalidateRowCache(tableId);
    }
}
