package com.nearinfinity.honeycomb.hbase;

import java.util.Map;

import com.google.common.collect.BiMap;
import com.google.inject.Inject;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.mysql.gen.IndexSchema;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;

public class HBaseStore implements Store {
    private final HBaseMetadata metadata;
    private final HBaseTableFactory tableFactory;
    private final MetadataCache cache;

    @Inject
    public HBaseStore(HBaseMetadata metadata, HBaseTableFactory tableFactory, MetadataCache cache) {
        this.metadata = metadata;
        this.tableFactory = tableFactory;
        this.cache = cache;
    }

    public long getTableId(String tableName) {
        return cache.tableCacheGet(tableName);
    }

    public BiMap<String, Long> getColumns(long tableId) {
        return cache.columnsCacheGet(tableId);
    }

    public Map<String, Long> getIndices(long tableId) {
        return cache.indicesCacheGet(tableId);
    }

    public TableSchema getSchema(Long tableId) {
        return cache.schemaCacheGet(tableId);
    }

    @Override
    public Table openTable(String tableName) {
        Long tableId = cache.tableCacheGet(tableName);
        return tableFactory.createTable(tableId, cache.schemaCacheGet(tableId));
    }

    @Override
    public TableSchema getSchema(String tableName) {
        return cache.schemaCacheGet(cache.tableCacheGet(tableName));
    }

    @Override
    public void createTable(String tableName, TableSchema schema) {
        metadata.createTable(tableName, schema);
    }

    @Override
    public void deleteTable(String tableName) {
        long tableId = cache.tableCacheGet(tableName);
        cache.invalidateCache(tableName, tableId);
        metadata.deleteTable(tableName);
    }

    @Override
    public void addIndex(String tableName, String indexName, IndexSchema schema) {
        final long tableId = cache.tableCacheGet(tableName);

        metadata.createTableIndex(tableId, indexName, schema);
        cache.invalidateSchemaCache(tableId);
        cache.invalidateIndicesCache(tableId);
    }

    @Override
    public void dropIndex(String tableName, String indexName) {
    }

    @Override
    public void renameTable(String curTableName, String newTableName) {
        long tableId = cache.tableCacheGet(curTableName);
        metadata.renameExistingTable(curTableName, newTableName);
        cache.invalidateCache(curTableName, tableId);
    }

    @Override
    public long getAutoInc(String tableName) {
        return cache.autoIncCacheGet(cache.tableCacheGet(tableName));
    }

    @Override
    public void setAutoInc(String tableName, long value) {
        long tableId = cache.tableCacheGet(tableName);
        long current = cache.autoIncCacheGet(tableId);
        if (value > current) {
            metadata.setAutoInc(tableId, value);
            cache.invalidateAutoIncCache(tableId);
        }
    }

    @Override
    public long incrementAutoInc(String tableName, long amount) {
        long tableId = cache.tableCacheGet(tableName);
        long value = metadata.incrementAutoInc(tableId, amount);
        cache.updateAutoIncCache(tableId, value);
        return value;
    }

    @Override
    public void truncateAutoInc(String tableName) {
        long tableId = cache.tableCacheGet(tableName);
        metadata.setAutoInc(tableId, 1);
        cache.invalidateAutoIncCache(tableId);
    }

    @Override
    public long getRowCount(String tableName) {
        return cache.rowsCacheGet(cache.tableCacheGet(tableName));
    }

    @Override
    public long incrementRowCount(String tableName, long amount) {
        long tableId = cache.tableCacheGet(tableName);
        long value = metadata.incrementRowCount(tableId, amount);
        cache.updateRowCache(tableId, value);
        return value;
    }

    @Override
    public void truncateRowCount(String tableName) {
        long tableId = cache.tableCacheGet(tableName);
        metadata.truncateRowCount(tableId);
        cache.invalidateRowCache(tableId);
    }
}
