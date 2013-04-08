package com.nearinfinity.honeycomb.hbase;

import com.google.common.collect.BiMap;
import com.google.inject.Inject;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.mysql.IndexSchema;
import com.nearinfinity.honeycomb.mysql.TableSchema;
import com.nearinfinity.honeycomb.util.Verify;

import static com.google.common.base.Preconditions.checkNotNull;

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
        return metadata.getTableId(tableName);
    }

    public BiMap<String, Long> getColumns(long tableId) {
        return cache.columnsCacheGet(tableId);
    }

    public long getIndexId(long tableId, String indexName) {
        return cache.indicesCacheGet(tableId).get(indexName);
    }

    public TableSchema getSchema(Long tableId) {
        return cache.schemaCacheGet(tableId);
    }

    @Override
    public Table openTable(String tableName) {
        Long tableId = cache.tableCacheGet(tableName);
        return tableFactory.createTable(tableId);
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
        cache.invalidateTableCache(tableName);
        cache.invalidateColumnsCache(tableId);
        cache.invalidateSchemaCache(tableId);
        metadata.deleteTable(tableName);
    }

    @Override
    public void addIndex(final String tableName, final String indexName, final IndexSchema schema) {
        Verify.isNotNullOrEmpty(tableName, "The table name is invalid");
        Verify.isNotNullOrEmpty(indexName, "The index name is invalid");
        checkNotNull(schema);

        final long tableId = cache.tableCacheGet(tableName);

        metadata.createTableIndex(tableId, indexName, schema);
        cache.invalidateSchemaCache(tableId);
        cache.invalidateIndicesCache(tableId);
    }

    @Override
    public void dropIndex(final String tableName, final String indexName) {
        Verify.isNotNullOrEmpty(tableName, "The table name is invalid");
        Verify.isNotNullOrEmpty(indexName, "The index name is invalid");

        final long tableId = cache.tableCacheGet(tableName);

        metadata.deleteTableIndex(tableId, indexName);
        cache.invalidateSchemaCache(tableId);
        cache.invalidateIndicesCache(tableId);
    }

    @Override
    public void renameTable(String curTableName, String newTableName) {
        long tableId = cache.tableCacheGet(curTableName);
        metadata.renameExistingTable(curTableName, newTableName);
        cache.invalidateTableCache(curTableName);
        cache.invalidateColumnsCache(tableId);
        cache.invalidateSchemaCache(tableId);
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
        cache.updateRowsCache(tableId, value);
        return value;
    }

    @Override
    public void truncateRowCount(String tableName) {
        long tableId = cache.tableCacheGet(tableName);
        metadata.truncateRowCount(tableId);
        cache.invalidateRowsCache(tableId);
    }
}
