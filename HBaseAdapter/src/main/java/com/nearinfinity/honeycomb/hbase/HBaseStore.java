package com.nearinfinity.honeycomb.hbase;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.BiMap;
import com.google.inject.Inject;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class HBaseStore implements Store {
    private static final Logger logger = Logger.getLogger(HBaseStore.class);
    private final HBaseMetadata metadata;
    private final HBaseTableFactory tableFactory;
    private LoadingCache<String, Long> tableCache;
    private LoadingCache<Long, BiMap<String, Long>> columnsCache;
    private LoadingCache<Long, Long> rowsCache;
    private LoadingCache<Long, Long> autoIncCache;
    private LoadingCache<Long, TableSchema> schemaCache;
    private LoadingCache<Long, Map<String, Long>> indicesCache;

    @Inject
    public HBaseStore(HBaseMetadata metadata, HBaseTableFactory tableFactory) {
        this.metadata = metadata;
        this.tableFactory = tableFactory;
        doInitialization();
    }

    public long getTableId(String tableName) {
        return tableCacheGet(tableName);
    }

    public BiMap<String, Long> getColumns(long tableId) {
        return columnsCacheGet(tableId);
    }

    public Map<String, Long> getIndices(long tableId) {
        return indicesCacheGet(tableId);
    }

    public TableSchema getSchema(Long tableId) {
        return schemaCacheGet(tableId);
    }

    @Override
    public Table openTable(String tableName) {
        Long tableId = tableCacheGet(tableName);
        return tableFactory.createTable(tableId, schemaCacheGet(tableId));
    }

    @Override
    public TableSchema getSchema(String tableName) {
        return schemaCacheGet(tableCacheGet(tableName));
    }

    @Override
    public void createTable(String tableName, TableSchema schema) {
        metadata.createTable(tableName, schema);
    }

    @Override
    public void deleteTable(String tableName) {
        long tableId = tableCacheGet(tableName);
        invalidateCache(tableName, tableId);
        metadata.deleteSchema(tableName);
    }

    @Override
    public void alterTable(String tableName, TableSchema schema) {
        long tableId = tableCacheGet(tableName);
        metadata.updateSchema(tableId, schemaCacheGet(tableId), schema);
        invalidateCache(tableName, tableId);
    }

    @Override
    public void renameTable(String curTableName, String newTableName) {
        long tableId = tableCacheGet(curTableName);
        metadata.renameExistingTable(curTableName, newTableName);
        invalidateCache(curTableName, tableId);
    }

    @Override
    public long getAutoInc(String tableName) {
        return autoIncCacheGet(tableCacheGet(tableName));
    }

    @Override
    public long incrementAutoInc(String tableName, long amount) {
        long tableId = tableCacheGet(tableName);
        long value = metadata.incrementAutoInc(tableId, amount);
        autoIncCache.put(tableId, value);
        return value;
    }

    @Override
    public void truncateAutoInc(String tableName) {
        long tableId = tableCacheGet(tableName);
        metadata.truncateAutoInc(tableId);
        autoIncCache.invalidate(tableId);
    }

    @Override
    public long getRowCount(String tableName) {
        return rowsCacheGet(tableCacheGet(tableName));
    }

    @Override
    public long incrementRowCount(String tableName, long amount) {
        long tableId = tableCacheGet(tableName);
        long value = metadata.incrementRowCount(tableId, amount);
        rowsCache.put(tableId, value);
        return value;
    }

    @Override
    public void truncateRowCount(String tableName) {
        long tableId = tableCacheGet(tableName);
        metadata.truncateRowCount(tableId);
        rowsCache.invalidate(tableId);
    }

    private void doInitialization() {
        tableCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<String, Long>() {
                    @Override
                    public Long load(String tableName) {
                        return metadata.getTableId(tableName);
                    }
                }
                );

        columnsCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<Long, BiMap<String, Long>>() {
                    @Override
                    public BiMap<String, Long> load(Long tableId) {
                        return metadata.getColumnIds(tableId);
                    }
                }
                );

        indicesCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<Long, Map<String, Long>>() {
                    @Override
                    public Map<String, Long> load(Long tableId) {
                        return metadata.getIndexIds(tableId);
                    }
                });

        autoIncCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<Long, Long>() {
                    @Override
                    public Long load(Long tableId) {
                        return metadata.getAutoInc(tableId);
                    }
                }
                );

        rowsCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<Long, Long>() {
                    @Override
                    public Long load(Long tableId) {
                        return metadata.getRowCount(tableId);
                    }
                }
                );

        schemaCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<Long, TableSchema>() {
                    @Override
                    public TableSchema load(Long tableId) {
                        return metadata.getSchema(tableId);
                    }
                }
                );
    }

    private void invalidateCache(String tableName, long tableId) {
        tableCache.invalidate(tableName);
        columnsCache.invalidate(tableId);
        schemaCache.invalidate(tableId);
    }

    private Long tableCacheGet(String tableName) {
        return cacheGet(tableCache, tableName);
    }

    private BiMap<String, Long> columnsCacheGet(Long tableId) {
        return cacheGet(columnsCache, tableId);
    }

    private Map<String, Long> indicesCacheGet(Long tableId) {
        return cacheGet(indicesCache, tableId);
    }

    private Long autoIncCacheGet(Long tableId) {
        return cacheGet(autoIncCache, tableId);
    }

    private Long rowsCacheGet(Long tableId) {
        return cacheGet(rowsCache, tableId);
    }

    private TableSchema schemaCacheGet(Long tableId) {
        return cacheGet(schemaCache, tableId);
    }

    private <K, V> V cacheGet(LoadingCache<K, V> cache, K key) {
        try {
            return cache.get(key);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            logger.error("Encountered unexpected exception during cache get:", cause);
            throw new RuntimeException(cause);
        }
    }
}
