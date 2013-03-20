package com.nearinfinity.honeycomb.hbase;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.BiMap;
import com.google.inject.Singleton;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ExecutionException;

@Singleton
public class MetadataCache {
    private static final Logger logger = Logger.getLogger(MetadataCache.class);
    private final LoadingCache<String, Long> tableCache;
    private final LoadingCache<Long, BiMap<String, Long>> columnsCache;
    private final LoadingCache<Long, Long> rowsCache;
    private final LoadingCache<Long, Long> autoIncCache;
    private final LoadingCache<Long, TableSchema> schemaCache;
    private final LoadingCache<Long, Map<String, Long>> indicesCache;

    public MetadataCache(final HBaseMetadata metadata) {
        tableCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<String, Long>() {
                    @Override
                    public Long load(String tableName) {
                        return metadata.getTableId(tableName);
                    }
                });

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

    public Long tableCacheGet(String tableName) {
        return cacheGet(tableCache, tableName);
    }

    public BiMap<String, Long> columnsCacheGet(Long tableId) {
        return cacheGet(columnsCache, tableId);
    }

    public Map<String, Long> indicesCacheGet(Long tableId) {
        return cacheGet(indicesCache, tableId);
    }

    public Long autoIncCacheGet(Long tableId) {
        return cacheGet(autoIncCache, tableId);
    }

    public Long rowsCacheGet(Long tableId) {
        return cacheGet(rowsCache, tableId);
    }

    public TableSchema schemaCacheGet(Long tableId) {
        return cacheGet(schemaCache, tableId);
    }

    public void updateRowCache(long tableId, long value) {
        rowsCache.put(tableId, value);
    }

    public void invalidateRowCache(long tableId) {
        rowsCache.invalidate(tableId);
    }

    public void invalidateCache(String tableName, long tableId) {
        tableCache.invalidate(tableName);
        columnsCache.invalidate(tableId);
        schemaCache.invalidate(tableId);
    }

    public void invalidateAutoIncCache(long tableId) {
        autoIncCache.invalidate(tableId);
    }

    public void updateAutoIncCache(long tableId, long value) {
        autoIncCache.put(tableId, value);
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
