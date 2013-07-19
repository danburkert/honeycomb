/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 * Copyright 2013 Near Infinity Corporation.
 */


package com.nearinfinity.honeycomb.hbase;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.BiMap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import com.nearinfinity.honeycomb.util.Verify;
import net.jcip.annotations.ThreadSafe;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * Caches metadata about tables to reduce HBase lookups.
 */
@Singleton
@ThreadSafe
public class MetadataCache {
    private static final Logger logger = Logger.getLogger(MetadataCache.class);
    private final LoadingCache<String, Long> tableCache;
    private final LoadingCache<Long, BiMap<String, Long>> columnsCache;
    private final LoadingCache<Long, Long> autoIncCache;
    private final LoadingCache<Long, TableSchema> schemaCache;
    private final LoadingCache<Long, Map<String, Long>> indicesCache;

    @Inject
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

    /**
     * Retrieve a table ID from the cache based on the table name.
     *
     * @param tableName Name of the table (cannot be null/empty)
     * @return Table ID
     */
    public long tableCacheGet(final String tableName) {
        Verify.isNotNullOrEmpty(tableName);
        return cacheGet(tableCache, tableName);
    }

    /**
     * Retrieve a BiMap of column name to column ID from cache based on table ID.
     *
     * @param tableId Table ID
     * @return BiMap of column name to column ID
     */
    public BiMap<String, Long> columnsCacheGet(final long tableId) {
        Verify.isValidId(tableId);
        return cacheGet(columnsCache, tableId);
    }

    /**
     * Retrieve a table schema from cache based on table ID.
     *
     * @param tableId Table ID
     * @return Table schema
     */
    public TableSchema schemaCacheGet(final long tableId) {
        Verify.isValidId(tableId);
        return cacheGet(schemaCache, tableId);
    }

    /**
     * Retrieve a map of index name to index ID from cache based on a table ID.
     *
     * @param tableId Table ID
     * @return Map of index name to index ID
     */
    public Map<String, Long> indicesCacheGet(Long tableId) {
        Verify.isValidId(tableId);
        return cacheGet(indicesCache, tableId);
    }

    /**
     * Retrieve the auto increment count for a table from cache.
     *
     * @param tableId Table ID
     * @return Auto increment count
     */
    public Long autoIncCacheGet(Long tableId) {
        Verify.isValidId(tableId);
        return cacheGet(autoIncCache, tableId);
    }

    /**
     * Evict the index mapping from the cache for the specified table id
     *
     * @param tableId Table ID
     */
    public void invalidateIndicesCache(long tableId) {
        Verify.isValidId(tableId);
        indicesCache.invalidate(tableId);
    }

    /**
     * Evict the {@link TableSchema} from the cache for the specified table id
     *
     * @param tableId Table ID
     */
    public void invalidateSchemaCache(long tableId) {
        Verify.isValidId(tableId);
        schemaCache.invalidate(tableId);
    }

    /**
     * Evict a table's columns cache.
     *
     * @param tableId Table ID
     */
    public void invalidateColumnsCache(long tableId) {
        Verify.isValidId(tableId);
        columnsCache.invalidate(tableId);
    }

    /**
     * Evict a table's metadata from the cache.
     *
     * @param tableName Table name
     */
    public void invalidateTableCache(String tableName) {
        Verify.isNotNullOrEmpty(tableName);
        tableCache.invalidate(tableName);
    }

    /**
     * Evict a table's auto increment count from the cache.
     *
     * @param tableId Table ID
     */
    public void invalidateAutoIncCache(long tableId) {
        Verify.isValidId(tableId);
        autoIncCache.invalidate(tableId);
    }

    /**
     * Updates a table's auto increment value in cache.
     *
     * @param tableId Table ID
     * @param value   New auto increment value
     */
    public void updateAutoIncCache(long tableId, long value) {
        Verify.isValidId(tableId);
        autoIncCache.put(tableId, value);
    }

    private static <K, V> V cacheGet(LoadingCache<K, V> cache, K key) {
        try {
            return cache.get(key);
        }
        catch (Exception e) {
            logger.error("Encountered unexpected exception during cache get:", e.getCause());
            throw (RuntimeException) e.getCause();
        }
    }
}