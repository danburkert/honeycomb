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
import net.jcip.annotations.ThreadSafe;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * Cache holding metadata about Honeycomb tables in the HBase backend
 */
@Singleton
@ThreadSafe
public class MetadataCache {
    private static final Logger logger = Logger.getLogger(MetadataCache.class);
    private final LoadingCache<String, String> idCache;
    private final LoadingCache<String, BiMap<String, Long>> columnsCache;
    private final LoadingCache<String, Map<String, Long>> indicesCache;
    private final LoadingCache<String, Long> autoIncCache;
    private final LoadingCache<String, TableSchema> schemaCache;

    @Inject
    public MetadataCache(final MultiTableHBaseMetadata metadata) {
        idCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<String, String>() {
                    @Override
                    public String load(String tableName) {
                        return metadata.getTableId(tableName);
                    }
                });

        columnsCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<String, BiMap<String, Long>>() {
                    @Override
                    public BiMap<String, Long> load(String tableName) {
                        return metadata.getColumnIds(tableName);
                    }
                }
                );

        indicesCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<String, Map<String, Long>>() {
                    @Override
                    public Map<String, Long> load(String tableName) {
                        return metadata.getIndexIds(tableName);
                    }
                });

        autoIncCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<String, Long>() {
                    @Override
                    public Long load(String tableName) {
                        return metadata.getAutoInc(tableName);
                    }
                }
                );

        schemaCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<String, TableSchema>() {
                    @Override
                    public TableSchema load(String tableName) {
                        return metadata.getSchema(tableName);
                    }
                }
                );
    }

    /**
     * Retrieve the table's cached ID
     */
    public String idCacheGet(final String tableName) {
        return cacheGet(idCache, tableName);
    }

    /**
     * Retrieve the table's cached BiMap of column name to column ID
     */
    public BiMap<String, Long> columnsCacheGet(String tableName) {
        return cacheGet(columnsCache, tableName);
    }

    /**
     * Retrieve the table's cached schema
     */
    public TableSchema schemaCacheGet(String tableName) {
        return cacheGet(schemaCache, tableName);
    }

    /**
     * Retrieve the table's cached map of index name to index id
     */
    public Map<String, Long> indicesCacheGet(String tableName) {
        return cacheGet(indicesCache, tableName);
    }

    /**
     * Retrieve the table's cached auto increment value
     */
    public Long autoIncCacheGet(String tableName) {
        return cacheGet(autoIncCache, tableName);
    }

    /**
     * Evict the table's cached index map
     */
    public void invalidateIndicesCache(String tableName) {
        indicesCache.invalidate(tableName);
    }

    /**
     * Evict the table's cached {@link TableSchema}
     */
    public void invalidateSchemaCache(String tableName) {
        schemaCache.invalidate(tableName);
    }

    /**
     * Evict the table's cached columns
     */
    public void invalidateColumnsCache(String tableName) {
        columnsCache.invalidate(tableName);
    }

    /**
     * Evict the table's cached id
     */
    public void invalidateTableCache(String tableName) {
        idCache.invalidate(tableName);
    }

    /**
     * Evict the table's cached auto increment value
     */
    public void invalidateAutoIncCache(String tableName) {
        autoIncCache.invalidate(tableName);
    }

    /**
     * Update the table's cached auto increment value
     */
    public void updateAutoIncCache(String tableName, long value) {
        autoIncCache.put(tableName, value);
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