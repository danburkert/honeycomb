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

import com.google.inject.Inject;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import com.nearinfinity.honeycomb.util.Verify;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An HBase backed {@link Store}
 */
public class HBaseStore implements Store {
    private final static ReadWriteLock autoIncrementLock = new ReentrantReadWriteLock();
    private final MultiTableHBaseMetadata metadata;
    private final HBaseTableFactory tableFactory;
    private final MetadataCache cache;
    private final HTableManager tableManager;

    /**
     * Construct a HBase store with metadata, a table factory and metadata cache.
     *
     * @param metadata     Table metadata backed by HBase
     * @param tableFactory Table factory
     * @param cache        Metadata cache
     */
    @Inject
    public HBaseStore(MultiTableHBaseMetadata metadata, HTableManager tableManager, HBaseTableFactory tableFactory, MetadataCache cache) {
        this.metadata = metadata;
        this.tableFactory = tableFactory;
        this.cache = cache;
        this.tableManager = tableManager;
    }

    @Override
    public Table openTable(String tableName) {
        return tableFactory.createTable(cache.idCacheGet(tableName));
    }

    @Override
    public TableSchema getSchema(String tableName) {
        return cache.schemaCacheGet(cache.idCacheGet(tableName));
    }

    @Override
    public void createTable(String tableName, TableSchema schema) {
        metadata.createTable(tableName, schema);
    }

    @Override
    public void deleteTable(String tableName) {
        long tableId = cache.idCacheGet(tableName);
        cache.invalidateTableCache(tableName);
        cache.invalidateColumnsCache(tableId);
        cache.invalidateSchemaCache(tableId);
        metadata.dropTable(tableName);
    }

    @Override
    public void addIndex(final String tableName, final IndexSchema schema) {
        Verify.isNotNullOrEmpty(tableName, "The table name is invalid");

        checkNotNull(schema);

        final long tableId = cache.idCacheGet(tableName);

        metadata.createTableIndex(tableId, schema);
        cache.invalidateSchemaCache(tableId);
        cache.invalidateIndicesCache(tableId);
    }

    @Override
    public void dropIndex(final String tableName, final String indexName) {
        Verify.isNotNullOrEmpty(tableName, "The table name is invalid");
        Verify.isNotNullOrEmpty(indexName, "The index name is invalid");

        final long tableId = cache.idCacheGet(tableName);

        metadata.deleteTableIndex(tableId, indexName);
        cache.invalidateSchemaCache(tableId);
        cache.invalidateIndicesCache(tableId);
    }

    @Override
    public void renameTable(String curTableName, String newTableName) {
        long tableId = cache.idCacheGet(curTableName);
        metadata.renameExistingTable(curTableName, newTableName);
        cache.invalidateTableCache(curTableName);
        cache.invalidateColumnsCache(tableId);
        cache.invalidateSchemaCache(tableId);
    }

    @Override
    public long getAutoInc(String tableName) {
        long tableId = cache.idCacheGet(tableName);
        try {
            autoIncrementLock.readLock().lock();
            return cache.autoIncCacheGet(tableId);
        } finally {
            autoIncrementLock.readLock().unlock();
        }
    }

    @Override
    public void setAutoInc(String tableName, long value) {
        long tableId = cache.idCacheGet(tableName);
        try {
            autoIncrementLock.writeLock().lock();
            long current = cache.autoIncCacheGet(tableId);
            if (value > current) {
                metadata.setAutoInc(tableId, value);
                cache.invalidateAutoIncCache(tableId);
            }
        } finally {
            autoIncrementLock.writeLock().unlock();
        }
    }

    @Override
    public long incrementAutoInc(String tableName, long amount) {
        long tableId = cache.idCacheGet(tableName);
        long value;
        try {
            autoIncrementLock.writeLock().lock();
            value = metadata.incrementAutoInc(tableId, amount);
            cache.updateAutoIncCache(tableId, value);
        } finally {
            autoIncrementLock.writeLock().unlock();
        }
        return value;
    }

    @Override
    public void truncateAutoInc(String tableName) {
        long tableId = cache.idCacheGet(tableName);
        try {
            autoIncrementLock.writeLock().lock();
            metadata.setAutoInc(tableId, 1);
            cache.invalidateAutoIncCache(tableId);
        } finally {
            autoIncrementLock.writeLock().unlock();
        }
    }

}
