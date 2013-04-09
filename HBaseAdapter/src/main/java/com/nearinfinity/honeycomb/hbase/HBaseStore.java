package com.nearinfinity.honeycomb.hbase;

import com.google.common.collect.BiMap;
import com.google.inject.Inject;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import com.nearinfinity.honeycomb.util.Verify;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkNotNull;

public class HBaseStore implements Store {
    private final HBaseMetadata metadata;
    private final HBaseTableFactory tableFactory;
    private final MetadataCache cache;

    private final static ReadWriteLock metadataLock = new ReentrantReadWriteLock();
    private final static ReadWriteLock rowsLock = new ReentrantReadWriteLock();
    private final static ReadWriteLock autoIncrementLock = new ReentrantReadWriteLock();

    @Inject
    public HBaseStore(HBaseMetadata metadata, HBaseTableFactory tableFactory, MetadataCache cache) {
        this.metadata = metadata;
        this.tableFactory = tableFactory;
        this.cache = cache;
    }

    public long getTableId(String tableName) {
        try {
            metadataLock.readLock().lock();
            return metadata.getTableId(tableName);
        } finally {
            metadataLock.readLock().unlock();
        }
    }

    public BiMap<String, Long> getColumns(long tableId) {
        try {
            metadataLock.readLock().lock();
            return cache.columnsCacheGet(tableId);
        } finally {
            metadataLock.readLock().unlock();
        }
    }

    public long getIndexId(long tableId, String indexName) {
        try {
            metadataLock.readLock().lock();
            return cache.indicesCacheGet(tableId).get(indexName);
        } finally {
            metadataLock.readLock().unlock();
        }
    }

    public TableSchema getSchema(Long tableId) {
        try {
            metadataLock.readLock().lock();
            return cache.schemaCacheGet(tableId);
        } finally {
            metadataLock.readLock().unlock();
        }
    }

    @Override
    public Table openTable(String tableName) {
        Long tableId;
        try {
            metadataLock.readLock().lock();
            tableId = cache.tableCacheGet(tableName);
        } finally {
            metadataLock.readLock().unlock();
        }

        return tableFactory.createTable(tableId);
    }

    @Override
    public TableSchema getSchema(String tableName) {
        try {
            metadataLock.readLock().lock();
            return cache.schemaCacheGet(cache.tableCacheGet(tableName));
        } finally {
            metadataLock.readLock().unlock();
        }
    }

    @Override
    public void createTable(String tableName, TableSchema schema) {
        try {
            metadataLock.writeLock().lock();
            metadata.createTable(tableName, schema);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    @Override
    public void deleteTable(String tableName) {
        try {
            metadataLock.writeLock().lock();
            long tableId = cache.tableCacheGet(tableName);
            cache.invalidateTableCache(tableName);
            cache.invalidateColumnsCache(tableId);
            cache.invalidateSchemaCache(tableId);
            metadata.deleteTable(tableName);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    @Override
    public void addIndex(final String tableName, final IndexSchema schema) {
        Verify.isNotNullOrEmpty(tableName, "The table name is invalid");
        checkNotNull(schema);

        try {
            metadataLock.writeLock().lock();
            final long tableId = cache.tableCacheGet(tableName);
            metadata.createTableIndex(tableId, schema);
            cache.invalidateSchemaCache(tableId);
            cache.invalidateIndicesCache(tableId);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    @Override
    public void dropIndex(final String tableName, final String indexName) {
        Verify.isNotNullOrEmpty(tableName, "The table name is invalid");
        Verify.isNotNullOrEmpty(indexName, "The index name is invalid");

        try {
            metadataLock.writeLock().lock();
            final long tableId = cache.tableCacheGet(tableName);
            metadata.deleteTableIndex(tableId, indexName);
            cache.invalidateSchemaCache(tableId);
            cache.invalidateIndicesCache(tableId);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    @Override
    public void renameTable(String curTableName, String newTableName) {
        try {
            metadataLock.writeLock().lock();
            long tableId = cache.tableCacheGet(curTableName);
            metadata.renameExistingTable(curTableName, newTableName);
            cache.invalidateTableCache(curTableName);
            cache.invalidateColumnsCache(tableId);
            cache.invalidateSchemaCache(tableId);
        } finally {
            metadataLock.writeLock().unlock();
        }
    }

    @Override
    public long getAutoInc(String tableName) {
        try {
            metadataLock.readLock().lock();
            long tableId = cache.tableCacheGet(tableName);
            try {
                autoIncrementLock.readLock().lock();
                return cache.autoIncCacheGet(tableId);
            } finally {
                autoIncrementLock.readLock().unlock();
            }
        } finally {
            metadataLock.readLock().unlock();
        }
    }

    @Override
    public void setAutoInc(String tableName, long value) {
        try {
            metadataLock.readLock().lock();
            long tableId = cache.tableCacheGet(tableName);
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
        } finally {
            metadataLock.readLock().unlock();
        }
    }

    @Override
    public long incrementAutoInc(String tableName, long amount) {
        long value;
        try {
            metadataLock.readLock().lock();
            long tableId = cache.tableCacheGet(tableName);
            try {
                autoIncrementLock.writeLock().lock();
                value = metadata.incrementAutoInc(tableId, amount);
                cache.updateAutoIncCache(tableId, value);
            } finally {
                autoIncrementLock.writeLock().unlock();
            }
        } finally {
            metadataLock.readLock().unlock();
        }
        return value;
    }

    @Override
    public void truncateAutoInc(String tableName) {
        try {
            metadataLock.readLock().lock();
            long tableId = cache.tableCacheGet(tableName);
            try {
                autoIncrementLock.writeLock().lock();
                metadata.setAutoInc(tableId, 1);
                cache.invalidateAutoIncCache(tableId);
            } finally {
                autoIncrementLock.writeLock().unlock();
            }
        } finally {
            metadataLock.readLock().unlock();
        }
    }

    @Override
    public long getRowCount(String tableName) {
        try {
            metadataLock.readLock().lock();
            long tableId = cache.tableCacheGet(tableName);
            try {
                rowsLock.readLock().lock();
                return cache.rowsCacheGet(tableId);
            } finally {
                rowsLock.readLock().unlock();
            }
        } finally {
            metadataLock.readLock().unlock();
        }
    }

    @Override
    public long incrementRowCount(String tableName, long amount) {
        long value;
        try {
            metadataLock.readLock().lock();
            long tableId = cache.tableCacheGet(tableName);
            try {
                rowsLock.writeLock().lock();
                value = metadata.incrementRowCount(tableId, amount);
                cache.updateRowsCache(tableId, value);
            } finally {
                rowsLock.writeLock().unlock();
            }
        } finally {
            metadataLock.readLock().unlock();
        }
        return value;
    }

    @Override
    public void truncateRowCount(String tableName) {
        try {
            metadataLock.readLock().lock();
            long tableId = cache.tableCacheGet(tableName);
            try {
                rowsLock.writeLock().lock();
                metadata.truncateRowCount(tableId);
                cache.invalidateRowsCache(tableId);
            } finally {
                rowsLock.writeLock().unlock();
            }
        } finally {
            metadataLock.readLock().unlock();
        }
    }
}
