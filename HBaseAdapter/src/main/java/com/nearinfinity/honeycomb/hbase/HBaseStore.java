package com.nearinfinity.honeycomb.hbase;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.BiMap;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.TableNotFoundException;
import com.nearinfinity.honeycomb.mysql.gen.TableSchema;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class HBaseStore implements Store {
    private static final String HBASE_TABLE = "nic";
    private final HBaseMetadata metadata;
    private final HBaseTableFactory tableFactory;
    private LoadingCache<String, Long> tableCache;
    private LoadingCache<Long, BiMap<String, Long>> columnsCache;
    private LoadingCache<Long, Long> rowsCache;
    private LoadingCache<Long, Long> autoIncCache;
    private LoadingCache<Long, TableSchema> schemaCache;

    public HBaseStore(HBaseMetadata metadata, HBaseTableFactory tableFactory) {
        this.metadata = metadata;
        this.tableFactory = tableFactory;
        this.doInitialization();
    }

    public long getTableId(String tableName) throws ExecutionException {
        return this.tableCache.get(tableName);
    }

    @Override
    public Table openTable(String tableName) throws Exception {
        return this.tableFactory.create(tableName);
    }

    @Override
    public TableSchema getTableMetadata(String tableName) throws Exception {
        return schemaCache.get(tableCache.get(tableName));
    }

    @Override
    public void createTable(String tableName, TableSchema schema) throws Exception {
        getHBaseMetadata().putSchema(tableName, schema);
    }

    @Override
    public void deleteTable(String tableName) throws Exception {
        HBaseMetadata metadata = getHBaseMetadata();
        metadata.deleteSchema(tableName);
        invalidateCache(tableName);

    }

    @Override
    public void alterTable(String tableName, TableSchema schema) throws Exception {
        long tableId = tableCache.get(tableName);
        HBaseMetadata metadata = getHBaseMetadata();
        metadata.updateSchema(tableId, schemaCache.get(tableId), schema);
        invalidateCache(tableName);

    }

    @Override
    public void renameTable(String curTableName, String newTableName) throws Exception {
        HBaseMetadata metadata = getHBaseMetadata();
        metadata.renameExistingTable(curTableName, newTableName);
        invalidateCache(curTableName);

    }

    @Override
    public long getAutoInc(String tableName) throws Exception {
        return autoIncCache.get(tableCache.get(tableName));
    }

    @Override
    public long incrementAutoInc(String tableName, long amount) throws Exception {
        long tableId = tableCache.get(tableName);
        HBaseMetadata metadata = getHBaseMetadata();
        long value = metadata.incrementAutoInc(tableId, amount);
        autoIncCache.put(tableId, value);
        return value;
    }

    @Override
    public void truncateAutoInc(String tableName) throws Exception {
        long tableId = tableCache.get(tableName);
        HBaseMetadata metadata = getHBaseMetadata();
        metadata.truncateAutoInc(tableId);
        autoIncCache.invalidate(tableId);
    }

    @Override
    public long getRowCount(String tableName) throws Exception {
        return rowsCache.get(tableCache.get(tableName));
    }

    public long incrementRowCount(long tableId, long amount) throws Exception {
        HBaseMetadata metadata = getHBaseMetadata();
        long value = metadata.incrementRowCount(tableId, amount);
        rowsCache.put(tableId, value);

        return value;
    }

    public void truncateRowCount(long tableId) throws Exception {
        HBaseMetadata metadata = getHBaseMetadata();
        metadata.truncateRowCount(tableId);
        rowsCache.invalidate(tableId);
    }

    private void doInitialization() {
        tableCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<String, Long>() {
                    @Override
                    public Long load(String tableName)
                            throws IOException, TableNotFoundException {
                        return getHBaseMetadata().getTableId(tableName);
                    }
                }
                );

        columnsCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<Long, BiMap<String, Long>>() {
                    @Override
                    public BiMap<String, Long> load(Long tableId)
                            throws IOException, TableNotFoundException {

                        return getHBaseMetadata().getColumnIds(tableId);
                    }
                }
                );

        autoIncCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<Long, Long>() {
                    @Override
                    public Long load(Long tableId)
                            throws IOException, TableNotFoundException {
                        return getHBaseMetadata().getAutoInc(tableId);
                    }
                }
                );

        rowsCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<Long, Long>() {
                    @Override
                    public Long load(Long tableId)
                            throws IOException, TableNotFoundException {
                        return getHBaseMetadata().getRowCount(tableId);
                    }
                }
                );

        schemaCache = CacheBuilder
                .newBuilder()
                .build(new CacheLoader<Long, TableSchema>() {
                    @Override
                    public TableSchema load(Long tableId)
                            throws IOException, TableNotFoundException {
                        return getHBaseMetadata().getSchema(tableId);

                    }
                }
                );
    }

    private void invalidateCache(String tableName) throws Exception {
        long tableId = tableCache.get(tableName);
        tableCache.invalidate(tableName);
        columnsCache.invalidate(tableId);
        schemaCache.invalidate(tableId);
    }

    private HBaseMetadata getHBaseMetadata() {
        return this.metadata;
    }
}