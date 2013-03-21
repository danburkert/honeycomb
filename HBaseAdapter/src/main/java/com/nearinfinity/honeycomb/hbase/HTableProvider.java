package com.nearinfinity.honeycomb.hbase;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

import com.google.inject.Provider;
import com.nearinfinity.honeycomb.config.ConfigurationHolder;
import com.nearinfinity.honeycomb.mysqlengine.HTableFactory;

public class HTableProvider implements Provider<HTableInterface> {

    private final HTablePool tablePool;
    private final String tableName;

    public HTableProvider(final ConfigurationHolder configuration) {
        String hTableName = configuration.getStorageTableName();
        long writeBufferSize = configuration.getStorageWriteBufferSize();
        int poolSize = configuration.getStorageTablePoolSize();
        boolean autoFlush = configuration.getStorageAutoFlushChanges();

        tableName = hTableName;
        tablePool = new HTablePool(configuration.getConfiguration(), poolSize, new HTableFactory(writeBufferSize, autoFlush));
    }

    @Override
    public HTableInterface get() {
        return tablePool.getTable(tableName);
    }
}
