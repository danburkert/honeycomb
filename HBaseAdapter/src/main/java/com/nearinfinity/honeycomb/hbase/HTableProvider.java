package com.nearinfinity.honeycomb.hbase;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

import com.google.inject.Provider;
import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.mysqlengine.HTableFactory;

public class HTableProvider implements Provider<HTableInterface> {
    private static final int DEFAULT_TABLE_POOL_SIZE = 5;
    private static final long DEFAULT_WRITE_BUFFER_SIZE = 5 * 1024 * 1024;
    private final HTablePool tablePool;
    private final String tableName;

    public HTableProvider(Configuration configuration) {
        String hTableName = configuration.get(Constants.HBASE_TABLE);
        long writeBufferSize = configuration.getLong("write_buffer_size", DEFAULT_WRITE_BUFFER_SIZE);
        int poolSize = configuration.getInt("honeycomb.pool_size", DEFAULT_TABLE_POOL_SIZE);
        boolean autoFlush = configuration.getBoolean("flush_changes_immediately", false);

        tableName = checkNotNull(hTableName, String.format("The configuration option, %s, was not specified", Constants.HBASE_TABLE));
        tablePool = new HTablePool(configuration, poolSize, new HTableFactory(writeBufferSize, autoFlush));
    }

    @Override
    public HTableInterface get() {
        return tablePool.getTable(tableName);
    }
}
