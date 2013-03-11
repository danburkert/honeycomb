package com.nearinfinity.honeycomb.hbase;

import com.google.inject.Provider;
import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.mysqlengine.HTableFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

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
        this.tablePool = new HTablePool(configuration, poolSize, new HTableFactory(writeBufferSize, autoFlush));
        this.tableName = hTableName;
    }

    @Override
    public HTableInterface get() {
        return this.tablePool.getTable(this.tableName);
    }
}
