package com.nearinfinity.honeycomb.hbase;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

import com.google.inject.Provider;
import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.mysqlengine.HTableFactory;

public class HTableProvider implements Provider<HTableInterface> {

    private static final String PROP_AUTO_FLUSH_CHANGES = "flush_changes_immediately";
    private static final String PROP_WRITE_BUFFER_SIZE = "write_buffer_size";
    private static final String PROP_TABLE_POOL_SIZE = "honeycomb.pool_size";

    /**
     * Default number of references to keep active for a table
     */
    private static final int DEFAULT_TABLE_POOL_SIZE = 5;

    /**
     * Default number of bytes used for write buffer storage
     */
    private static final long DEFAULT_WRITE_BUFFER_SIZE = 5 * 1024 * 1024;

    private final HTablePool tablePool;
    private final String tableName;

    public HTableProvider(final Configuration configuration) {
        String hTableName = configuration.get(Constants.HBASE_TABLE);
        long writeBufferSize = configuration.getLong(PROP_WRITE_BUFFER_SIZE, DEFAULT_WRITE_BUFFER_SIZE);
        int poolSize = configuration.getInt(PROP_TABLE_POOL_SIZE, DEFAULT_TABLE_POOL_SIZE);
        boolean autoFlush = configuration.getBoolean(PROP_AUTO_FLUSH_CHANGES, false);

        tableName = checkNotNull(hTableName, String.format("The configuration option, %s, was not specified", Constants.HBASE_TABLE));
        tablePool = new HTablePool(configuration, poolSize, new HTableFactory(writeBufferSize, autoFlush));
    }

    @Override
    public HTableInterface get() {
        return tablePool.getTable(tableName);
    }
}
