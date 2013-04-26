package com.nearinfinity.honeycomb.hbase;

import com.google.inject.Provider;
import com.nearinfinity.honeycomb.config.ConfigConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

public class HTableProvider implements Provider<HTableInterface> {

    private final HTablePool tablePool;
    private final String tableName;

    public HTableProvider(final Configuration configuration) {
        String hTableName = configuration.get(ConfigConstants.TABLE_NAME);
        long writeBufferSize = configuration.getLong(ConfigConstants.WRITE_BUFFER,
                ConfigConstants.DEFAULT_WRITE_BUFFER);
        int poolSize = configuration.getInt(ConfigConstants.TABLE_POOL_SIZE,
                ConfigConstants.DEFAULT_TABLE_POOL_SIZE);
        boolean autoFlush = configuration.getBoolean(ConfigConstants.AUTO_FLUSH,
                ConfigConstants.DEFAULT_AUTO_FLUSH);

        tableName = hTableName;
        tablePool = new HTablePool(configuration, poolSize,
                new HTableFactory(writeBufferSize, autoFlush));
    }

    @Override
    public HTableInterface get() {
        return tablePool.getTable(tableName);
    }
}
