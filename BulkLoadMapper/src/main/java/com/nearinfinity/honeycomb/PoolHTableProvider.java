package com.nearinfinity.honeycomb;

import com.google.inject.Provider;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

import static com.google.common.base.Preconditions.checkNotNull;

public class PoolHTableProvider implements Provider<HTableInterface> {
    private final HTablePool pool;
    private final String tableName;
    public PoolHTableProvider(String tableName, HTablePool pool) {
        checkNotNull(tableName);
        checkNotNull(pool);
        this.pool = pool;
        this.tableName = tableName;
    }
    @Override
    public HTableInterface get() {
        return pool.getTable(tableName);
    }
}
