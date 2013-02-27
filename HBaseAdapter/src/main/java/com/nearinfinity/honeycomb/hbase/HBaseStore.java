package com.nearinfinity.honeycomb.hbase;

import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.TableNotFoundException;
import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.hbaseclient.SqlTableCreator;
import com.nearinfinity.honeycomb.mysql.gen.TableMetadata;
import com.nearinfinity.honeycomb.mysqlengine.HTableFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

import java.io.IOException;

public class HBaseStore implements Store {

    private static HBaseStore instance = null;
    private final HTablePool hTablePool;
    private final String hTableName;

    private static final int DEFAULT_TABLE_POOL_SIZE = 5;
    private static final long DEFAULT_WRITE_BUFFER_SIZE = 5 * 1024 * 1024;

    protected HBaseStore(Configuration conf) throws IOException {
        hTableName = conf.get(Constants.HBASE_TABLE);
        String zkQuorum = conf.get(Constants.ZK_QUORUM);

        int poolSize = conf.getInt("honeycomb.pool_size", DEFAULT_TABLE_POOL_SIZE);
        long writeBuffer = conf.getLong("write_buffer_size", DEFAULT_WRITE_BUFFER_SIZE);
        boolean autoFlush = conf.getBoolean("flush_changes_immediately", false);

        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", zkQuorum);
        hbaseConf.set(Constants.HBASE_TABLE, hTableName);
        SqlTableCreator.initializeSqlTable(hbaseConf);
        hTablePool = new HTablePool(hbaseConf, poolSize,
                new HTableFactory(writeBuffer, autoFlush));
    }

    @Override
    public Store getStore(Configuration configuration) throws IOException {
        if (instance == null) {
            instance = new HBaseStore(configuration);
        }
        return instance;
    }

    @Override
    public Table open(String name) throws TableNotFoundException {
        return new HBaseTable(hTablePool.getTable(hTableName), name);
    }

    @Override
    public Table create(TableMetadata metadata) throws IOException {
        return null;
    }

    @Override
    public void deleteTable(String name) throws IOException {
    }
}