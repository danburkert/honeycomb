package com.nearinfinity.honeycomb.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.MapBinder;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.hbaseclient.SqlTableCreator;

public class HBaseModule extends AbstractModule {
    private static final Logger logger = Logger.getLogger(HBaseModule.class);
    private final HTableProvider hTableProvider;

    public HBaseModule(final Configuration configuration) throws IOException {
        hTableProvider = new HTableProvider(configuration);

        try {
            String hTableName = configuration.get(Constants.HBASE_TABLE);
            String zkQuorum = configuration.get(Constants.ZK_QUORUM);
            Configuration hBaseConfiguration = HBaseConfiguration.create();
            hBaseConfiguration.set("hbase.zookeeper.quorum", zkQuorum);
            hBaseConfiguration.set(Constants.HBASE_TABLE, hTableName);
            SqlTableCreator.initializeSqlTable(hBaseConfiguration);
        } catch (IOException e) {
            logger.fatal("Could not create HBaseStore. Aborting initialization.");
            throw e;
        }
    }

    @Override
    protected void configure() {
        final MapBinder<String, Store> storeMapBinder =
                MapBinder.newMapBinder(binder(), String.class, Store.class);

        storeMapBinder.addBinding(Constants.HBASE_TABLESPACE).to(HBaseStore.class);

        install(new FactoryModuleBuilder()
                .implement(Table.class, HBaseTable.class)
                .build(HBaseTableFactory.class));

        bind(HTableProvider.class).toInstance(hTableProvider);
        bind(HTableInterface.class).toProvider(hTableProvider);
    }
}
