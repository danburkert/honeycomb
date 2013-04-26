package com.nearinfinity.honeycomb.hbase;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.MapBinder;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.config.AdaptorType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class HBaseModule extends AbstractModule {
    private static final Logger logger = Logger.getLogger(HBaseModule.class);
    private final HTableProvider hTableProvider;

    public HBaseModule(final Map<String, String> options) throws IOException {
        // Add the HBase resources to the core application configuration
        Configuration hBaseConfiguration = HBaseConfiguration.create();

        for (Map.Entry<String, String> option : options.entrySet()) {
            hBaseConfiguration.set(option.getKey(), option.getValue());
        }

        hTableProvider = new HTableProvider(hBaseConfiguration);

        try {
            TableCreator.createTable(hBaseConfiguration);
        } catch (IOException e) {
            logger.fatal("Could not create HBaseStore. Aborting initialization.");
            logger.fatal(hBaseConfiguration.toString());
            throw e;
        } catch (Exception e) {
            logger.fatal(hBaseConfiguration.toString());
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void configure() {
        final MapBinder<AdaptorType, Store> storeMapBinder =
                MapBinder.newMapBinder(binder(), AdaptorType.class, Store.class);

        storeMapBinder.addBinding(AdaptorType.HBASE).to(HBaseStore.class);

        install(new FactoryModuleBuilder()
                .implement(Table.class, HBaseTable.class)
                .build(HBaseTableFactory.class));

        bind(HTableProvider.class).toInstance(hTableProvider);
        bind(HTableInterface.class).toProvider(hTableProvider);
    }
}
