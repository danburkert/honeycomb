package com.nearinfinity.honeycomb.hbase;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.config.ConfigConstants;
import com.nearinfinity.honeycomb.config.ConfigurationHolder;
import com.nearinfinity.honeycomb.config.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import java.io.IOException;

public class HBaseModule extends AbstractModule {
    private static final Logger logger = Logger.getLogger(HBaseModule.class);
    private final HTableProvider hTableProvider;
    private final ConfigurationHolder configHolder;

    public HBaseModule(final ConfigurationHolder configuration) throws IOException {
        // Add the HBase resources to the core application configuration
        Configuration hBaseConfiguration = HBaseConfiguration.addHbaseResources(configuration.getConfiguration());

        configHolder = new ConfigurationHolder(hBaseConfiguration);

        hTableProvider = new HTableProvider(configHolder);

        try {
            TableCreator.createTable(configHolder);
        } catch (IOException e) {
            logger.fatal("Could not create HBaseStore. Aborting initialization.");
            logger.fatal(configHolder.toString());
            throw e;
        } catch (Exception e) {
            logger.fatal(configHolder.toString());
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void configure() {
        final MapBinder<String, Store> storeMapBinder =
                MapBinder.newMapBinder(binder(), String.class, Store.class);

        storeMapBinder.addBinding(Constants.HBASE_TABLESPACE).to(HBaseStore.class);

        bind(Long.class).annotatedWith(Names.named(ConfigConstants.PROP_WRITE_BUFFER_SIZE))
                .toInstance(configHolder.getStorageWriteBufferSize());

        install(new FactoryModuleBuilder()
                .implement(Table.class, HBaseTable.class)
                .build(HBaseTableFactory.class));

        bind(HTableProvider.class).toInstance(hTableProvider);
        bind(HTableInterface.class).toProvider(hTableProvider);
    }
}
