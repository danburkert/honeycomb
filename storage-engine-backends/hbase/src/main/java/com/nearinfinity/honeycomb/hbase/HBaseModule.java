package com.nearinfinity.honeycomb.hbase;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.config.AdapterType;
import com.nearinfinity.honeycomb.exceptions.RuntimeIOException;
import com.nearinfinity.honeycomb.hbase.config.ConfigConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class HBaseModule extends AbstractModule {
    private static final Logger logger = Logger.getLogger(HBaseModule.class);
    private final HTableProvider hTableProvider;
    private final Configuration configuration;

    public HBaseModule(final Map<String, String> options) {
        // Add the HBase resources to the core application configuration
        configuration = HBaseConfiguration.create();

        for (Map.Entry<String, String> option : options.entrySet()) {
            configuration.set(option.getKey(), option.getValue());
        }

        hTableProvider = new HTableProvider(configuration);

        try {
            TableCreator.createTable(configuration);
        } catch (IOException e) {
            logger.fatal("Could not create HBase table. Aborting initialization.");
            logger.fatal(configuration.toString());
            throw new RuntimeIOException(e);
        }
    }

    @Override
    protected void configure() {
        final MapBinder<AdapterType, Store> storeMapBinder =
                MapBinder.newMapBinder(binder(), AdapterType.class, Store.class);

        storeMapBinder.addBinding(AdapterType.HBASE).to(HBaseStore.class);

        install(new FactoryModuleBuilder()
                .implement(Table.class, HBaseTable.class)
                .build(HBaseTableFactory.class));

        bind(HTableProvider.class).toInstance(hTableProvider);
        bind(HTableInterface.class).toProvider(hTableProvider);

        bind(Long.class).annotatedWith(Names.named(ConfigConstants.WRITE_BUFFER))
                .toInstance(configuration.getLong(ConfigConstants.WRITE_BUFFER,
                        ConfigConstants.DEFAULT_WRITE_BUFFER));
        bind(String.class).annotatedWith(Names.named(ConfigConstants.COLUMN_FAMILY))
                .toInstance(configuration.get(ConfigConstants.COLUMN_FAMILY));
    }
}
