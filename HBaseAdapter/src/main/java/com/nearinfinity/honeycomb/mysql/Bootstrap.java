package com.nearinfinity.honeycomb.mysql;

import com.google.inject.*;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.multibindings.MapBinder;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.Table;
import com.nearinfinity.honeycomb.hbase.HBaseStore;
import com.nearinfinity.honeycomb.hbase.HBaseTable;
import com.nearinfinity.honeycomb.hbase.HBaseTableFactory;
import com.nearinfinity.honeycomb.hbase.HTableProvider;
import com.nearinfinity.honeycomb.hbaseclient.Constants;
import com.nearinfinity.honeycomb.hbaseclient.SqlTableCreator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import static java.text.MessageFormat.format;

public class Bootstrap extends AbstractModule {
    private static final String CONFIG_PATH = "/etc/mysql/honeycomb.xml";
    private static final int DEFAULT_TABLE_POOL_SIZE = 5;
    private static final long DEFAULT_WRITE_BUFFER_SIZE = 5 * 1024 * 1024;
    private static final Logger logger = Logger.getLogger(Bootstrap.class);
    private static HTablePool hTablePool;
    private HTableProvider hTableProvider;

    public static HandlerProxyFactory startup() throws IOException, SAXException, ParserConfigurationException {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.readConfiguration();
        Injector injector = Guice.createInjector(bootstrap);
        return injector.getInstance(HandlerProxyFactory.class);
    }

    public void readConfiguration() throws ParserConfigurationException, SAXException, IOException {
        File configFile = new File(CONFIG_PATH);
        Configuration params;
        try {
            if (!(configFile.exists() && configFile.canRead() && configFile.isFile())) {
                throw new FileNotFoundException(CONFIG_PATH + " doesn't exist or cannot be read.");
            }
            params = Util.readConfiguration(configFile);
            logger.info(format("Read in {0} parameters.", params.size()));
            //TODO: Check if HBase is available
            this.hTableProvider = new HTableProvider(params);

            String hTableName = params.get(Constants.HBASE_TABLE);
            String zkQuorum = params.get(Constants.ZK_QUORUM);
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", zkQuorum);
            configuration.set(Constants.HBASE_TABLE, hTableName);
            SqlTableCreator.initializeSqlTable(configuration);

        } catch (ParserConfigurationException e) {
            logger.fatal("The xml parser was not configured properly.", e);
            throw e;
        } catch (SAXException e) {
            logger.fatal("Exception while trying to parse the config file.", e);
            throw e;
        } catch (ZooKeeperConnectionException e) {
            logger.fatal("Could not connect to zookeeper. ", e);
            throw e;
        } catch (IOException e) {
            logger.fatal("Could not create HBaseStore. Aborting initialization.");
            throw e;
        }
    }

    @Override
    protected void configure() {
        MapBinder<String, Store> stores = MapBinder.newMapBinder(binder(), String.class, Store.class);
        stores.addBinding("hbase").to(HBaseStore.class);

        install(new FactoryModuleBuilder()
                .implement(Table.class, HBaseTable.class)
                .build(HBaseTableFactory.class));

        bind(new TypeLiteral<Provider<HTableInterface>>() {
        }).toInstance(this.hTableProvider);
        bind(HTableInterface.class).toProvider(this.hTableProvider);
    }
}
