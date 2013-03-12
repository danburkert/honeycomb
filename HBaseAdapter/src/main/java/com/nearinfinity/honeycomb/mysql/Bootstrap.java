package com.nearinfinity.honeycomb.mysql;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.multibindings.MapBinder;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.hbase.HBaseModule;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import static java.text.MessageFormat.format;

public class Bootstrap extends AbstractModule {
    private static final String CONFIG_PATH = "/etc/mysql/honeycomb.xml";
    private static final Logger logger = Logger.getLogger(Bootstrap.class);
    private Configuration params;

    public static HandlerProxyFactory startup() throws ParserConfigurationException, SAXException, IOException {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.readConfiguration();
        Injector injector = Guice.createInjector(bootstrap);
        return injector.getInstance(HandlerProxyFactory.class);
    }

    public void readConfiguration() throws IOException, ParserConfigurationException, SAXException {
        File configFile = new File(CONFIG_PATH);
        try {
            if (!(configFile.exists() && configFile.canRead() && configFile.isFile())) {
                throw new FileNotFoundException(CONFIG_PATH + " doesn't exist or cannot be read.");
            }
            params = Util.readConfiguration(configFile);
            logger.info(format("Read in {0} parameters.", params.size()));
        } catch (ParserConfigurationException e) {
            logger.fatal("The xml parser was not configured properly.", e);
            throw e;
        } catch (SAXException e) {
            logger.fatal("Exception while trying to parse the config file.", e);
            throw e;
        }
    }

    @Override
    protected void configure() {
        MapBinder<String, Store> stores = MapBinder.newMapBinder(binder(), String.class, Store.class);
        try {
            HBaseModule hBaseModule = new HBaseModule(params, stores);
            install(hBaseModule);
        } catch (IOException e) {
            logger.fatal("Failure during HBase initialization.", e);
        }
    }
}
