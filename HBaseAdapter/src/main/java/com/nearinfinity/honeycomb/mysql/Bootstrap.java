package com.nearinfinity.honeycomb.mysql;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.hbase.HBaseModule;
import com.nearinfinity.honeycomb.hbaseclient.Constants;
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

    private Bootstrap() {
    }

    /**
     * The beginning function called by JNI to wire up all of the object graph dependencies.
     *
     * @return HandlerProxyFactory with all dependencies setup
     * @throws ParserConfigurationException Configuration xml was incorrect
     * @throws SAXException                 if a DocumentBuilder cannot be created which satisfies the configuration requested
     * @throws IOException                  An IO exception occurred during parsing or the file was not found
     */
    public static HandlerProxyFactory startup() throws ParserConfigurationException, SAXException, IOException {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.readConfiguration();
        Injector injector = Guice.createInjector(bootstrap);
        return injector.getInstance(HandlerProxyFactory.class);
    }

    @Override
    protected void configure() {
        MapBinder<String, Store> stores = MapBinder.newMapBinder(binder(), String.class, Store.class);
        bind(String.class).annotatedWith(Names.named(Constants.DEFAULT_TABLESPACE)).toInstance(Constants.HBASE_TABLESPACE);
        try {
            HBaseModule hBaseModule = new HBaseModule(params, stores);
            install(hBaseModule);
        } catch (IOException e) {
            logger.fatal("Failure during HBase initialization.", e);
        }
    }

    private void readConfiguration() throws IOException, ParserConfigurationException, SAXException {
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
}
