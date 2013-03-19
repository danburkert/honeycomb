package com.nearinfinity.honeycomb.mysql;

import static java.text.MessageFormat.format;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import com.nearinfinity.honeycomb.hbase.HBaseModule;
import com.nearinfinity.honeycomb.hbaseclient.Constants;

public class Bootstrap extends AbstractModule {
    private static final String CONFIG_PATH = "/etc/mysql/honeycomb.xml";
    private static final Logger logger = Logger.getLogger(Bootstrap.class);
    private Configuration params;

    private Bootstrap() {
    }

    /**
     * The initial function called by JNI to wire-up the required object graph dependencies
     *
     * @return {@link HandlerProxyFactory} with all dependencies setup
     * @throws ParserConfigurationException If the XML parser could not be configured correctly
     * @throws SAXException                 If a {@link DocumentBuilder} cannot be created which satisfies the configuration requested
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
        bind(String.class).annotatedWith(Names.named(Constants.DEFAULT_TABLESPACE)).toInstance(Constants.HBASE_TABLESPACE);

        try {
            HBaseModule hBaseModule = new HBaseModule(params);
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
            logger.fatal("The XML parser was not configured properly.", e);
            throw e;
        } catch (SAXException e) {
            logger.fatal("Exception while trying to parse the config file.", e);
            throw e;
        }
    }
}
