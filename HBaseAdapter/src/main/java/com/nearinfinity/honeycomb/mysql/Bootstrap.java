package com.nearinfinity.honeycomb.mysql;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import com.nearinfinity.honeycomb.hbase.HBaseModule;
import com.nearinfinity.honeycomb.hbaseclient.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

import static java.text.MessageFormat.format;

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
     */
    public static HandlerProxyFactory startup() {
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
            throw new RuntimeException(e);
        }
    }

    private void readConfiguration() {
        File configFile = new File(CONFIG_PATH);
        if (!(configFile.exists() && configFile.canRead() && configFile.isFile())) {
            throw new RuntimeException(CONFIG_PATH + " doesn't exist or cannot be read.");
        }
        params = Util.readConfiguration(configFile);
        logger.info(format("Read in {0} parameters.", params.size()));
    }
}
