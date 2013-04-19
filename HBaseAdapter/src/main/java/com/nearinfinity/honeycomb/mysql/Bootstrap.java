package com.nearinfinity.honeycomb.mysql;

import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import com.nearinfinity.honeycomb.config.ConfigurationHolder;
import com.nearinfinity.honeycomb.config.ConfigurationParser;
import com.nearinfinity.honeycomb.config.Constants;
import com.nearinfinity.honeycomb.hbase.HBaseModule;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;

import static java.lang.String.format;


public final class Bootstrap extends AbstractModule {
    private static final Logger logger = Logger.getLogger(Bootstrap.class);
    private final String configFilename;
    private final String configSchema;
    private ConfigurationHolder configHolder;

    private Bootstrap(String configFilename, String configSchema) {
        this.configFilename = configFilename;
        this.configSchema = configSchema;
    }

    /**
     * The initial function called by JNI to wire-up the required object graph dependencies
     *
     * @return {@link HandlerProxyFactory} with all dependencies setup
     */
    public static HandlerProxyFactory startup(String configFilename, String configSchema) {
        ensureLoggingPathsCorrect();
        Bootstrap bootstrap = new Bootstrap(configFilename, configSchema);
        bootstrap.readConfiguration();

        Injector injector = Guice.createInjector(bootstrap);
        return injector.getInstance(HandlerProxyFactory.class);
    }

    private static void ensureLoggingPathsCorrect() {
        System.err.println("Testing the file paths for log4j");
        Enumeration allAppenders = Logger.getRootLogger().getAllAppenders();
        while (allAppenders.hasMoreElements()) {
            Appender appender = (Appender) allAppenders.nextElement();
            if (appender instanceof FileAppender) {
                FileAppender fileAppender = (FileAppender) appender;
                File f = new File(fileAppender.getFile());
                System.err.println("Testing: " + f.getName());
                if (f.exists()) {
                    if (!f.canWrite())
                        System.err.println("Cannot write to " + f.getName());
                } else {
                    String createFailure = "Could not create logging file " + f.getName();
                    try {
                        if (!f.createNewFile()) {
                            System.err.println(createFailure);
                            throw new RuntimeException(createFailure);
                        }
                    } catch (IOException e) {
                        System.err.println(createFailure);
                        throw new RuntimeException(createFailure, e);
                    }
                }
            }
        }
    }

    /**
     * Determines if the specified file is accessible and available for reading
     *
     * @param file The file to inspect
     * @return True if file is available, False otherwise
     */

    private static boolean isFileAvailable(final File file) {
        if (!(file.exists() && file.canRead() && file.isFile())) {
            final String errorMsg = format("File is not available: %s", file.getAbsolutePath());
            logger.fatal(errorMsg);
            throw new RuntimeException(errorMsg);
        }

        return true;
    }

    @Override
    protected void configure() {
        bind(String.class).annotatedWith(Names.named(Constants.DEFAULT_TABLESPACE)).toInstance(Constants.HBASE_TABLESPACE);

        // Setup the HBase bindings only if the adapter has been configured
        if (configHolder.isAdapterConfigured(Constants.HBASE_TABLESPACE)) {
            try {
                HBaseModule hBaseModule = new HBaseModule(configHolder);
                install(hBaseModule);
            } catch (IOException e) {
                logger.fatal("Failure during HBase initialization.", e);
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Initiates the process for reading the application configuration data
     */
    private void readConfiguration() {
        final File configFile = new File(configFilename);
        final File configSchemaFile = new File(configSchema);

        if (isFileAvailable(configFile) && isFileAvailable(configSchemaFile)) {

            final InputSupplier<FileInputStream> schemaSupplier = Files.newInputStreamSupplier(configSchemaFile);
            final InputSupplier<FileInputStream> configSupplier = Files.newInputStreamSupplier(configFile);

            if (ConfigurationParser.validateConfiguration(schemaSupplier, configSupplier)) {
                final ConfigurationParser configParser = new ConfigurationParser();
                configHolder = configParser.parseConfiguration(configSupplier, new Configuration());

                logger.info(String.format("Honeycomb configuration: %s", configHolder.toString()));
                logger.debug(format("Read %d configuration properties ",
                        configHolder.getConfiguration().size()));
            } else {
                final String errorMsg = format("Configuration file validation failed. Check %s for correctness.", configFile.getPath());
                logger.fatal(errorMsg);
                throw new RuntimeException(errorMsg);
            }
        }
    }
}
