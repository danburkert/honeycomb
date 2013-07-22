/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Copyright 2013 Near Infinity Corporation.
 */


package com.nearinfinity.honeycomb;

import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.nearinfinity.honeycomb.config.BackendType;
import com.nearinfinity.honeycomb.config.ConfigParser;
import com.nearinfinity.honeycomb.config.Constants;
import com.nearinfinity.honeycomb.config.HoneycombConfiguration;
import com.nearinfinity.honeycomb.exceptions.StorageBackendCreationException;
import com.nearinfinity.honeycomb.mysql.HandlerProxyFactory;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.Enumeration;
import java.util.Map;


/**
 * Serves as the initial starting point of the storage engine proxy that is
 * used to store the configuration and bootstrap the application
 */
public final class Bootstrap extends AbstractModule {
    private static final Logger logger = Logger.getLogger(Bootstrap.class);
    private final HoneycombConfiguration configuration;

    private Bootstrap(HoneycombConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * The initial function called by JNI to wire-up the required object graph
     * dependencies
     *
     * @return {@link HandlerProxyFactory} with all dependencies setup
     */
    public static HandlerProxyFactory startup() {
        ensureLoggingPathsCorrect();
        ClassLoader loader = Bootstrap.class.getClassLoader();
        URL configURL = loader.getResource(Constants.HONEYCOMB_SITE);
        URL defaultURL = loader.getResource(Constants.HONEYCOMB_DEFAULT);
        URL schemaURL = loader.getResource(Constants.CONFIG_SCHEMA);

        Map<String, String> properties = Maps.newHashMap();
        tryExtractDefaultProperties(defaultURL, schemaURL, properties);
        tryExtractClasspathProperties(configURL, schemaURL, properties);
        tryExtractEnvironmentProperties(schemaURL, properties);

        Bootstrap bootstrap = new Bootstrap(new HoneycombConfiguration(properties));

        Injector injector = Guice.createInjector(bootstrap);
        return injector.getInstance(HandlerProxyFactory.class);
    }

    private static void tryExtractEnvironmentProperties(URL schemaURL, Map<String, String> properties) {
        try {
            Map<String, String> env = System.getenv();
            String filePath = env.get(Constants.HONEYCOMB_ENVIRONMENT);
            if (filePath != null) {
                Map<String, String> parse = ConfigParser.parse(filePath, schemaURL);
                if (parse != null) {
                    properties.putAll(parse);
                }
            }
        } catch (Exception e) {
            logger.warn("When trying to read from environmental variable.", e);
        }
    }

    private static void tryExtractClasspathProperties(URL configURL, URL schemaURL, Map<String, String> properties) {
        if (configURL != null) {
            properties.putAll(ConfigParser.parse(configURL, schemaURL));
        } else {
            String msg = "Unable to find " + Constants.HONEYCOMB_SITE + " on the classpath.";
            logger.warn(msg);
        }
    }

    private static void tryExtractDefaultProperties(URL defaultURL, URL schemaURL, Map<String, String> properties) {
        if (defaultURL == null || schemaURL == null) {
            String msg = "Unable to find " + (defaultURL == null ?
                    Constants.HONEYCOMB_DEFAULT : Constants.CONFIG_SCHEMA) + " on the classpath.";
            logger.error(msg);
            logger.info("Classpath:\n" + System.getProperty("java.class.path").replace(':', '\n'));
            throw new IllegalStateException(msg);
        }

        properties.putAll(ConfigParser.parse(defaultURL, schemaURL));
    }

    private static void ensureLoggingPathsCorrect() {
        Enumeration<?> allAppenders = Logger.getRootLogger().getAllAppenders();
        while (allAppenders.hasMoreElements()) {
            Appender appender = (Appender) allAppenders.nextElement();
            if (appender instanceof FileAppender) {
                FileAppender fileAppender = (FileAppender) appender;
                String file = fileAppender.getFile();
                if (file == null) {
                    continue;
                }
                File f = new File(file);
                if (f.exists()) {
                    if (!f.canWrite()) {
                        System.err.println("Logger unable to write to " + f.getName());
                    }
                } else {
                    String createFailure = "Logger unable to create file " + f.getName();
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

    @Override
    protected void configure() {
        bind(HoneycombConfiguration.class).toInstance(configuration);

        for (BackendType adapter : BackendType.values()) {
            if (configuration.isBackendEnabled(adapter)) {
                try {
                    Class<?> moduleClass = Class.forName(adapter.getModuleClass());
                    Constructor<?> moduleCtor = moduleClass.getConstructor(Map.class);
                    Object module = moduleCtor.newInstance(configuration.getProperties());
                    install((Module) module);
                } catch (ClassNotFoundException e) {
                    logger.error("The " + adapter.getName() + " adapter is" +
                            " enabled, but could not be found on the classpath.");
                    throw new StorageBackendCreationException(adapter.getName(), e);
                } catch (Exception e) {
                    logger.error("Exception while attempting to reflect on the "
                            + adapter.getName() + " adapter.", e);
                    throw new StorageBackendCreationException(adapter.getName(), e);
                }
            }
        }
    }
}
