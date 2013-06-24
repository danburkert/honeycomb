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


package com.nearinfinity.honeycomb.config;

import com.google.common.collect.Lists;

import java.io.File;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Configuration object which holds configuration options for the Honeycomb
 * system, not an individual adapter.
 */
public class HoneycombConfiguration {
    private final Map<String, Map<String, String>> adapters;
    private AdapterType defaultAdapter;

    /**
     * Create a new system configuration
     *
     * @param adapters       Map of adapter name to configuration
     * @param defaultAdapter Default adapter selected
     */
    public HoneycombConfiguration(Map<String, Map<String, String>> adapters,
                                  String defaultAdapter) {
        this.adapters = adapters;
        this.defaultAdapter = AdapterType.valueOf(defaultAdapter.toUpperCase());
    }

    /**
     * Query whether an adapter is available in the system.
     *
     * @param adapter Type of adapter
     * @return Adapter exists
     */
    public boolean isAdapterConfigured(AdapterType adapter) {
        checkNotNull(adapter);
        return adapters.containsKey(adapter.getName());
    }

    /**
     * Retrieve the options associated with a specific adapter.
     *
     * @param adapter Type of adapter
     * @return Configuration options
     */
    public Map<String, String> getAdapterOptions(AdapterType adapter) {
        checkNotNull(adapter);
        return adapters.get(adapter.getName());
    }

    /**
     * Retrieve the default adapter
     *
     * @return Default adapter type
     */
    public AdapterType getDefaultAdapter() {
        return defaultAdapter;
    }

    /**
     * Load honeycomb.xml.  Checks for honeycomb.xml and honeycomb.xsd in the
     * following locations:
     *
     *      /var/log/mysql
     *      $HONEYCOMB_CONFIGURATION
     *      /etc
     *      /etc/mysql
     *      $MYSQL_HOME
     *      $HOME
     *      cwd
     */
    public static HoneycombConfiguration create() {
        List<String> paths = Lists.newArrayList("/var/log/mysql");
        paths.add(System.getProperty("HONEYCOMB_CONFIGURATION", ""));
        paths.add("/etc");
        paths.add("/etc/mysql");
        paths.add(System.getProperty("MYSQL_HOME", ""));
        paths.add(System.getProperty("user.home", ""));
        paths.add(System.getProperty("user.dir", ""));

        File xml;
        File xsd;
        for (String path : paths) {
            xml = new File(path + "/honeycomb.xml");
            xsd = new File(path + "/honeycomb.xsd");

            if (xml.exists() && xsd.exists()) {
                return ConfigurationParser.parseConfiguration(xml.getPath(), xsd.getPath());
            }
        }

        throw new RuntimeException("Unable to locate Honeycomb.xml or Honeycomb.xsd");
    }
}
