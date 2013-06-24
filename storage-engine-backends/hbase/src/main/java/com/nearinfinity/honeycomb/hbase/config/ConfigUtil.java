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


package com.nearinfinity.honeycomb.hbase.config;

import com.nearinfinity.honeycomb.config.AdapterType;
import com.nearinfinity.honeycomb.config.HoneycombConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.Map;

public class ConfigUtil {
    public static Configuration combineConfiguration(Map<String, String> hcConfig,
                                                     Configuration hbaseConfig) {
        for (Map.Entry<String, String> option : hcConfig.entrySet()) {
            hbaseConfig.set(option.getKey(), option.getValue());
        }
        return hbaseConfig;
    }

    public static Configuration combineConfiguration(Map<String, String> hcConfig) {
        return combineConfiguration(hcConfig, HBaseConfiguration.create());
    }

    public static Configuration createConfiguration() {
        return combineConfiguration(
                HoneycombConfiguration.create().getAdapterOptions(AdapterType.HBASE),
                HBaseConfiguration.create()
        );
    }

    /**
     * Validates a single property in configuration.  If property is not present,
     * prints to standard error.
     * @param conf configuration
     * @param property property being checked
     * @param defaultVal default value for the property
     * @return property is valid
     */
    public static boolean validateProperty(Configuration conf, String property, Object defaultVal) {
        if (conf.get(property) == null) {
            String msg = property + " not configured.  ";
            if (defaultVal != null) {
                msg += "Using the default value (" + defaultVal + ").";
            }
            System.err.println(msg);
            return false;
        }
        return true;
    }
}
