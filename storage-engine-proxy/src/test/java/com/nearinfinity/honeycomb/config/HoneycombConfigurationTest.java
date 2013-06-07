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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class HoneycombConfigurationTest {

    private static Map<String, String> hbaseConfigs = new HashMap<String, String>() {{
        put("option1", "value1");
        put("option2", "value2");
    }};

    private static Map<String, Map<String, String>> adapterConfigs = new HashMap<String, Map<String, String>>() {{
        put(AdapterType.HBASE.getName(), hbaseConfigs);
    }};

    private HoneycombConfiguration configuration;

    @Before
    public void setupTests() {
        configuration = new HoneycombConfiguration(adapterConfigs, "hbase");
    }

    @Test
    public void testIsAdapterConfigured() throws Exception {
        Assert.assertTrue(configuration.isAdapterConfigured(AdapterType.HBASE));
    }

    @Test
    public void testIsAdapterNotConfigured() throws Exception {
        Assert.assertFalse(configuration.isAdapterConfigured(AdapterType.MEMORY));
    }

    @Test
    public void testGetAdapterOptions() throws Exception {
        Assert.assertEquals(hbaseConfigs, configuration.getAdapterOptions(AdapterType.HBASE));
    }
}
