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

import com.google.common.collect.Maps;
import net.java.quickcheck.Generator;
import net.java.quickcheck.collection.Pair;
import net.java.quickcheck.generator.CombinedGenerators;
import net.java.quickcheck.generator.PrimitiveGenerators;
import net.java.quickcheck.generator.iterable.Iterables;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class HoneycombConfigurationTest {

    Map<String, String> properties;
    HoneycombConfiguration configuration;

    @Test
    public void testIsBackendEnabled() throws Exception {
        properties = new HashMap<String, String>() {{
            put("honeycomb.hbase.enabled", "true");
            put("honeycomb.memory.enabled", "true");
        }};
        configuration = new HoneycombConfiguration(properties);
        Assert.assertTrue(configuration.isBackendEnabled(BackendType.HBASE));
        Assert.assertTrue(configuration.isBackendEnabled(BackendType.MEMORY));

        properties = new HashMap<String, String>() {{
            put("honeycomb.hbase.enabled", "TRUE");
            put("honeycomb.memory.enabled", "TRUE");
        }};
        configuration = new HoneycombConfiguration(properties);
        Assert.assertTrue(configuration.isBackendEnabled(BackendType.HBASE));
        Assert.assertTrue(configuration.isBackendEnabled(BackendType.MEMORY));


        properties = new HashMap<String, String>() {{
            put("honeycomb.hbase.enabled", "  TrUE      ");
            put("honeycomb.memory.enabled", "  TrUE      ");
        }};
        configuration = new HoneycombConfiguration(properties);
        Assert.assertTrue(configuration.isBackendEnabled(BackendType.HBASE));
        Assert.assertTrue(configuration.isBackendEnabled(BackendType.MEMORY));
    }

    @Test
    public void testIsBackendDisabled() throws Exception {
        properties = new HashMap<String, String>() {{
            put("honeycomb.hbase.enabled", "false");
            put("honeycomb.memory.enabled", "false");
        }};
        configuration = new HoneycombConfiguration(properties);
        Assert.assertFalse(configuration.isBackendEnabled(BackendType.HBASE));
        Assert.assertFalse(configuration.isBackendEnabled(BackendType.MEMORY));

        properties = new HashMap<String, String>() {{
            put("honeycomb.hbase.enabled", "FALSE");
            put("honeycomb.memory.enabled", "FALSE");
        }};
        configuration = new HoneycombConfiguration(properties);
        Assert.assertFalse(configuration.isBackendEnabled(BackendType.HBASE));
        Assert.assertFalse(configuration.isBackendEnabled(BackendType.MEMORY));

        properties = new HashMap<String, String>() {{
            put("honeycomb.hbase.enabled", "  FaLSe   ");
            put("honeycomb.memory.enabled", "  FaLSe   ");
        }};
        configuration = new HoneycombConfiguration(properties);
        Assert.assertFalse(configuration.isBackendEnabled(BackendType.HBASE));
        Assert.assertFalse(configuration.isBackendEnabled(BackendType.MEMORY));
    }

    @Test
    public void testBackendDisabledByDefault() throws Exception {
        properties = new HashMap<String, String>() {{
            put("honeycomb.hbase.enabled", "");
            put("honeycomb.memory.enabled", "");
        }};
        configuration = new HoneycombConfiguration(properties);
        Assert.assertFalse(configuration.isBackendEnabled(BackendType.HBASE));
        Assert.assertFalse(configuration.isBackendEnabled(BackendType.MEMORY));

        properties = new HashMap<String, String>() {{
            put("honeycomb.hbase.enabled", "t");
            put("honeycomb.memory.enabled", "t");
        }};
        configuration = new HoneycombConfiguration(properties);
        Assert.assertFalse(configuration.isBackendEnabled(BackendType.HBASE));
        Assert.assertFalse(configuration.isBackendEnabled(BackendType.MEMORY));

        properties = new HashMap<String, String>() {{
            put("honeycomb.hbase.enabled", "  foooz   ");
            put("honeycomb.memory.enabled", "  foooz   ");
        }};
        configuration = new HoneycombConfiguration(properties);
        Assert.assertFalse(configuration.isBackendEnabled(BackendType.HBASE));
        Assert.assertFalse(configuration.isBackendEnabled(BackendType.MEMORY));
    }

    @Test
    public void testArbitraryProperties() throws Exception {
        Generator<String> strings = PrimitiveGenerators.strings(1, 100);
        Generator<Pair<String, String>> propertyGen = CombinedGenerators.pairs(strings, strings);

        properties = Maps.newHashMap();

        for (Pair<String, String> property : Iterables.toIterable(propertyGen)) {
            properties.put(property.getFirst(), property.getSecond());
        }

        configuration = new HoneycombConfiguration(properties);
        Assert.assertEquals(properties, configuration.getProperties());
    }

    @Test
    public void testDefaultBackend() throws Exception {
        properties = new HashMap<String, String>() {{
            put("honeycomb.backends.default", "hbase");
        }};
        configuration = new HoneycombConfiguration(properties);
        Assert.assertEquals(configuration.getDefaultBackend(), BackendType.HBASE);

        properties = new HashMap<String, String>() {{
            put("honeycomb.backends.default", "  hBasE   ");
        }};
        configuration = new HoneycombConfiguration(properties);
        Assert.assertEquals(configuration.getDefaultBackend(), BackendType.HBASE);

        properties = new HashMap<String, String>() {{
            put("honeycomb.backends.default", "memory");
        }};
        configuration = new HoneycombConfiguration(properties);
        Assert.assertEquals(configuration.getDefaultBackend(), BackendType.MEMORY);

        properties = new HashMap<String, String>() {{
            put("honeycomb.backends.default", "  MemORy       ");
        }};
        configuration = new HoneycombConfiguration(properties);
        Assert.assertEquals(configuration.getDefaultBackend(), BackendType.MEMORY);
    }
}
