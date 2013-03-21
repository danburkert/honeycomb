package com.nearinfinity.honeycomb.config;


import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Before;
import org.junit.Test;

public class ConfigurationHolderTest {

    private static final String VALUE_TEST = "test";
    private static final String VALUE_BEFORE = "before";
    private static final String VALUE_AFTER = "after";

    private static final String PROP_FOO = "foo";
    private static final String PROP_BAR = "bar";

    private Configuration conf;

    @Before
    public void setupTestCase() {
        conf = HBaseConfiguration.create();
    }

    @SuppressWarnings("unused")
    @Test(expected = NullPointerException.class)
    public void testConstructionNullConfiguration() {
        new ConfigurationHolder(null);
    }

    @Test
    public void testImmutabilityAfterConstruction() {
        conf.set(PROP_FOO, VALUE_BEFORE);

        final ConfigurationHolder config = new ConfigurationHolder(conf);

        // Change the property value on the reference used for construction
        conf.set(PROP_FOO, VALUE_AFTER);

        // Verify that the configuration hasn't been mutated
        assertEquals(VALUE_BEFORE, config.getConfiguration().get(PROP_FOO));
    }


    @Test
    public void testImmutabilityAfterRetrieval() {
        conf.set(PROP_FOO, VALUE_BEFORE);

        final ConfigurationHolder config = new ConfigurationHolder(conf);
        final Configuration retrievedConfig = config.getConfiguration();

        // Change the property value on the returned reference
        retrievedConfig.set(PROP_FOO, VALUE_AFTER);

        // Verify that the configuration hasn't been mutated
        assertEquals(VALUE_BEFORE, config.getConfiguration().get(PROP_FOO));
    }


    @Test
    public void testCopyAndUpdateExistingConfiguration() {
        conf.set(PROP_FOO, VALUE_BEFORE);

        final ConfigurationHolder config = new ConfigurationHolder(conf);
        final Configuration retrievedConfig = config.getConfiguration();

        // Add a new property to the returned reference
        retrievedConfig.set(PROP_BAR, VALUE_TEST);

        final ConfigurationHolder newConfig = new ConfigurationHolder(retrievedConfig);

        // Verify that the new configuration contains the properties
        assertEquals(VALUE_BEFORE, newConfig.getConfiguration().get(PROP_FOO));
        assertEquals(VALUE_TEST, newConfig.getConfiguration().get(PROP_BAR));
    }
}
