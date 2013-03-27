package com.nearinfinity.honeycomb.config;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;

public class ConfigurationParserTest {

    private static final String ELEMENT_VALID = "<foo/>";
    private static final String ELEMENT_INVALID = "</>";

    private static final String PROP_TABLE_NAME = "tableName";

    private static final String CONFIGURED_ADAPTER = "hbase";
    private static final String DUMMY_TABLE_NAME = "dummy";

    private static final String ROOT_XML_DOC = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";

    private static final String SCHEMA_DEFINITION = ROOT_XML_DOC +
            "<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">" +
            "<xs:element name=\"foo\" type=\"xs:string\" />" +
            "</xs:schema>";

    private static final String ADAPTER_CONFIG_DOC = "<options><adapters>" +
            format("<adapter name=\"%s\">", CONFIGURED_ADAPTER) +
            "<configuration>" +
            format("<%s>%s</%s>", PROP_TABLE_NAME, DUMMY_TABLE_NAME, PROP_TABLE_NAME) +
            "</configuration></adapter></adapters></options>";

    private Configuration config;
    private ConfigurationParser parser;

    @Mock
    private InputSupplier<InputStream> supplier;

    @Before
    public void setupTestCase() {
        MockitoAnnotations.initMocks(this);
        config = new Configuration();
        parser = new ConfigurationParser();
    }

    @Test(expected = NullPointerException.class)
    public void testValidateConfigurationNullValidation() {
        ConfigurationParser.validateConfiguration(null, supplier);
    }

    @Test(expected = NullPointerException.class)
    public void testValidateConfigurationNullConfig() {
        ConfigurationParser.validateConfiguration(supplier, null);
    }

    @Test
    public void testValidateConfiguration() {
        final String validDocument = ROOT_XML_DOC + ELEMENT_VALID;

        assertTrue(ConfigurationParser.validateConfiguration(
                ByteStreams.newInputStreamSupplier(SCHEMA_DEFINITION.getBytes(Charsets.UTF_8)),
                ByteStreams.newInputStreamSupplier(validDocument.getBytes(Charsets.UTF_8))));
    }

    @Test
    public void testValidateConfigurationInvalidElement() {
        final String invalidDocument = ROOT_XML_DOC + ELEMENT_INVALID;

        assertFalse(ConfigurationParser.validateConfiguration(
                ByteStreams.newInputStreamSupplier(SCHEMA_DEFINITION.getBytes(Charsets.UTF_8)),
                ByteStreams.newInputStreamSupplier(invalidDocument.getBytes(Charsets.UTF_8))));
    }

    @Test
    public void testParseConfigurationInvalidDocument() {
        // Add invalid element to document to cause parse exception
        final String invalidDocument = ROOT_XML_DOC + ADAPTER_CONFIG_DOC + ELEMENT_INVALID;

        final ConfigurationHolder holder = parser.parseConfiguration(
                ByteStreams.newInputStreamSupplier(invalidDocument.getBytes(Charsets.UTF_8)), config);

        assertFalse(holder.isAdapterConfigured(CONFIGURED_ADAPTER));
    }

    @Test
    public void testParseConfiguration() {
        final String configDocument = ROOT_XML_DOC + ADAPTER_CONFIG_DOC;

        final ConfigurationHolder holder = parser.parseConfiguration(
                ByteStreams.newInputStreamSupplier(configDocument.getBytes(Charsets.UTF_8)), config);

        assertTrue(holder.isAdapterConfigured(CONFIGURED_ADAPTER));
        assertEquals(DUMMY_TABLE_NAME, holder.getStorageTableName());
    }

    @Test
    public void testParseConfigurationIgnorePropertyOverwrite() {
        final String configDocument = ROOT_XML_DOC + ADAPTER_CONFIG_DOC;

        // Set the property that will be found in the configuration
        final String initialTableName = "test";
        config.set(format("%s.%s", CONFIGURED_ADAPTER, PROP_TABLE_NAME), initialTableName);

        final ConfigurationHolder holder = parser.parseConfiguration(
                ByteStreams.newInputStreamSupplier(configDocument.getBytes(Charsets.UTF_8)), config);

        assertTrue(holder.isAdapterConfigured(CONFIGURED_ADAPTER));

        // Verify that the original configured table name has been unmodified
        assertNotEquals(DUMMY_TABLE_NAME, holder.getStorageTableName());
        assertEquals(initialTableName, holder.getStorageTableName());
    }
}
