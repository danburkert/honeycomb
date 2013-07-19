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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provides methods for validating and parsing configuration files
 */
public class ConfigParser {
    private static final Logger logger = Logger.getLogger(ConfigParser.class);
    private static final XPath xPath = XPathFactory.newInstance().newXPath();

    private ConfigParser() {}

    /**
     * Create a dictionary of properties from a configuration file URL.  Validates
     * the configuration file against and XSD before attempting to parse.
     *
     * @param configURL URL to configuration file to be parsed
     * @param schemaURL URL to schema file to be validated against
     * @return map containing configuration properties
     */
    public static Map<String, String> parse(URL configURL,
                                            URL schemaURL) {
        checkNotNull(configURL);
        checkNotNull(schemaURL);

        final InputSupplier<? extends InputStream> configSupplier =
                Resources.newInputStreamSupplier(configURL);
        final InputSupplier<? extends InputStream> schemaSupplier =
                Resources.newInputStreamSupplier(schemaURL);

        try {
            validateConfig(configSupplier, schemaSupplier);
        } catch (Exception e) {
            String msg = "Unable to validate " + configURL.toString();
            logger.error(msg);
            throw new RuntimeException(msg, e);
        }
        try {
            return parseProperties(configSupplier);
        } catch (Exception e) {
            String msg = "Unable to parse " + configURL.toString();
            logger.error(msg);
            throw new RuntimeException(msg, e);
        }
    }

    /**
     * Validates the configuration against the given schema.
     *
     * @param filename The filename of the configuration being validated
     * @param configSupplier The supplier that provides the configuration to inspect
     * @param schemaSupplier The supplier that provides the schema used to inspect the configuration
     */
    private static void validateConfig(final InputSupplier<? extends InputStream> configSupplier,
                                       final InputSupplier<? extends InputStream> schemaSupplier)
            throws Exception {
        final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        final Schema schema = schemaFactory.newSchema(new StreamSource(schemaSupplier.getInput()));
        final Validator validator = schema.newValidator();
        validator.validate(new StreamSource(configSupplier.getInput()));
    }

    /**
     * Parse the configuration file and return a map of property name to value
     * @param configSupplier Configuration file input stream
     * @return Map of properties
     * @throws Exception on parse error
     */
    private static Map<String, String> parseProperties(final InputSupplier<? extends InputStream> configSupplier)
            throws Exception {

        Document doc = DocumentBuilderFactory
                .newInstance()
                .newDocumentBuilder()
                .parse(configSupplier.getInput());

        NodeList propertyNodes;
        propertyNodes = (NodeList) xPath.evaluate("/configuration/property", doc, XPathConstants.NODESET);

        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();

        for (int i = 0; i < propertyNodes.getLength(); i++) {
            Node propertyNode = propertyNodes.item(i);
            if (propertyNode.getNodeType() == Node.ELEMENT_NODE) {
                NodeList fields = propertyNode.getChildNodes();
                String name = null;
                String value = null;
                for (int j = 0; j < fields.getLength(); j++) {
                    Node field = fields.item(j);
                    if (field.getNodeType() == Node.ELEMENT_NODE) {
                        if ("name".equals(field.getNodeName())) {
                            name = field.getChildNodes().item(0).getNodeValue();
                        } else if ("value".equals(field.getNodeName())) {
                            value = field.getChildNodes().item(0).getNodeValue();
                        }
                    }
                }
                properties.put(name, value);
            }
        }

        return properties.build();
    }

}
