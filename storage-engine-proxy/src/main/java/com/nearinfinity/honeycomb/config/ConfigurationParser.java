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
import com.google.common.collect.Lists;
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
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * Provides capabilities for validation and parsing of the application configuration content
 */
public class ConfigurationParser {
    private static final Logger logger = Logger.getLogger(ConfigurationParser.class);

    /**
     * XPath query to find adapter names from document.
     */
    private static final String QUERY_ADAPTER_NAMES = "/options/adapters/adapter/@name";

    /**
     * XPath query to find adapter configuration options from document
     */
    private static final String QUERY_ADAPTER_CONFIG_NODES = "/options/adapters/adapter[@name='%s']/configuration/*";

    private static final String QUERY_DEFAULT_ADAPTER = "/options/defaultAdapter";

    private static final XPath xPath = XPathFactory.newInstance().newXPath();

    private ConfigurationParser() {}

    /**
     * Create a {@link HoneycombConfiguration} from a configuration file path and configuration
     * schema validator path.
     *
     * @param configURL Path to configuration file to be parsed
     * @param schemaURL Path to schema file to be validated against
     * @return ConfigurationParser object holding configuration options
     */
    public static HoneycombConfiguration parseConfiguration(URL configURL,
                                                            URL schemaURL) {

        final InputSupplier<? extends InputStream> configSupplier =
                Resources.newInputStreamSupplier(configURL);
        final InputSupplier<? extends InputStream> schemaSupplier =
                Resources.newInputStreamSupplier(schemaURL);

        checkValidConfig(configSupplier, schemaSupplier);
        Document doc = parseDocument(configSupplier);
        Map<String, Map<String, String>> adapters = parseAdapters(doc);
        String defaultAdapter = parseDefaultAdapter(doc);
        return new HoneycombConfiguration(adapters, defaultAdapter);
    }

    /**
     * Performs validation on the configuration content supplied by the
     * configuration supplier against the schema document provided by the
     * validation supplier.  Throws Runtime exception if validation fails.
     *
     * @param configSupplier The supplier that provides the configuration to inspect, not null
     * @param schemaSupplier The supplier that provides the schema used to inspect the configuration, not null
     */
    private static void checkValidConfig(final InputSupplier<? extends InputStream> configSupplier,
                                         final InputSupplier<? extends InputStream> schemaSupplier) {
        try {
            final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            final Schema schema = schemaFactory.newSchema(new StreamSource(schemaSupplier.getInput()));
            final Validator validator = schema.newValidator();
            validator.validate(new StreamSource(configSupplier.getInput()));
        } catch (Exception e) {
            logger.error("Unable to validate honeycomb configuration.", e);
            throw new RuntimeException("Exception while validating honeycomb configuration.", e);
        }
    }

    /**
     * Parses the application configuration and returns the XML document.
     * @param configSupplier File supplier containing application configuration
     * @return XML Document
     */
    private static Document parseDocument(final InputSupplier<? extends InputStream> configSupplier) {
        try {
            return DocumentBuilderFactory
                    .newInstance()
                    .newDocumentBuilder()
                    .parse(configSupplier.getInput());

        } catch (Exception e) {
            logger.error("Unable to parse honeycomb configuration.", e);
            throw new RuntimeException("Exception while parsing honeycomb configuration.", e);
        }
    }

    /**
     * Determines if the option name is already namespaced (contains a '.').
     * @param optionName
     * @return
     */
    private static boolean isNamespaced(String optionName) {
        return optionName.contains(".");
    }

    /**
     * Namespaces the given option name by {@value Constants#HONEYCOMB_NAMESPACE} and the adapter type.
     * @param adapterName
     * @param optionName
     * @return
     */
    private static String prependNamespace(String adapterName, String optionName) {
        return Constants.HONEYCOMB_NAMESPACE + "." + adapterName + "." + optionName;
    }

    private static Map<String, String> parseOptions(String adapterName, Document doc) {
        String optionsQuery = String.format(QUERY_ADAPTER_CONFIG_NODES, adapterName);
        NodeList optionNodes;
        try {
            optionNodes = (NodeList) xPath.evaluate(optionsQuery, doc, XPathConstants.NODESET);
        } catch (XPathExpressionException e) {
            logger.error("Unable to parse options for " + adapterName + " adapter.", e);
            throw new RuntimeException("Exception while parsing options for " + adapterName + " adapter.", e);
        }
        ImmutableMap.Builder<String, String> options = ImmutableMap.builder();

        for (int i = 0; i < optionNodes.getLength(); i++) {
            Node optionNode = optionNodes.item(i);
            if (optionNode.getNodeType() == Node.ELEMENT_NODE) {
                String optionName = optionNode.getNodeName();
                String namespacedOptionName = isNamespaced(optionName)
                        ? optionName
                        : prependNamespace(adapterName, optionName);
                options.put(namespacedOptionName, optionNode.getTextContent());
            }
        }

        return options.build();
    }

    private static Map<String, Map<String, String>> parseAdapters(Document doc) {
        // Extract adapter names from document
        NodeList adapterNameNodes;
        try {
            adapterNameNodes = (NodeList) xPath.evaluate(QUERY_ADAPTER_NAMES, doc, XPathConstants.NODESET);
        } catch (XPathExpressionException e) {
            logger.error("Unable to parse adapter names from honeycomb configuration.", e);
            throw new RuntimeException("Exception while parsing adapter names from honeycomb configuration.", e);
        }
        List<String> adapterNames = Lists.newArrayList();
        for (int i = 0; i < adapterNameNodes.getLength(); i++) {
            Node adapterNameNode = adapterNameNodes.item(i);
            if (adapterNameNode.getNodeType() == Node.ATTRIBUTE_NODE) {
                adapterNames.add(adapterNameNode.getNodeValue());
            }
        }

        // Extract individual adapter options from the document
        ImmutableMap.Builder<String, Map<String, String>> adapters = ImmutableMap.builder();
        for (String adapterName : adapterNames) {
            adapters.put(adapterName, parseOptions(adapterName, doc));
        }

        return adapters.build();
    }

    private static String parseDefaultAdapter(Document doc) {
        try {
            Node defaultAdapterNode = (Node) xPath.evaluate(QUERY_DEFAULT_ADAPTER, doc, XPathConstants.NODE);
            return defaultAdapterNode.getTextContent();
        } catch (XPathExpressionException e) {
            logger.error("Unable to parse default adapter from honeycomb configuration.", e);
            throw new RuntimeException("Exception while parsing default adapter from honeycomb configuration.", e);        }
    }
}
