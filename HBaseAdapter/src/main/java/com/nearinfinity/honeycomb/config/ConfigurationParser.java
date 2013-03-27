package com.nearinfinity.honeycomb.config;

import com.google.common.collect.Lists;
import com.google.common.io.InputSupplier;
import net.jcip.annotations.NotThreadSafe;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provides capabilities for validation and parsing of the application configuration content
 */
@NotThreadSafe
public class ConfigurationParser {

    private static final Logger logger = Logger.getLogger(ConfigurationParser.class);
    /**
     * XPath expression for extracting the adapter names from the document
     */
    private static final String XPATH_ADAPTER_NAME_ATTR = "/options/adapters/adapter/@name";
    /**
     * XPath expression for extracting the configuration details for a specific adapter
     */
    private static final String XPATH_ADAPTER_CONFIG_NODES = "/options/adapters/adapter[@name='%s']/configuration/*";
    private final DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
    private final XPath xpath = XPathFactory.newInstance().newXPath();

    /**
     * Performs validation on the configuration content supplied by the configuration supplier against
     * the schema document provided by the validation supplier
     *
     * @param validationSupplier The supplier that provides the schema used to inspect the configuration, not null
     * @param configSupplier     The supplier that provides the configuration to inspect, not null
     * @return True if the configuration is validated, False otherwise
     */
    public static boolean validateConfiguration(final InputSupplier<? extends InputStream> validationSupplier,
                                                final InputSupplier<? extends InputStream> configSupplier) {
        checkNotNull(validationSupplier, "The validation supplier is invalid");
        checkNotNull(configSupplier, "The configuration supplier is invalid");

        boolean validated = false;

        try {
            final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            final Schema schema = schemaFactory.newSchema(new StreamSource(validationSupplier.getInput()));
            final Validator validator = schema.newValidator();
            validator.validate(new StreamSource(configSupplier.getInput()));
            validated = true;
        } catch (SAXException e) {
            logger.error("Parse error occurred during validation", e);
        } catch (IOException e) {
            logger.error("IO error occurred while processing the source being validated", e);
        }

        return validated;
    }

    /**
     * Performs the operations necessary to parse the configuration content supplied by the
     * configuration supplier and stores the resultant data in the provided configuration container
     *
     * @param configSupplier The supplier that provides the configuration to parse, not null
     * @param config         The container used to store the parsed data, not null
     * @return A {@link ConfigurationHolder} object that holds the parsed information
     */
    public ConfigurationHolder parseConfiguration(final InputSupplier<? extends InputStream> configSupplier,
                                                  final Configuration config) {
        checkNotNull(configSupplier, "The configuration supplier is invalid");
        checkNotNull(config, "The configuration container is invalid");

        try {
            final DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            final Document doc = docBuilder.parse(configSupplier.getInput());
            final NodeList adapterAttrs = (NodeList) xpath.evaluate(XPATH_ADAPTER_NAME_ATTR, doc, XPathConstants.NODESET);

            parseAdapterDetails(doc, adapterAttrs, config);
        } catch (XPathExpressionException e) {
            logger.error("XPath expression could not be evaluated: " + XPATH_ADAPTER_NAME_ATTR, e);
        } catch (SAXException e) {
            logger.error("Parse error occurred while parsing the config file", e);
        } catch (IOException e) {
            logger.error("IO error occurred while parsing the config file", e);
        } catch (ParserConfigurationException e) {
            logger.error("The XML parser was not configured properly", e);
        }

        return new ConfigurationHolder(config);
    }

    /**
     * Parses the configuration information for the specified adapter from the provided document details
     *
     * @param configNode  The configuration node details
     * @param config      The container used to store the parsed data
     * @param adapterName The name of the adapter being parsed
     */
    private static void parseAdapterConfigProperties(final NodeList configNode, final Configuration config, final String adapterName) {
        for (int index = 0; index < configNode.getLength(); index++) {
            final Node propertyNode = configNode.item(index);

            if (propertyNode.getNodeType() == Node.ELEMENT_NODE) {
                // Prefix the property with the adapter name to avoid collisions
                final String propertyName = String.format("%s.%s", adapterName, propertyNode.getNodeName());
                final String propertyValue = propertyNode.getTextContent();

                // Check to see if the property has already been configured
                if (config.get(propertyName) == null) {
                    config.set(propertyName, propertyValue);
                    logger.debug(String.format("Set configuration property %s = %s", propertyName, propertyValue));
                } else {
                    logger.warn("Unable to set configuration property: " + propertyName);
                }
            }
        }
    }

    /**
     * Parses the pertinent adapter attribute information from the provided document details
     *
     * @param doc          The document that is being parsed
     * @param adapterAttrs The node details of the extracted adapter attributes
     * @param config       The container used to store the parsed data
     */
    private void parseAdapterDetails(final Document doc, final NodeList adapterAttrs, final Configuration config) {
        final List<String> adapters = Lists.newArrayList();
        final int adapterCount = adapterAttrs.getLength();

        if (adapterCount > 0) {
            logger.debug(String.format("Found %d configured adapters", adapterCount));

            for (int index = 0; index < adapterCount; index++) {
                final Node node = adapterAttrs.item(index);

                if (node.getNodeType() == Node.ATTRIBUTE_NODE) {
                    final String adapterName = node.getNodeValue();
                    adapters.add(adapterName);

                    // Attempt to parse the configuration options for this adapter
                    final String xpathExpr = String.format(XPATH_ADAPTER_CONFIG_NODES, adapterName);
                    try {
                        NodeList configOpts = (NodeList) xpath.evaluate(xpathExpr, doc, XPathConstants.NODESET);

                        logger.debug("Parsing configuration options for adapter: " + adapterName);
                        parseAdapterConfigProperties(configOpts, config, adapterName);
                    } catch (XPathExpressionException e) {
                        logger.error("XPath expression could not be evaluated: " + xpathExpr, e);
                    }
                }
            }

            // Store the adapter names in the configuration
            config.setStrings(ConfigConstants.PROP_CONFIGURED_ADAPTERS, adapters.toArray(new String[0]));
        }
    }
}
