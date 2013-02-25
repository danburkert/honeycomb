package com.nearinfinity.honeycomb.mysql;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Utility class containing helper functions.
 */
public class Util {
    private static final Logger logger = Logger.getLogger(Util.class);

    /**
     * Returns a 16 byte wide buffer from a {@link UUID}.
     * @param uuid
     */
    public static byte[] UUIDToBytes(UUID uuid) {
        checkNotNull(uuid, "uuid must not be null.");
        return ByteBuffer.allocate(16)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    /**
     * Create a {@link UUID} from a {@link byte[]} 16 bytes long.
     * @param bytes A byte buffer 16 bytes wide
     */
    public static UUID BytesToUUID(byte[] bytes) {
        checkNotNull(bytes, "bytes must not be null.");
        checkArgument(bytes.length == 16, "bytes must be of length 16.");
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return new UUID(buffer.getLong(), buffer.getLong());
    }

    public static Configuration readConfiguration(File source)
            throws IOException, ParserConfigurationException, SAXException {
        Configuration params = new Configuration(false);
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(source);
        NodeList options = doc.getElementsByTagName("adapteroption");
        logger.info(String.format("Number of options %d", options.getLength()));
        for (int i = 0; i < options.getLength(); i++) {
            Element node = (Element) options.item(i);
            String name = node.getAttribute("name");
            String nodeValue = node.getTextContent();
            logger.info(String.format("Node %s = %s", name, nodeValue));
            params.set(name, nodeValue);
        }

        return params;
    }
}
