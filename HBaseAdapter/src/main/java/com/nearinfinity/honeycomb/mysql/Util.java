package com.nearinfinity.honeycomb.mysql;

import com.nearinfinity.honeycomb.mysql.gen.TableSchema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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

    public static final int UUID_WIDTH = 16;

    /**
     * Returns a UUID_WIDTH byte wide buffer from a {@link UUID}.
     *
     * @param uuid
     */
    public static byte[] UUIDToBytes(UUID uuid) {
        checkNotNull(uuid, "uuid must not be null.");
        return ByteBuffer.allocate(UUID_WIDTH)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }

    /**
     * Create a {@link UUID} from a {@link byte[]} UUID_WIDTH bytes long.
     *
     * @param bytes A byte buffer UUID_WIDTH bytes wide
     */
    public static UUID BytesToUUID(byte[] bytes) {
        checkNotNull(bytes, "bytes must not be null.");
        checkArgument(bytes.length == UUID_WIDTH, "bytes must be of length " + UUID_WIDTH + ".");
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return new UUID(buffer.getLong(), buffer.getLong());
    }

    public static byte[] serializeTableSchema(TableSchema schema) throws IOException {
        return serializeAvroObject(schema, TableSchema.class);
    }

    public static TableSchema deserializeTableSchema(byte[] schema) throws IOException {
        return (TableSchema) deserializeAvroObject(schema, TableSchema.class);
    }

    /**
     * Serialize obj into byte[]
     *
     * @return Serialized row
     * @throws IOException when serialization fails
     */
    public static byte[] serializeAvroObject(Object obj, Class clazz) throws IOException {
        DatumWriter<Object> writer = new SpecificDatumWriter<Object>(clazz);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(obj, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    /**
     * Deserialize obj into new clazz instance
     *
     * @param obj byte buffer containing serialized Object
     * @return new Row instance from serializedRow
     * @throws IOException
     */
    public static Object deserializeAvroObject(byte[] obj, Class clazz)
            throws IOException {
        DatumReader<Object> reader = new SpecificDatumReader<Object>(clazz);
        ByteArrayInputStream in = new ByteArrayInputStream(obj);
        Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        return reader.read(null, decoder);
    }

    public static String generateHexString(final byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }

        return sb.toString();
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
