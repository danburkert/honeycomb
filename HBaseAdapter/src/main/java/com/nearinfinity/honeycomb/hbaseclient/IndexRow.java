package com.nearinfinity.honeycomb.hbaseclient;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

public class IndexRow {
    private byte[] uuid;
    private TreeMap<String, byte[]> rowMap;
    private static final Logger logger = Logger.getLogger(IndexRow.class);


    public IndexRow() {
        this.uuid = null;
    }

    public Map<java.lang.String, byte[]> getRowMap() {
        return this.rowMap;
    }

    public byte[] getUUID() {
        return uuid;
    }

    /**
     * Extract a SQL row out of an HBase index row result.
     *
     * @param result HBase index row result
     */
    @SuppressWarnings("unchecked")
    public void parseResult(Result result) {
        this.setUUID(ResultParser.parseUUID(result));
        byte[] mapBytes = ResultParser.parseValueMap(result);
        checkNotNull(mapBytes, "mapBytes");


        SpecificDatumReader<com.nearinfinity.honeycomb.mysql.Row> rowReader =
                new SpecificDatumReader<com.nearinfinity.honeycomb.mysql.Row>(com.nearinfinity.honeycomb.mysql.Row.class);


        Decoder decoder = DecoderFactory.get().binaryDecoder(mapBytes, null);
        com.nearinfinity.honeycomb.mysql.Row row = null;
        try {
            row = rowReader.read(null, decoder);
        } catch (IOException e) {
            e.printStackTrace();
        }

        rowMap = new TreeMap<String, byte[]>();

        for (Map.Entry<String, ByteBuffer> entry : row.getRecords().entrySet()) {
            String s = entry.getKey();
            byte[] b = entry.getValue().array();

            rowMap.put(s, b);
        }
    }

    private void setUUID(UUID rowUuid) {
        this.uuid = ByteBuffer.allocate(16)
                .putLong(rowUuid.getMostSignificantBits())
                .putLong(rowUuid.getLeastSignificantBits())
                .array();
    }
}
