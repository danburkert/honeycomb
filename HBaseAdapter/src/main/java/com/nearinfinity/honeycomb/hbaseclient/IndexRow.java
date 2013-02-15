package com.nearinfinity.honeycomb.hbaseclient;

import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

public class IndexRow {
    private static final Logger logger = Logger.getLogger(IndexRow.class);
    private byte[] uuid;
    private TreeMap<String, byte[]> rowMap;


    public IndexRow() {
        this.uuid = null;
    }

    public Map<java.lang.String, byte[]> getRowMap() {
        return this.rowMap;
    }

    public byte[] getUUID() {
        return uuid;
    }

    private void setUUID(UUID rowUuid) {
        this.uuid = ByteBuffer.allocate(16)
                .putLong(rowUuid.getMostSignificantBits())
                .putLong(rowUuid.getLeastSignificantBits())
                .array();
    }

    /**
     * Extract a SQL row out of an HBase index row result.
     *
     * @param result HBase index row result
     */
    @SuppressWarnings("unchecked")
    public void parseResult(Result result) {
        long start = System.currentTimeMillis();
        this.setUUID(ResultParser.parseUUID(result));
        byte[] mapBytes = ResultParser.parseValueMap(result);
        checkNotNull(mapBytes, "mapBytes");
        rowMap = Util.deserializeMap(mapBytes);
        long end = System.currentTimeMillis();
        Metrics.getInstance().addParseResultTime(end - start);
    }
}
