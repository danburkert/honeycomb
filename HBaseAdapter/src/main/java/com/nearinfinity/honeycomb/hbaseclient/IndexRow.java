package com.nearinfinity.honeycomb.hbaseclient;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.hbase.client.Result;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class IndexRow {
    private byte[] uuid;
    private TreeMap<String, byte[]> rowMap;

    public IndexRow() {
        this.uuid = null;
    }

    public Map<String, byte[]> getRowMap() {
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
        byte[] mapBytes = ResultParser.parseValueMap(result);
        Gson gson = new Gson();
        Type type = new TypeToken<TreeMap<String, byte[]>>() {
        }.getType();

        this.rowMap = gson.fromJson(new String(mapBytes), type);
        this.setUUID(ResultParser.parseUUID(result));
    }

    private void setUUID(UUID rowUuid) {
        this.uuid = ByteBuffer.allocate(16)
                .putLong(rowUuid.getMostSignificantBits())
                .putLong(rowUuid.getLeastSignificantBits())
                .array();
    }
}
