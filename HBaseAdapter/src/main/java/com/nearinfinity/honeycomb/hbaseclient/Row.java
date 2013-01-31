package com.nearinfinity.honeycomb.hbaseclient;

import org.apache.hadoop.hbase.client.Result;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class Row {
    private Map<String, byte[]> rowMap;
    private byte[] uuid;
    private Result result;

    public Row() {
        this.uuid = new byte[16];
    }

    public Row(Map<String, byte[]> rowMap, UUID uuidObj) {
        this.rowMap = rowMap;
        this.uuid = new byte[16];
        this.setUUID(uuidObj);
    }

    public Result getResult() {
        return this.result;
    }

    public Map<String, byte[]> getRowMap() {
        return rowMap;
    }

    public byte[] getUUID() {
        return this.uuid;
    }

    public String[] getKeys() {
        Set<String> strings = rowMap.keySet();
        return (String[]) strings.toArray(new String[strings.size()]);
    }

    public byte[][] getValues() {
        return (byte[][]) rowMap.values().toArray();
    }

    /**
     * Extracts the SQL row from an HBase row
     *
     * @param result SQL row
     * @param info   Table metadata
     */
    public void parse(Result result, TableInfo info) {
        Map<String, byte[]> values = ResultParser.parseDataRow(result, info);
        UUID uuid = ResultParser.parseUUID(result);
        this.setRowMap(values);
        this.setUUID(uuid);
        this.setResult(result);
    }

    private void setResult(Result result) {
        this.result = result;
    }

    private void setRowMap(Map<String, byte[]> rowMap) {
        this.rowMap = rowMap;
    }

    private void setUUID(UUID rowUuid) {
        ByteBuffer.wrap(this.uuid)
                .putLong(rowUuid.getMostSignificantBits())
                .putLong(rowUuid.getLeastSignificantBits());
    }
}
