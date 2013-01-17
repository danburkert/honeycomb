package com.nearinfinity.honeycomb.mysqlengine;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Row {
    private Map<String, byte[]> rowMap;
    private byte[] uuid;

    public Row() {
        this.rowMap = new HashMap<String, byte[]>();
        this.uuid = new byte[16];
    }

    public Row(Map<String, byte[]> rowMap, UUID uuidObj) {
        this.rowMap = rowMap;
        this.uuid = new byte[16];
        this.setUUID(uuidObj);
    }

    public Map<String, byte[]> getRowMap() {
        return rowMap;
    }

    public void setRowMap(Map<String, byte[]> rowMap) {
        this.rowMap = rowMap;
    }

    public byte[] getUUID() {
        return this.uuid;
    }

    public void setUUID(UUID rowUuid) {
        ByteBuffer.wrap(this.uuid)
                .putLong(rowUuid.getMostSignificantBits())
                .putLong(rowUuid.getLeastSignificantBits());
    }

    public String[] getKeys() {
        String[] keyArray = new String[rowMap.size()];
        int i = 0;
        for (String key : rowMap.keySet()) {
            keyArray[i++] = key;
        }
        return keyArray;
    }

    public byte[][] getValues() {
        byte[][] valueArray = new byte[rowMap.size()][];
        int i = 0;
        for (Map.Entry<String, byte[]> entry : rowMap.entrySet()) {
            valueArray[i++] = entry.getValue();
        }
        return valueArray;
    }

    public void parse(Map<String, byte[]> values, UUID uuid) {
        this.setRowMap(values);
        this.setUUID(uuid);
    }
}
