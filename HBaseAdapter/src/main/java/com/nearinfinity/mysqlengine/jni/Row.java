package com.nearinfinity.mysqlengine.jni;

import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/1/12
 * Time: 10:32 AM
 * To change this template use File | Settings | File Templates.
 */
public class Row {
    private static final Logger logger = Logger.getLogger(Row.class);

    private Map<String, byte[]> rowMap;

    private byte[] uuid;

    public Row() {
        this.rowMap = new HashMap<String, byte[]>();
        this.uuid = new byte[0];
    }

    public Row(Map<String, byte[]> rowMap, UUID uuidObj) {
        this.rowMap = rowMap;
        this.setUUID(uuidObj);
    }

    public Map<String, byte[]> getRowMap() {
        return rowMap;
    }

    public void setRowMap(Map<String, byte[]> rowMap) {
        this.rowMap = new HashMap<String, byte[]>(rowMap);
    }

    public void setUUID(UUID rowUuid) {
        this.uuid = ByteBuffer.allocate(16)
                .putLong(rowUuid.getMostSignificantBits())
                .putLong(rowUuid.getLeastSignificantBits())
                .array();
    }

    public byte[] getUUID() {
        return this.uuid;
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
}
