package com.nearinfinity.mysqlengine.jni;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/1/12
 * Time: 10:32 AM
 * To change this template use File | Settings | File Templates.
 */
public class Row {
    private Map<String, byte[]> rowMap;

    public Map<String, byte[]> getRowMap() {
        return rowMap;
    }

    public void setRowMap(Map<String, byte[]> rowMap) {
        this.rowMap = new HashMap<String, byte[]>(rowMap);
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
