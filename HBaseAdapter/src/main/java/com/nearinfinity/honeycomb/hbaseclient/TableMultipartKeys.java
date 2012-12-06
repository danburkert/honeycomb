package com.nearinfinity.honeycomb.hbaseclient;

import java.util.LinkedList;
import java.util.List;

public class TableMultipartKeys {
    private final List<List<String>> multipartKeys = new LinkedList<List<String>>();
    private final List<List<String>> uniqueKeys = new LinkedList<List<String>>();

    public void addMultipartKey(String keys, boolean unique) {
        String[] keyPieces = keys.split(",");
        LinkedList<String> addition = new LinkedList<String>();
        for (String piece : keyPieces) {
            addition.add(piece);
        }

        if (unique) {
            uniqueKeys.add(addition);
        }

        multipartKeys.add(addition);
    }

    public byte[] toJson() {
        return Util.serializeList(multipartKeys);
    }

    public List<List<String>> indexKeys() {
        return multipartKeys;
    }

    public List<List<String>> uniqueKeys() {
        return uniqueKeys;
    }

    public static byte[] indexJson(final List<List<String>> keys) {
        return Util.serializeList(keys);
    }

    public byte[] uniqueKeysToJson() {
        return Util.serializeList(uniqueKeys);
    }
}
