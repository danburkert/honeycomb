package com.nearinfinity.hbaseclient;

import java.util.LinkedList;
import java.util.List;

public class TableMultipartKeys {
    private final List<List<String>> multipartKeys = new LinkedList<List<String>>();

    public void addMultipartKey(String keys) {
        String[] keyPieces = keys.split(",");
        LinkedList<String> addition = new LinkedList<String>();
        for (String piece : keyPieces) {
            addition.add(piece);
        }

        multipartKeys.add(addition);
    }

    public byte[] toJson() {
        return Util.serializeList(multipartKeys);
    }

    public static byte[] indexJson(final List<List<String>> keys) {
        return Util.serializeList(keys);
    }
}
