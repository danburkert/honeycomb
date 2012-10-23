package com.nearinfinity.hbaseclient;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.LinkedList;

public class TableMultipartKeys {
    private final LinkedList<LinkedList<String>> multipartKeys = new LinkedList<LinkedList<String>>();
    private static final Type type = new TypeToken<LinkedList<LinkedList<String>>>() {
    }.getType();

    public void addMultipartKey(String keys) {
        String[] keyPieces = keys.split(",");
        LinkedList<String> addition = new LinkedList<String>();
        for (String piece : keyPieces) {
            addition.add(piece);
        }

        multipartKeys.add(addition);
    }

    public byte[] toJson() {
        return new Gson().toJson(multipartKeys, type).getBytes();
    }

    public static byte[] indexJson(final LinkedList<LinkedList<String>> keys) {
        return new Gson().toJson(keys, type).getBytes();
    }
}
