package com.nearinfinity.hbaseclient;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.LinkedList;

public class TableMultipartKeys {
    private LinkedList<LinkedList<String>> multipartKeys = new LinkedList<LinkedList<String>>();

    public void addMultipartKey(String keys) {
        String[] keyPieces = keys.split(",");
        LinkedList<String> addition = new LinkedList<String>();
        for (String piece : keyPieces) {
            addition.add(piece);
        }

        multipartKeys.add(addition);
    }

    public String toJson() {
        Type type = new TypeToken<LinkedList<LinkedList<String>>>() {
        }.getType();
        return new Gson().toJson(multipartKeys, type);
    }
}
