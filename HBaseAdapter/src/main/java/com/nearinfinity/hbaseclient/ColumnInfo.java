package com.nearinfinity.hbaseclient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 7/25/12
 * Time: 2:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class ColumnInfo {
    private long id;
    private String name;
    private Map<ColumnMetadata, byte[]> metadata;
    public ColumnInfo(long id, String name, Map<ColumnMetadata, byte[]> metadata) {
        this.id = id;
        this.name = name;
        this.metadata = new HashMap<ColumnMetadata, byte[]>(metadata);
    }

    public long getId() {
        return this.id;
    }

    public String getName() {
        return this.name;
    }

    public Map<ColumnMetadata, byte[]> getMetadata() {
        return this.metadata;
    }
}
