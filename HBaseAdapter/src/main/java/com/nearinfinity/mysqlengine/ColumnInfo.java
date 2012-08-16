package com.nearinfinity.mysqlengine;

import java.util.ArrayList;
import java.util.List;

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
    private ArrayList<ColumnMetadata> metadata;
    public ColumnInfo(long id, String name, List<ColumnMetadata> metadata) {
        this.id = id;
        this.name = name;
        this.metadata = new ArrayList<ColumnMetadata>(metadata);
    }

    public long getId() {
        return this.id;
    }

    public String getName() {
        return this.name;
    }

    public ArrayList<ColumnMetadata> getMetadata() {
        return this.metadata;
    }
}
