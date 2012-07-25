package com.nearinfinity.mysqlengine;

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
    public ColumnInfo(long id, String name) {
        this.id = id;
        this.name = name;
    }

    public long getId() {
        return this.id;
    }

    public String getName() {
        return this.name;
    }
}
