package com.nearinfinity.mysqlengine.jni;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: showell
 * Date: 8/13/12
 * Time: 9:08 AM
 * To change this template use File | Settings | File Templates.
 */
public class RowArray
{
    private final List<Map<String, byte[]>> rowsHeld = new LinkedList<Map<String, byte[]>>();

    public RowArray()
    {
    }

    public void addRow(Map<String, byte[]> row)
    {
        this.rowsHeld.add(row);
    }

    public List<Map<String, byte[]>> rowList()
    {
        return this.rowsHeld;
    }
}
