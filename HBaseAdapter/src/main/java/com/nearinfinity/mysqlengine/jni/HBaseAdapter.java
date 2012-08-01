package com.nearinfinity.mysqlengine.jni;

import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/1/12
 * Time: 9:46 AM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseAdapter<E> {
    public static boolean createTable(String tableName, List<String> columnNames) throws HBaseAdapterException {
        return false;
    }

    public static long startScan(String tableName) throws HBaseAdapterException {
        return 0L;
    }

    public static Map<String, byte[]> nextRow(long scanId) throws HBaseAdapterException {
        return null;
    }

    public static void endScan(long scanId) throws HBaseAdapterException {

    }

    public static boolean writeRow(Map<String, byte[]> values) throws HBaseAdapterException {
        return false;
    }

    public void bob(E e)
      {
      }
}
