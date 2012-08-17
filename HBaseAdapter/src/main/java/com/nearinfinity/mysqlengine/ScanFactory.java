package com.nearinfinity.mysqlengine;

import org.apache.hadoop.hbase.client.Scan;

/**
 * Created by IntelliJ IDEA.
 * User: showell
 * Date: 8/16/12
 * Time: 3:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class ScanFactory {
    private static int cacheAmount = 5000;

    public static Scan buildScan(byte[] start, byte[] end) {
        Scan scan = new Scan(start, end);
        scan.setCaching(cacheAmount);
        return scan;
    }

    public static Scan buildScan() {
        Scan scan = new Scan();
        scan.setCaching(cacheAmount);
        return scan;
    }

    public static void setCacheAmount(int cache) {
        cacheAmount = cache;
    }
}
