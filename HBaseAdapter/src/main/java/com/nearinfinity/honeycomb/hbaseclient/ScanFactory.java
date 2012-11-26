package com.nearinfinity.honeycomb.hbaseclient;

import org.apache.hadoop.hbase.client.Scan;

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
