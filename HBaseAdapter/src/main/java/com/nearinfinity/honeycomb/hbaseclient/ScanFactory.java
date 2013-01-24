package com.nearinfinity.honeycomb.hbaseclient;

import org.apache.hadoop.hbase.client.Scan;

public class ScanFactory {
    private static int cacheAmount = 5000;

    /**
     * Create a scan with start and end keys and a pre-specified caching level.
     *
     * @param start Start key
     * @param end   End key
     * @return Scan
     */
    public static Scan buildScan(byte[] start, byte[] end) {
        Scan scan = new Scan(start, end);
        scan.setCaching(cacheAmount);
        return scan;
    }

    /**
     * Create a scan with a pre-specified caching level.
     *
     * @return Scan
     */
    public static Scan buildScan() {
        Scan scan = new Scan();
        scan.setCaching(cacheAmount);
        return scan;
    }

    public static void setCacheAmount(int cache) {
        cacheAmount = cache;
    }
}
