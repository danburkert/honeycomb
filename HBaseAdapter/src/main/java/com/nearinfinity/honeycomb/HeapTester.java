package com.nearinfinity.honeycomb;

import com.nearinfinity.honeycomb.mysqlengine.HBaseAdapter;
import com.nearinfinity.honeycomb.hbaseclient.Row;

public class HeapTester {

    public static void main(String[] args) {
        int count = 0;
        try {
            HBaseAdapter.initialize();
            System.out.println("HBase Adapter Initialized");
            stressTest();
        } catch (Exception e) {
            System.out.println("Exception during initialization: " + count);
            e.printStackTrace();
        }
    }

    public static void stressTest () {
        int count = 0;
        try {
            for (int i = 0; i < 50; i++) {
                long scanId = HBaseAdapter.startScan("hbase.hc_05", true);
                System.out.println("Scan Started");
                boolean cont = true;
                while (cont) {
                    Row result = HBaseAdapter.nextRow(scanId);
//                for (Map.Entry<String, byte[]> entry : result.getRowMap().entrySet()) {
//                    System.out.print(entry.getKey() + ": " +
//                            Bytes.toString(entry.getValue()) + " ");
//                }
//                System.out.println();
                    if (result != null) {
                        count++;
                        if (result.getRowMap().size() != 9)
                            throw new Exception("Result set is wrong size!");
                    } else {
                        cont = false;
                    }

                };

                System.out.println("Count: " + count);
                System.out.println("Scan Finished");
                count = 0;
            }

        } catch (Exception e) {
            System.out.println("Count at time of exception: " + count);
            e.printStackTrace();
        }
    }
}