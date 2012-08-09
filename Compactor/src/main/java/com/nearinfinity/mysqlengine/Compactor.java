package com.nearinfinity.mysqlengine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.ipc.HBaseClient;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/8/12
 * Time: 2:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class Compactor {
    private HTable table;
    private int interval;

    private static final byte[] NIC = "nic".getBytes();

    private static final byte[] IS_DELETED = "isDeleted".getBytes();

    public Compactor(String[] args) {
        this.interval = Integer.parseInt(args[0]);
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "localhost");

        try {
            this.table = new HTable(configuration, args[1]);
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Usage: Compactor <interval_in_seconds> <hbase_table_name>");
            System.exit(1);
        }
        new Compactor(args).go();
    }

    public void go() throws IOException {
        while (true) {
            Scan scan = new Scan();

            long deleted = 1L;
            Filter filter = new SingleColumnValueFilter(NIC, IS_DELETED, CompareFilter.CompareOp.EQUAL, Bytes.toBytes(deleted));
            scan.setFilter(filter);

            ResultScanner scanner = table.getScanner(scan);
            List<Delete> deleteList = new LinkedList<Delete>();
            for (Result result : scanner) {
                //Delete the data row
                byte[] rowKey = result.getRow();
                Delete rowDelete = new Delete(rowKey);
                deleteList.add(rowDelete);

                //Build the index rows and append to deleteList
                Scan indexScan = new Scan();
                ResultScanner indexScanner = table.getScanner(indexScan);
            }

            table.delete(deleteList);

            //Now sleep
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
