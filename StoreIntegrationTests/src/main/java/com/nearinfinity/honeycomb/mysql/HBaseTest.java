package com.nearinfinity.honeycomb.mysql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.fest.assertions.Assertions.assertThat;

public class HBaseTest {
    public static void main(String[] args) throws Throwable {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, "localhost");
        byte[] columnFamily = "cf".getBytes();
        byte[] qualifier = "q1".getBytes();

        byte[] origVal = "orig".getBytes();
        byte[] newVal = "new".getBytes();

        int iterations = 100;

        HTable hTable = new HTable(conf, "test");
        hTable.setAutoFlush(true);

        List<UUID> uuids = new ArrayList<UUID>();

        for (int i = 0; i < iterations; i++) {
            UUID uuid = UUID.randomUUID();
            uuids.add(uuid);

            Put put = new Put(Util.UUIDToBytes(uuid));
            put.add(columnFamily, qualifier, origVal);
            hTable.put(put);
        }

        for (int i = 0; i < iterations; i++) {
            byte[] rowkey = Util.UUIDToBytes(uuids.get(i));
            Delete delete = new Delete(rowkey);
            Put put = new Put(rowkey);
            put.add(columnFamily, qualifier, newVal);

            Get get = new Get(rowkey);
            long ts = hTable.get(get).getMap().get(columnFamily).get(qualifier).lastEntry().getKey();

            delete.setTimestamp(ts);

            hTable.delete(delete);
            hTable.put(put);
        }

        for (int i = 0; i < iterations; i++) {

            Get get = new Get(Util.UUIDToBytes(uuids.get(i)));
            Result r = hTable.get(get);
            try {
                assertThat(r.getValue(columnFamily, qualifier)).isEqualTo(newVal);
            } catch (Throwable e) {
                System.out.println("iteration " + i);
                throw e;
            }
        }

        for (int i = 0; i < iterations; i++) {
            hTable.delete(new Delete(Util.UUIDToBytes(uuids.get(i))));
        }
    }
}
