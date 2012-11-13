package com.nearinfinity.HBaseTableCreator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.MedianSplitPolicy;

import java.io.IOException;

public class TableCreator {
    public static void main(String[] args) throws IOException, InterruptedException {
        String table_name = "t1";
        String family_name = "f1";
        HTableDescriptor td = new HTableDescriptor(table_name);
        td.addFamily(new HColumnDescriptor(new HColumnDescriptor(family_name)));
        td.setValue("SPLIT_POLICY", "MedianRegionSplitPolicyTest");
        td.setValue(HTableDescriptor.SPLIT_POLICY, MedianSplitPolicy.class.getName());

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "localhost");
        HBaseAdmin admin = new HBaseAdmin(configuration);
        admin.createTable(td);
        admin.flush("t1");
    }
}
