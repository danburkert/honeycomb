package com.nearinfinity.HBaseTableCreator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.MedianRegionSplitPolicy;

import java.io.IOException;

public class TableCreator
{
    public static void main( String[] args ) throws IOException, InterruptedException {
        HTableDescriptor td = new HTableDescriptor("sql");
        td.addFamily(new HColumnDescriptor(new HColumnDescriptor("nic")));
        td.setValue("SPLIT_POLICY", "MedianRegionSplitPolicyTest");
        td.setValue(HTableDescriptor.SPLIT_POLICY, MedianRegionSplitPolicy.class.getName());

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "localhost");
        HBaseAdmin admin = new HBaseAdmin(configuration);
        admin.createTable(td);
        admin.flush("sql");
    }
}
