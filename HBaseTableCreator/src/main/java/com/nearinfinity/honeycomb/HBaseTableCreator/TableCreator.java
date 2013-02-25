package com.nearinfinity.honeycomb.HBaseTableCreator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.MedianSplitPolicy;
import org.apache.hadoop.hbase.regionserver.StoreFile;

import java.io.IOException;

public class TableCreator {
    public static void main(String[] args) throws IOException, InterruptedException {
        String table_name = "sql";
        String family_name = "nic";
        HTableDescriptor td = new HTableDescriptor(table_name);
        HColumnDescriptor cd = new HColumnDescriptor(family_name);

        cd.setBloomFilterType(StoreFile.BloomType.ROW);
        cd.setDataBlockEncoding(DataBlockEncoding.PREFIX);
        cd.setCompressionType(Compression.Algorithm.SNAPPY);

        td.addFamily(new HColumnDescriptor(cd));
        td.setValue(HTableDescriptor.SPLIT_POLICY, MedianSplitPolicy.class.getName());

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "localhost");
        HBaseAdmin admin = new HBaseAdmin(configuration);
        admin.createTable(td);
        admin.flush("sql");
    }
}