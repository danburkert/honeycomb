package com.nearinfinity.bulkloader;


import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Random;

public class PutPartitioner extends Partitioner<ImmutableBytesWritable, Put> implements Configurable {

    public static final String COLUMN_COUNT = "mapreduce.samplepartitioner.columncount";

    private int columnCount;

    private Configuration conf;

    private Random random;

    @Override
    public int getPartition(ImmutableBytesWritable immutableBytesWritable, Put put, int numPartitions) {
        return this.random.nextInt(numPartitions);
    }

    public static void setColumnCount(Configuration conf, int columnCount) {
        conf.setInt(COLUMN_COUNT, columnCount);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        this.columnCount = conf.getInt(COLUMN_COUNT, 3);
        this.random = new Random();
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
}
