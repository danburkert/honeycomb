package com.nearinfinity.bulkloader;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class SamplingPartitionerTest {
    @Test
    public void getPartitionCantBeNegative() throws Exception {
        SamplingPartitioner partitioner = new SamplingPartitioner();
        byte[] arr = ByteBuffer.allocate(1 + 8 + 8).put((byte)4).putLong(0).putLong(Integer.MAX_VALUE).array();
        int part = partitioner.getPartition(new ImmutableBytesWritable(arr), null, 33);
        Assert.assertTrue(part >= 0);
    }
}
