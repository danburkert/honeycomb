package com.nearinfinity.bulkloader;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.eclipse.jdt.internal.core.Assert;
import org.junit.Test;

import java.util.Arrays;

public class SamplingMapperTest {
    @Test
    public void extractIndexRowWorks() throws Exception {
        byte[] buffer = new byte[17];
        buffer[0] = 4;
        Bytes.putLong(buffer, 1, 8);
        Bytes.putLong(buffer, 9, 5);
        Put p = new Put(buffer);
        byte[] result = SamplingMapper.extractReduceKey(p);
        Assert.isTrue(Arrays.equals(result, buffer));
    }

    @Test
    public void extractDataRowWorks() throws Exception {
        byte[] buffer = new byte[9];
        buffer[0] = 3;
        Bytes.putLong(buffer, 1, 8);
        Put p = new Put(buffer);
        byte[] result = SamplingMapper.extractReduceKey(p);
        Assert.isTrue(Arrays.equals(result, buffer));
    }
}
