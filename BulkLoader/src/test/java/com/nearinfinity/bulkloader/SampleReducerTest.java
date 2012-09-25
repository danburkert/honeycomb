package com.nearinfinity.bulkloader;

import org.apache.hadoop.hbase.client.Put;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class SampleReducerTest {
    @Test
    public void testCreateSplits() throws Exception {
        List<Put> putList = new LinkedList<Put>();
        long heapSize = createPut(10, putList);
        heapSize += createPut(10, putList);
        heapSize += createPut(10, putList);
        heapSize += createPut(10, putList);
        heapSize += createPut(10, putList);
        heapSize += createPut(10, putList);
        Set<byte[]> splits = SampleReducer.createSplits(putList, heapSize / 2);
        Assert.assertEquals(2, splits.size());
        splits = SampleReducer.createSplits(putList, heapSize + 1);
        Assert.assertEquals(1, splits.size());
    }

    private long createPut(int size, List<Put> putList) {
        byte[] buffer = new byte[size];
        Put put = new Put(buffer);
        putList.add(put);
        return put.heapSize();
    }
}
