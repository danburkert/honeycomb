package com.nearinfinity.bulkloader;

import com.google.gson.Gson;
import com.nearinfinity.hbaseclient.TableInfo;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class SamplingReducerTest {
    @Test
    public void testCreateSplits() throws Exception {
        List<Put> putList = new LinkedList<Put>();
        long heapSize = createPut(10, putList);
        heapSize += createPut(10, putList);
        heapSize += createPut(10, putList);
        heapSize += createPut(10, putList);
        heapSize += createPut(10, putList);
        heapSize += createPut(10, putList);
        List<byte[]> splits = SamplingReducer.createSplits(putList, heapSize / 2);
        Assert.assertEquals(2, splits.size());
        splits = SamplingReducer.createSplits(putList, heapSize + 1);
        Assert.assertEquals(1, splits.size());
    }

    @Test
    public void testAddSplits() throws Exception {
        List<byte[]> splits = new LinkedList<byte[]>();
        List<byte[]> additional = new LinkedList<byte[]>();
        byte[] first = new byte[5];
        byte[] second = new byte[5];
        Arrays.fill(first, (byte) 0);
        Arrays.fill(second, (byte) 127);
        splits.add(first);
        splits.add(second);
        splits.add(second);
        SamplingReducer.addSplits(splits.iterator(), additional, 1);
        Assert.assertEquals(1, additional.size());
    }

    private long createPut(int size, List<Put> putList) {
        byte[] buffer = new byte[size];
        Put put = new Put(buffer);
        putList.add(put);
        return put.heapSize();
    }
}
