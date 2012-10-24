package com.nearinfinity.hbaseclient.strategy;

import com.nearinfinity.hbaseclient.Index;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class IndexTest {
    @Test
    public void testNextColumn() throws Exception {
        byte[] column = ByteBuffer.allocate(8*3)
                .putLong(1)
                .putLong(2)
                .putLong(3)
                .array();
        byte[] expected = ByteBuffer.allocate(8*3)
                .putLong(1)
                .putLong(2)
                .putLong(4)
                .array();
        byte[] result = Index.incrementColumn(column, 8 * 2);
        Assert.assertArrayEquals(expected, result);
    }
}
