package com.nearinfinity.bulkloader;

import junit.framework.Assert;
import org.junit.Test;


public class CreateTableTest {
    @Test
    public void testCreateSplits() throws Exception {
        byte[][] result = CreateTable.createSplits();
        for (int x = 0; x < result.length; x++) {
            Assert.assertNotNull(result[x]);
        }
    }
}
