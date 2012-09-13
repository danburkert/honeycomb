package com.nearinfinity.bulkloader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: dburkert
 * Date: 9/13/12
 * Time: 10:07 AM
 * To change this template use File | Settings | File Templates.
 */
public class BulkLoaderTest {
    private BulkLoader.BulkLoaderMapper mapper;
    private MapDriver driver;

    @Before
    public void setUp() throws Exception {
        mapper = new BulkLoader.BulkLoaderMapper();
        driver = MapDriver.newMapDriver(mapper);
    }

    @After
    public void tearDown() throws Exception {
    }


    @Test public void testMapper() throws Exception {
    }
}