package com.nearinfinity.bulkloader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.*;

public class SampleReducer extends TableReducer<ImmutableBytesWritable, Put, Writable> {
    private long regionSize;
    private static final long _1GB = 1 << 30;
    private static final Log LOG = LogFactory.getLog(SampleReducer.class);

    public static final byte[] DUMMY_FAMILY = "dummy".getBytes();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        regionSize = Math.min(conf.getLong("region_size", _1GB), _1GB);
        LOG.info("Region size " + regionSize);
    }

    @Override
    public void reduce(ImmutableBytesWritable key, Iterable<Put> rowKeys, Context context) throws IOException, InterruptedException {
        Set<byte[]> splits = createSplits(rowKeys, regionSize);

        for (byte[] split : splits) {
            LOG.info("Split key: " + Bytes.toStringBinary(split));
            Put put = new Put(split).add(DUMMY_FAMILY, "spacer".getBytes(), new byte[0]);
            context.write(null, put);
        }
    }

    public static Set<byte[]> createSplits(Iterable<Put> rowKeys, long regionSize) throws IOException, InterruptedException {
        long putSize = 0;
        boolean first = false;
        Set<byte[]> splits = new HashSet<byte[]>();
        Put firstPut = null;
        for (Put put : rowKeys) {
            if (!first) {
                firstPut = new Put(put);
            }

            long increment = putSize + put.heapSize();

            if (increment >= regionSize) {
                byte[] row = put.getRow();
                splits.add(row);
                putSize = 0;
            } else {
                putSize = increment;
            }
        }

        if (splits.isEmpty()) {
            splits.add(firstPut.getRow());
        }

        return splits;
    }
}
