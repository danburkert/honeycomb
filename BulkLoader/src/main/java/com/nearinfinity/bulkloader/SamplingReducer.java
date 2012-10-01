package com.nearinfinity.bulkloader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class SamplingReducer extends TableReducer<ImmutableBytesWritable, Put, Writable> {
    private long regionSize;
    private static final long _1GB = 1 << 30;
    public static final byte[] DUMMY_FAMILY = "dummy".getBytes();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        regionSize = Math.min(conf.getLong("region_size", _1GB), _1GB);
    }

    @Override
    public void reduce(ImmutableBytesWritable key, Iterable<Put> rowKeys, Context context) throws IOException, InterruptedException {
        // Row keys can only be iterated over *once*
        Set<byte[]> splits = createSplits(rowKeys, regionSize);

        for (byte[] split : splits) {
            Put put = new Put(split).add(DUMMY_FAMILY, "spacer".getBytes(), new byte[0]);
            context.write(null, put);
        }
    }

    public static Set<byte[]> createSplits(Iterable<Put> rowKeys, long regionSize) {
        long putSize = 0;
        boolean first = true;
        Set<byte[]> splits = new HashSet<byte[]>();
        Put firstPut = new Put();
        for (Put put : rowKeys) {
            if (first) {
                firstPut = new Put(put);
                first = false;
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
