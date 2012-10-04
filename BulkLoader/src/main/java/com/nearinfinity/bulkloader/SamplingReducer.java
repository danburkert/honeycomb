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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static java.lang.String.format;

public class SamplingReducer extends TableReducer<ImmutableBytesWritable, Put, Writable> {
    private long regionSize, hfilesExpected;
    public static final long _1GB = 1 << 30;
    public static final byte[] DUMMY_FAMILY = "dummy".getBytes();
    private static final Log LOG = LogFactory.getLog(SamplingReducer.class);


    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        double samplePercent = Double.parseDouble(conf.get("sample_percent"));
        regionSize = Math.round(samplePercent * conf.getLong("hbase.hregion.max.filesize", _1GB));
        hfilesExpected = conf.getLong("hfiles_expected", 10);
    }

    @Override
    public void reduce(ImmutableBytesWritable key, Iterable<Put> rowKeys, Context context) throws IOException, InterruptedException {
        // Row keys can only be iterated over *once*
        List<byte[]> splits = createSplits(rowKeys, regionSize);

        int size = splits.size();
        LOG.info(format("Splits created: %d/Expected splits %d", size, hfilesExpected));
        if (size < hfilesExpected) {
            addExtraSplits(splits);
        }

        for (byte[] split : splits) {
            Put put = new Put(split).add(DUMMY_FAMILY, "spacer".getBytes(), new byte[0]);
            context.write(null, put);
        }
    }

    private void addExtraSplits(List<byte[]> splits) {
        long neededSplits = hfilesExpected - splits.size();
        LOG.info(format("Adding %d extra splits to the split list.", neededSplits));
        List<byte[]> additionalSplits;
        additionalSplits = new LinkedList<byte[]>();
        while (splits.size() < hfilesExpected) {
            addSplits(splits.iterator(), additionalSplits, neededSplits);
            splits.addAll(additionalSplits);
            additionalSplits.clear();
        }

        LOG.info(format("Total splits %d", splits.size()));
    }

    public static void addSplits(Iterator<byte[]> splitList, List<byte[]> additionalSplits, long neededSplits) {
        if (neededSplits == 0) {
            return;
        }

        if (!splitList.hasNext()) {
            return;
        }

        byte[] first = splitList.next();
        if (!splitList.hasNext()) {
            return;
        }

        byte[] second = splitList.next();
        byte[][] split;
        try {
            if (Bytes.compareTo(first, second) < 0) {
                split = Bytes.split(first, second, true, 1);
            } else {
                split = Bytes.split(second, first, true, 1);
            }
        } catch (IllegalArgumentException e) {
            LOG.warn(String.format("Could not find split between %s and %s", Bytes.toStringBinary(first), Bytes.toStringBinary(second)));
            addSplits(splitList, additionalSplits, neededSplits - 1);
            return;
        }

        byte[] differenceKey = split[1];
        additionalSplits.add(differenceKey);
        addSplits(splitList, additionalSplits, neededSplits - 1);
    }

    public static List<byte[]> createSplits(Iterable<Put> rowKeys, long regionSize) {
        long putSize = 0;
        boolean first = true;
        List<byte[]> splits = new LinkedList<byte[]>();
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
