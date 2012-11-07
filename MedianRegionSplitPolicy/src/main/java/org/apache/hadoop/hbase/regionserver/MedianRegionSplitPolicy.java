package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MedianRegionSplitPolicy
extends IncreasingToUpperBoundRegionSplitPolicy {

    @Override
    protected byte[] getSplitPoint() {
        Collection<Store> stores = region.getStores().values();

        // Holds the midpoint and size in bytes of each StoreFile
        List<Pair<byte[], Long>> sfile_samples =
                new ArrayList<Pair<byte[], Long>>();



        for (Store s : stores) {
            List<StoreFile> sfiles = s.getStorefiles();
            for (StoreFile sf : sfiles) {
                try {
                    byte[] midkey = sf.createReader().midkey();
                    long size = sf.createReader().length();

                    sfile_samples.add(Pair.newPair(midkey, size));


                } catch (IOException e) {
                    // Empty StoreFile, or problem reading it
                    e.printStackTrace();
                    break;
                }
            }
        }
        return combineSamples(sfile_samples);
    }

    static byte[] combineSamples(List<Pair<byte[], Long>> sfile_samples) {
        BigInteger weighted_rowkey_sum = BigInteger.ZERO;

        long region_size = 0;  // Size in bytes of region
        int max_rowkey_len = 0;  // Length of longest midpoint row key

        // Calculate total region size, and the max rowkey sample length
        for(Pair<byte[], Long> sample : sfile_samples) {
            byte[] rowkey = sample.getFirst();
            long size = sample.getSecond();
            max_rowkey_len = Math.max(max_rowkey_len, rowkey.length);
            region_size += size;
        }

        for(Pair<byte[], Long> sample : sfile_samples) {
            byte[] rowkey = sample.getFirst();
            long size = sample.getSecond();

            // Pad the rowkey out to account for longer row keys
            byte[] rowkey_pad = Bytes.padTail(rowkey, max_rowkey_len - rowkey.length);
            BigInteger rowkey_val = new BigInteger(1, rowkey_pad);
            BigInteger weighted_rowkey_sample =
                    rowkey_val.multiply((BigInteger.valueOf(size)));
            weighted_rowkey_sum =
                    weighted_rowkey_sum.add(weighted_rowkey_sample);
        }

        BigInteger weighted_rowkey =
                weighted_rowkey_sum.divide(BigInteger.valueOf(region_size));
        byte[] split_point = weighted_rowkey.toByteArray();

        // If the leading byte is 0, it came from BigInteger adding a byte to indicate a
        // positive two's compliment value.  Strip it.
        if(split_point[0] == 0x00) {
            split_point = Bytes.tail(split_point, 1);
        }
        return split_point;

    }
}
