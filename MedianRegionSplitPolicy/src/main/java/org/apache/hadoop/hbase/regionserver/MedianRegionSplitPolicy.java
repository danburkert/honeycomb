package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.KeyValue;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class MedianRegionSplitPolicy
extends IncreasingToUpperBoundRegionSplitPolicy {
    static final Log LOG = LogFactory.getLog(MedianRegionSplitPolicy.class);

    @Override
    protected byte[] getSplitPoint() {
        LOG.info("getSplitPoint");
        Collection<Store> stores = region.getStores().values();

        // Holds the midpoint and size in bytes of each StoreFile
        List<Pair<byte[], Long>> samples =
                new ArrayList<Pair<byte[], Long>>();

        for (Store s : stores) {
            List<StoreFile> sfiles = s.getStorefiles();
            for (StoreFile sf : sfiles) {
                try {
                    byte[] midkey = sf.createReader().midkey();
                    KeyValue kv = KeyValue.createKeyValueFromKey(midkey);
                    byte[] rowkey = kv.getRow();
                    long size = sf.createReader().length();
                    samples.add(Pair.newPair(rowkey, size));

                } catch (IOException e) {
                    // Empty StoreFile, or problem reading it
                    e.printStackTrace();
                    break;
                }
            }
        }

        // If there are no store files, revert to default split point
        if(samples.isEmpty()) {
            return super.getSplitPoint();
        }
        return combineSamples(samples);
    }

    static byte[] combineSamples(List<Pair<byte[], Long>> samples) {
        LOG.info("combineSamples");

        BigInteger sigma_weighted_median = BigInteger.ZERO;

        long total_weight = 0;
        int max_sample_len = 0;  // All sample medians must be padded to the same length

        for(Pair<byte[], Long> sample : samples) {
            byte[] sample_median = sample.getFirst();
            long weight = sample.getSecond();
            LOG.info("combineSamples -> sample_median: " + Arrays.toString(sample_median));
            LOG.info("combineSamples -> weight: " + weight);

            max_sample_len = Math.max(max_sample_len, sample_median.length);
            total_weight += weight;
        }

        for(Pair<byte[], Long> sample : samples) {
            byte[] rowkey = sample.getFirst();
            long size = sample.getSecond();

            // Pad the rowkey out to account for longer row keys
            byte[] rowkey_pad =
                    Bytes.padTail(rowkey, max_sample_len - rowkey.length);
            BigInteger rowkey_val = new BigInteger(1, rowkey_pad);
            BigInteger weighted_rowkey_sample =
                    rowkey_val.multiply((BigInteger.valueOf(size)));
            sigma_weighted_median =
                    sigma_weighted_median.add(weighted_rowkey_sample);
        }

        BigInteger median_value =
                sigma_weighted_median.divide(BigInteger.valueOf(total_weight));
        byte[] median = median_value.toByteArray();

        // If the leading byte is 0, it came from BigInteger adding a byte to indicate a
        // positive two's compliment value.  Strip it.
        if(median[0] == 0x00) {
          median = Bytes.tail(median, 1);
        }

        LOG.info("combineSamples-> split_point: " +
        Bytes.toString(median));

        return median;

    }
}
