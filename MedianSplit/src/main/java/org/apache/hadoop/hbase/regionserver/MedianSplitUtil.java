package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Utilities used by the MedianSplitPolicy and MedianSplitObserver classes
 */
public class MedianSplitUtil {
    static final Log LOG = LogFactory.getLog(MedianSplitUtil.class);

    /**
     * Sample region store files for median split key and size in bytes
     * @param region
     * @return
     */
    static List<Pair<byte[], Long>> sampleRegion(HRegion region) {
        LOG.info("Sampling region " + region.getRegionNameAsString());

        List<Pair<byte[], Long>> samples = new ArrayList<Pair<byte[], Long>>();
        for (Store store : region.getStores().values()) {
            for (StoreFile sf : store.getStorefiles()) {
                try {
                    byte[] midkey = sf.createReader().midkey();
                    KeyValue kv = KeyValue.createKeyValueFromKey(midkey);
                    byte[] rowkey = kv.getRow();
                    long size = sf.createReader().length();
                    samples.add(Pair.newPair(rowkey, size));

                } catch (IOException e) {
                    // Empty StoreFile, or problem reading it
                    LOG.error("Encountered problem reading store file: " +
                            sf.toString());
                    break;
                }
            }
        }
        return samples;
    }

    /**
     * Combine sample byte arrays by summing each sample by its associated
     * weight, and dividing the result by the sum of the total weight.
     * Because samples are byte arrays, they must be normalized to the
     * length of the longest sample array.
     * @param samples
     * @return
     */
    static byte[] combineSamples(List<Pair<byte[], Long>> samples) {

        LOG.info("Combining " + samples.size() + " samples");

        long total_weight = 0;
        int max_sample_len = 0;

        // Find the max sample array length, and sum the sample weights
        for (Pair<byte[], Long> s : samples) {
            byte[] sample = s.getFirst();
            long weight = s.getSecond();

            max_sample_len = Math.max(max_sample_len, sample.length);
            total_weight += weight;
        }

        BigInteger weighted_samples_sum = BigInteger.ZERO;

        for (Pair<byte[], Long> s : samples) {
            byte[] sample = s.getFirst();
            long size = s.getSecond();

            byte[] normalized_sample =
                    Bytes.padTail(sample, max_sample_len - sample.length);
            BigInteger sample_val = new BigInteger(1, normalized_sample);

            weighted_samples_sum = weighted_samples_sum
                    .add(sample_val.multiply(BigInteger.valueOf(size)));
        }

        BigInteger combined_val =
                weighted_samples_sum.divide(BigInteger.valueOf(total_weight));
        byte[] combined = combined_val.toByteArray();

        // If the leading byte is 0, it came from BigInteger adding a byte to
        // indicate a positive two's compliment value.  Strip it.
        if (combined[0] == 0x00) {
            combined = Bytes.tail(combined, 1);
        }

        LOG.info("Combined value: " + Bytes.toString(combined));
        return combined;
    }
}
