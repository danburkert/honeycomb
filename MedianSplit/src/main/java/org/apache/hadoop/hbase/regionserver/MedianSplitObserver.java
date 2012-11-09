package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Pair;

import java.util.List;

/**
 * Observe region server and intercept split requests and split on median rowkey.
 */
public class MedianSplitObserver extends BaseRegionObserver {
    static final Log LOG = LogFactory.getLog(MedianSplitObserver.class);

    @Override
    public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) {
        HRegion region = e.getEnvironment().getRegion();
        LOG.info("Intercepting split request for region "
                + region.getRegionNameAsString());

        // Sample the regions for median and weight
        List<Pair<byte[], Long>> samples = MedianSplitUtil.sampleRegion(region);

        // If there are no store files, revert to default split
        if (samples.isEmpty()) {
            LOG.info("No store files found. Reverting to default split.");
            return;
        }

        // Otherwise combine samples
        byte[] split_key =  MedianSplitUtil.combineSamples(samples);

        // Split the region on the median row key
        region.forceSplit(split_key);

        // And bypass the triggering split
        e.bypass();
    }
}