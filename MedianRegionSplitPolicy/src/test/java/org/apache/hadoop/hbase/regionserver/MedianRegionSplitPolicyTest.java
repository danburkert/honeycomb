package org.apache.hadoop.hbase.regionserver;

import junit.framework.TestCase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.regionserver.MedianRegionSplitPolicy;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: dburkert
 * Date: 11/7/12
 * Time: 2:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class MedianRegionSplitPolicyTest extends TestCase {
    public void setUp() throws Exception {

    }

    public void tearDown() throws Exception {

    }

    public void testCombineSamples() {
        List<Pair<byte[], Long>> samples = new ArrayList<Pair<byte[], Long>>();
        byte[] weighted_median;
        byte[] expected;

        // (0xff, 10) -> 0xff
        samples.clear();
        samples.add(Pair.newPair(new byte[]{(byte) 0xff}, 10l));
        weighted_median = MedianRegionSplitPolicy.combineSamples(samples);
        expected = new byte[]{(byte) 0xff};
        assertTrue(Arrays.equals(expected, weighted_median));

        // ("aa", 1), ("cc", 1) -> "bb"
        samples.clear();
        samples.add(Pair.newPair("aa".getBytes(), 1l));
        samples.add(Pair.newPair("cc".getBytes(), 1l));
        weighted_median = MedianRegionSplitPolicy.combineSamples(samples);
        expected = "bb".getBytes();
        assertTrue(Arrays.equals(expected, weighted_median));

        // ("aa", 1), ("dd", 2) -> "cc"
        samples.clear();
        samples.add(Pair.newPair("aa".getBytes(), 1l));
        samples.add(Pair.newPair("dd".getBytes(), 2l));
        weighted_median = MedianRegionSplitPolicy.combineSamples(samples);
        expected = "cc".getBytes();
        assertTrue(Arrays.equals(expected, weighted_median));

        // ("aaa", 1), ("aa", 1) -> "aa0"
        samples.clear();
        samples.add(Pair.newPair("aaa".getBytes(), 1l));
        samples.add(Pair.newPair("aa".getBytes(), 1l));
        weighted_median = MedianRegionSplitPolicy.combineSamples(samples);
        expected = "aa0".getBytes();
        assertTrue(Arrays.equals(expected, weighted_median));
    }

}
