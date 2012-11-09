package org.apache.hadoop.hbase.regionserver;

import junit.framework.TestCase;
import org.apache.hadoop.hbase.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MedianSplitPolicyTest extends TestCase {

    public void testCombineSamples() {
        List<Pair<byte[], Long>> samples = new ArrayList<Pair<byte[], Long>>();
        byte[] combined;
        byte[] expected;

        // (0xff, 10) -> 0xff
        samples.clear();
        samples.add(Pair.newPair(new byte[]{(byte) 0xff}, 10l));
        combined = MedianSplitPolicy.combineSamples(samples);
        expected = new byte[]{(byte) 0xff};
        assertTrue(Arrays.equals(expected, combined));

        // ("aa", 1), ("cc", 1) -> "bb"
        samples.clear();
        samples.add(Pair.newPair("aa".getBytes(), 1l));
        samples.add(Pair.newPair("cc".getBytes(), 1l));
        combined = MedianSplitPolicy.combineSamples(samples);
        expected = "bb".getBytes();
        assertTrue(Arrays.equals(expected, combined));

        // ("aa", 1), ("dd", 2) -> "cc"
        samples.clear();
        samples.add(Pair.newPair("aa".getBytes(), 1l));
        samples.add(Pair.newPair("dd".getBytes(), 2l));
        combined = MedianSplitPolicy.combineSamples(samples);
        expected = "cc".getBytes();
        assertTrue(Arrays.equals(expected, combined));

        // ("aaa", 1), ("aa", 1) -> "aa0"
        samples.clear();
        samples.add(Pair.newPair("aaa".getBytes(), 1l));
        samples.add(Pair.newPair("aa".getBytes(), 1l));
        combined = MedianSplitPolicy.combineSamples(samples);
        expected = "aa0".getBytes();
        assertTrue(Arrays.equals(expected, combined));
    }
}