package com.nearinfinity.honeycomb.hbase;

import com.google.common.primitives.UnsignedBytes;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.PrimitiveGenerators;
import net.java.quickcheck.generator.iterable.Iterables;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class VarEncoderTest {
    // POSITIVE_NORMAL distribution gives more probability to numbers near 0.
    // This is an attempt to get a uniform distribution over the number of
    // significant bytes in the long, instead of a uniform distribution over the
    // range of the long.  It still doesn't do enough; we need an exponential distribution.
    private static final Generator<Long> ULONG_GEN = PrimitiveGenerators.longs(0, Long.MAX_VALUE);

    @Test
    public void testULongEncDec() {
        for (long n : Iterables.toIterable(ULONG_GEN)) {
            Assert.assertEquals(n, VarEncoder.decodeULong(VarEncoder.encodeULong(n)));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testULongEncFailsWhenNeg() {
        VarEncoder.encodeULong(-1);
    }

    @Test
    public void testULongEncSort() {
        List<Long> longs = new ArrayList<Long>();
        List<byte[]> bytes = new ArrayList<byte[]>();

        for (long n : Iterables.toIterable(ULONG_GEN)) {
            longs.add(new Long(n));
            bytes.add(VarEncoder.encodeULong(n));
        }

        Collections.sort(longs);
        Collections.sort(bytes, UnsignedBytes.lexicographicalComparator());

        for (int i = 0; i < longs.size(); i++) {
            long n = longs.get(i);
            byte[] nBytes = bytes.get(i);
            Assert.assertEquals(n, VarEncoder.decodeULong(nBytes));
            Assert.assertArrayEquals(nBytes, VarEncoder.encodeULong(n));
        }
    }
}