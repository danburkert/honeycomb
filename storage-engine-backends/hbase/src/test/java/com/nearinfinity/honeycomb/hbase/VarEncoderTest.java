/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 * Copyright 2013 Altamira Corporation.
 */


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