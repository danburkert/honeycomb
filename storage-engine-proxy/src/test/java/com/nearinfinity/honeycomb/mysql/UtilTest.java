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
 * Copyright 2013 Near Infinity Corporation.
 */


package com.nearinfinity.honeycomb.mysql;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import net.java.quickcheck.generator.iterable.Iterables;

import org.junit.Test;

import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.mysql.generators.UUIDGenerator;

public class UtilTest {
    @Test
    public void testUUIDBytes() {
        for (UUID uuid : Iterables.toIterable(new UUIDGenerator())) {
            assertEquals(uuid, Util.bytesToUUID(Util.UUIDToBytes(uuid)));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBytesToUUIDInvalidLength() {
        Util.bytesToUUID(new byte[] {0x00});
    }

    @Test
    public void testAppendValidByteArrays() {
        final List<byte[]> array = Lists.newArrayList();
        final byte[] expectedArray = new byte[] {0x01, 0x02};

        array.add(Arrays.copyOfRange(expectedArray, 0, 1));
        array.add(Arrays.copyOfRange(expectedArray, 1, 2));

        assertArrayEquals(expectedArray, Util.appendByteArrays(array));
    }

    @Test(expected = NullPointerException.class)
    public void testAppendByteArraysNullContainer() {
        Util.appendByteArrays(null);
    }

    @Test
    public void testAppendValidByteArraysWithPrefix() {
        final byte prefix = (byte) 0xFF;
        final byte[] expectedArray = new byte[] {prefix, 0x01, 0x02};

        assertArrayEquals(expectedArray, Util.appendByteArraysWithPrefix(prefix,
                Arrays.copyOfRange(expectedArray, 1, 2),
                Arrays.copyOfRange(expectedArray, 2, 3)));
    }

    @Test
    public void testAppendNoByteArraysWithPrefix() {
        final byte prefix = (byte) 0xFF;
        final byte[] expectedArray = new byte[] {prefix};

        assertArrayEquals(expectedArray, Util.appendByteArraysWithPrefix(prefix));
    }

    @Test(expected = NullPointerException.class)
    public void testGenerateHexStringNullArray() {
        Util.generateHexString(null);
    }

    @Test
    public void testGenerateHexString() {
        final byte[] bytes = {0x0A, 0x0B, 0x17};

        assertEquals("0A0B17", Util.generateHexString(bytes));
    }
}
