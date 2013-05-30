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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Helper functions for variable length encoding data in a binary-sort safe
 * manner.
 */
public class VarEncoder {
    /**
     * Variable length encodes a long.
     *
     * @param value Long value
     * @return Variable length encoded bytes
     */
    public static byte[] encodeULong(long value) {
        checkArgument(value >= 0, "Encoded long must be non-negative");
        int size = varLongSize(value);
        byte[] encodedValue = new byte[1 + size];
        encodedValue[0] = (byte) size;
        for (int i = size; i > 0; i--) {
            encodedValue[i] = (byte) value;
            value >>>= 8;
        }
        return encodedValue;
    }

    /**
     * Decode a variable length encoded long in a byte array
     *
     * @param encodedValue Encoded byte string
     * @return Long value
     */
    public static long decodeULong(final byte[] encodedValue) {
        checkNotNull(encodedValue);
        long value = 0;
        long mask;
        for (int i = 1; i < encodedValue.length; i++) {
            mask = 0xFF & encodedValue[i];
            value = value << 8;
            value |= mask;
        }
        return value;
    }

    /**
     * Combine many byte arrays into one
     *
     * @param arrays List of byte arrays
     * @return Combined byte array
     */
    public static byte[] appendByteArrays(List<byte[]> arrays) {
        checkNotNull(arrays);
        int size = 0;
        for (byte[] array : arrays) {
            size += array.length;
        }
        ByteBuffer bb = ByteBuffer.allocate(size);
        for (byte[] array : arrays) {
            bb.put(array);
        }
        return bb.array();
    }

    /**
     * Combine many byte arrays into one with a prefix at the beginning of the combined array.
     *
     * @param prefix Byte prefix
     * @param arrays Many byte arrays
     * @return Combined byte array
     */
    public static byte[] appendByteArraysWithPrefix(byte prefix, byte[]... arrays) {
        checkNotNull(prefix);
        List<byte[]> elements = new ArrayList<byte[]>();
        byte[] prefixBytes = {prefix};
        elements.add(prefixBytes);
        elements.addAll(Arrays.asList(arrays));
        return appendByteArrays(elements);
    }

    private static int varLongSize(final long value) {
        if ((value & (0xffffffffffffffffL << 8)) == 0) return 1;
        if ((value & (0xffffffffffffffffL << 16)) == 0) return 2;
        if ((value & (0xffffffffffffffffL << 24)) == 0) return 3;
        if ((value & (0xffffffffffffffffL << 32)) == 0) return 4;
        if ((value & (0xffffffffffffffffL << 40)) == 0) return 5;
        if ((value & (0xffffffffffffffffL << 48)) == 0) return 6;
        if ((value & (0xffffffffffffffffL << 56)) == 0) return 7;
        return 8;
    }
}