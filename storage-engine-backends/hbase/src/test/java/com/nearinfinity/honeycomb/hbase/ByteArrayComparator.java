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


package com.nearinfinity.honeycomb.hbase;

import java.util.Comparator;

import com.google.common.primitives.UnsignedBytes;

/**
 * Comparator to sort byte arrays.  Short byte arrays sort first.  Arrays of
 * equal length sort according to byte value comparison from left to right.
 */
public class ByteArrayComparator implements Comparator<byte[]> {
    @Override
    public int compare(byte[] b1, byte[] b2) {
        if (b1.length != b2.length) {
            return b1.length - b2.length;
        }

        for (int i = 0; i < b1.length; i++) {
            if (b1[i] == b2[i]) {
                continue;
            }
            return UnsignedBytes.compare(b1[i], b2[i]);
        }

        return 0;
    }
}