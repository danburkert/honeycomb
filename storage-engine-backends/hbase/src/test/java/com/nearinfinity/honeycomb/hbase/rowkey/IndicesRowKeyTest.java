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


package com.nearinfinity.honeycomb.hbase.rowkey;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

import com.nearinfinity.honeycomb.hbase.VarEncoder;

public class IndicesRowKeyTest {
    private static final long TABLE_ID = 1;
    private static final byte INDICES_ROW_PREFIX = 0x02;

    @SuppressWarnings("unused")
    @Test(expected = IllegalArgumentException.class)
    public void testConstructIndicesRowInvalidTableId() {
        final long invalidTableId = -1;
        new IndicesRowKey(invalidTableId);
    }

    @Test
    public void testEncodeIndicesRow() {
        final IndicesRowKey row = new IndicesRowKey(TABLE_ID);

        final byte[] expectedEncoding = VarEncoder.appendByteArraysWithPrefix(INDICES_ROW_PREFIX,
                                    VarEncoder.encodeULong(TABLE_ID));

        assertArrayEquals(expectedEncoding, row.encode());
    }
}
