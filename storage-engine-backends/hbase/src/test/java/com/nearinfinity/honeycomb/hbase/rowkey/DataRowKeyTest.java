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


package com.nearinfinity.honeycomb.hbase.rowkey;

import static org.junit.Assert.assertArrayEquals;

import java.util.UUID;

import org.junit.Test;

import com.gotometrics.orderly.UnsignedLongRowKey;
import com.nearinfinity.honeycomb.mysql.Util;

public class DataRowKeyTest {

    private static final long TABLE_ID = 1;
    private static final byte DATA_ROW_PREFIX = 0x06;
    private static final byte[] SERIALIZED_TABLEID = new RowKeyValue(new UnsignedLongRowKey(), TABLE_ID).serialize();

    @Test(expected = IllegalArgumentException.class)
    public void testConstructDataRowInvalidTableId() {
        final long invalidTableId = -1;
        new DataRowKey(invalidTableId);
    }

    @Test
    public void testEncodeDataRow() {
        final UUID rowUUID = UUID.randomUUID();
        final DataRowKey row = new DataRowKey(TABLE_ID, rowUUID);

        final byte[] expectedEncoding = Util.appendByteArraysWithPrefix(DATA_ROW_PREFIX,
                SERIALIZED_TABLEID, Util.UUIDToBytes(rowUUID));

        assertArrayEquals(expectedEncoding, row.encode());
    }

    @Test
    public void testEncodeDataRowNullUUID() {
        final DataRowKey row = new DataRowKey(TABLE_ID);
        final byte[] expectedEncoding = Util.appendByteArraysWithPrefix(DATA_ROW_PREFIX, SERIALIZED_TABLEID);

        assertArrayEquals(expectedEncoding, row.encode());
    }
}
