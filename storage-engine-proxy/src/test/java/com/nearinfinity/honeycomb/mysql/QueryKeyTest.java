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

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.nearinfinity.honeycomb.mysql.gen.QueryType;

public class QueryKeyTest {

    private static final String TEST_INDEX = "testIndex";

    @Test(expected = NullPointerException.class)
    public void testCreationInvalidQueryFields() {
        new QueryKey(TEST_INDEX, QueryType.EXACT_KEY, null);
    }

    @Test(expected = NullPointerException.class)
    public void testCreationInvalidQueryType() {
        new QueryKey(TEST_INDEX, null, ImmutableMap.<String, ByteBuffer>of());
    }

    @Test(expected = NullPointerException.class)
    public void testCreationInvalidIndexName() {
        new QueryKey(null, QueryType.EXACT_KEY, ImmutableMap.<String, ByteBuffer>of());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIndexSchemaCreationEmptyIndexName() {
        new QueryKey("", QueryType.EXACT_KEY, ImmutableMap.<String, ByteBuffer>of());
    }

    @Test(expected = NullPointerException.class)
    public void testDeserializeInvalidSerializedSchema() {
        QueryKey.deserialize(null);
    }

    @Test
    public void testDeserializeValidSerializedSchemaAndIndexName() {
        final QueryKey key = new QueryKey(TEST_INDEX, QueryType.EXACT_KEY, ImmutableMap.<String, ByteBuffer>of());
        final QueryKey actualKey = QueryKey.deserialize(key.serialize());

        assertEquals(key.getIndexName(), actualKey.getIndexName());
        assertEquals(key.getQueryType(), actualKey.getQueryType());
        assertEquals(key.getKeys(), actualKey.getKeys());
    }
}
