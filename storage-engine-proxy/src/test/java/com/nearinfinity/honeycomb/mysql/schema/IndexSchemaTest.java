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


package com.nearinfinity.honeycomb.mysql.schema;

import static org.junit.Assert.assertEquals;
import nl.jqno.equalsverifier.EqualsVerifier;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class IndexSchemaTest {

    private static final String INDEX_A = "INDEX_A";
    private static final String COLUMN_A = "colA";

    private static final IndexSchema TEST_INDEX_SCHEMA = new IndexSchema(INDEX_A,
            ImmutableList.<String>of(COLUMN_A), false);


    @Test(expected = NullPointerException.class)
    public void testIndexSchemaCreationInvalidColumns() {
        new IndexSchema(INDEX_A, null, true);
    }

    @Test(expected = NullPointerException.class)
    public void testIndexSchemaCreationNullIndexName() {
        new IndexSchema(null, ImmutableList.<String>of(), true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIndexSchemaCreationEmptyIndexName() {
        new IndexSchema("", ImmutableList.<String>of(), true);
    }

    @Test
    public void testIndexSchemaCreation() {
        new IndexSchema(INDEX_A, ImmutableList.<String>of(), true);
    }

    @Test(expected = NullPointerException.class)
    public void testDeserializeNullSerializedSchema() {
        IndexSchema.deserialize(null, INDEX_A);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeserializeEmptyIndexName() {
        IndexSchema.deserialize(TEST_INDEX_SCHEMA.serialize(), "");
    }

    @Test(expected = NullPointerException.class)
    public void testDeserializeInvalidIndexName() {
        IndexSchema.deserialize(TEST_INDEX_SCHEMA.serialize(), null);
    }

    @Test
    public void testDeserializeValidSerializedSchemaAndIndexName() {
        final IndexSchema actualSchema = IndexSchema.deserialize(TEST_INDEX_SCHEMA.serialize(),
                TEST_INDEX_SCHEMA.getIndexName());

        assertEquals(TEST_INDEX_SCHEMA.getIndexName(), actualSchema.getIndexName());
        assertEquals(TEST_INDEX_SCHEMA, actualSchema);
    }

    @Test
    public void equalsContract() {
        EqualsVerifier.forClass(IndexSchema.class).verify();
    }
}
