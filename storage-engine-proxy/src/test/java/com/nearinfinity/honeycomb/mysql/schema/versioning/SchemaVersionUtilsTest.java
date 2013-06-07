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


package com.nearinfinity.honeycomb.mysql.schema.versioning;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.nearinfinity.honeycomb.exceptions.UnknownSchemaVersionException;

public class SchemaVersionUtilsTest {

    @Test(expected = UnknownSchemaVersionException.class)
    public void testProcessSchemaVersionUnsupportedVersion() {
        SchemaVersionUtils.processSchemaVersion((byte)0x02, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testProcessSchemaVersionNegativeSupportedVersion() {
        SchemaVersionUtils.processSchemaVersion((byte)0x00, -1);
    }

    @Test
    public void testProcessSchemaVersionSupportedVersion() {
        assertTrue(SchemaVersionUtils.processSchemaVersion((byte)0x7E, 63));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecodeAvroSchemaVersionExceedMinimumValue() {
        SchemaVersionUtils.decodeAvroSchemaVersion((byte)-5);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecodeAvroSchemaVersionExceedMaximumValue() {
        SchemaVersionUtils.decodeAvroSchemaVersion((byte)0x7F);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecodeAvroSchemaVersionOddEncodedValue() {
        SchemaVersionUtils.decodeAvroSchemaVersion((byte)0x01);
    }
}
