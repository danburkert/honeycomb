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


package com.nearinfinity.honeycomb.mysql.schema.versioning;

import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;

public class RowSchemaInfoTest {
    private RowSchemaInfo rowSchemaInfo;

    @Before
    public void setupTestCases() {
        rowSchemaInfo = new RowSchemaInfo();
    }


    @Test(expected = IllegalArgumentException.class)
    public void testFindSchemaNegativeSchemaVersion() {
        rowSchemaInfo.findSchema(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFindSchemaExceedAvailableVersions() {
        rowSchemaInfo.findSchema(Integer.MAX_VALUE);
    }

    @Test
    public void testFindSchemaValidSchemaVersion() {
        assertNotNull(rowSchemaInfo.findSchema(0));
    }
}
