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


package integrationtests;

import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;

public abstract class TestConstants {
    public static final UUID ZERO_UUID = new UUID(0L, 0L);
    public static final UUID FULL_UUID = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");

    public static final String COLUMN1 = "c1";
    public static final String COLUMN2 = "c2";
    public static final String INDEX3 = "i3";
    public static final String INDEX2 = "i2";
    public static final String INDEX1 = "i1";
    public static final String TABLE_NAME = "hbase/test";

    public static final TableSchema TABLE_SCHEMA =
            new TableSchema(
                    ImmutableList.of(
                            ColumnSchema.builder(COLUMN1, ColumnType.LONG).build(),
                            ColumnSchema.builder(COLUMN2, ColumnType.LONG).build()
                    ),
                    ImmutableList.of(
                            new IndexSchema(TestConstants.INDEX1,
                                    Lists.newArrayList(TestConstants.COLUMN1), false),
                            new IndexSchema(TestConstants.INDEX2,
                                    Lists.newArrayList(TestConstants.COLUMN1,
                                            TestConstants.COLUMN2), false)
                    )
            );

    private TestConstants() {

    }
}
