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

import com.nearinfinity.honeycomb.mysql.generators.RowGenerator;
import com.nearinfinity.honeycomb.mysql.generators.TableSchemaGenerator;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import net.java.quickcheck.generator.iterable.Iterables;
import org.junit.Assert;
import org.junit.Test;

public class RowTest {

    /**
     * Test that row serialization and deserialization is isomorphic in the
     * serialization direction.
     *
     * @throws Exception
     */
    @Test
    public void testSerDe() throws Exception {
        TableSchema schema = new TableSchemaGenerator().next();
        for (Row row : Iterables.toIterable(new RowGenerator(schema))) {
            Assert.assertEquals(row, Row.deserialize(row.serialize(), schema));
        }
    }
}