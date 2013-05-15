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


package com.nearinfinity.honeycomb.mysql.generators;

import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.CombinedGenerators;
import net.java.quickcheck.generator.PrimitiveGenerators;

import java.util.Collections;
import java.util.List;
import java.util.Random;

public class IndexSchemaGenerator implements Generator<IndexSchema> {
    private static final Generator<Integer> lengthGen =
            PrimitiveGenerators.integers(1, 4);
    private static final Random RAND = new Random();
    private final List<String> columnNames;

    /**
     * Maximum number of characters in an identifier
     * @see <a href="https://dev.mysql.com/doc/refman/5.5/en/identifiers.html">https://dev.mysql.com/doc/refman/5.5/en/identifiers.html</a>
     */
    public static final int MYSQL_MAX_NAME_LENGTH = 64;
    public static final Generator<String> MYSQL_NAME_GEN =
            CombinedGenerators.uniqueValues(PrimitiveGenerators.strings(1, MYSQL_MAX_NAME_LENGTH));


    public IndexSchemaGenerator(List<String> columnNames) {
        this.columnNames = Lists.newArrayList(columnNames); // Must be mutable
    }

    @Override
    public IndexSchema next() {
        Collections.shuffle(columnNames);
        List<String> columns = columnNames.subList(0, Math.min(lengthGen.next(), columnNames.size()));
        return new IndexSchema(MYSQL_NAME_GEN.next(), columns, RAND.nextBoolean());
    }
}