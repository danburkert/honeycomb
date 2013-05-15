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

import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.CombinedGenerators;
import net.java.quickcheck.generator.PrimitiveGenerators;

import java.util.Random;

public class ColumnSchemaGenerator implements Generator<ColumnSchema> {
    //    private static final int MYSQL_MAX_VARCHAR = 65535;
    private static final int MYSQL_MAX_VARCHAR = 128; // Set artificially low so tests are fast
    private static final Generator<ColumnType> typeGen = PrimitiveGenerators.enumValues(ColumnType.class);
    private static final Generator<Integer> lengthGen = PrimitiveGenerators.integers(0, MYSQL_MAX_VARCHAR);
    private static final Random RAND = new Random();

    /**
     * Maximum number of characters in an identifier
     * @see <a href="https://dev.mysql.com/doc/refman/5.5/en/identifiers.html">https://dev.mysql.com/doc/refman/5.5/en/identifiers.html</a>
     */
    public static final int MYSQL_MAX_NAME_LENGTH = 64;
    public static final Generator<String> MYSQL_NAME_GEN =
            CombinedGenerators.uniqueValues(PrimitiveGenerators.strings(1, MYSQL_MAX_NAME_LENGTH));

    @Override
    public ColumnSchema next() {
        ColumnType type = typeGen.next();
        ColumnSchema.Builder builder = ColumnSchema.builder(MYSQL_NAME_GEN.next(), type);
        switch (type) {
            case STRING:
            case BINARY:
                builder.setMaxLength(lengthGen.next());
                break;
            case LONG:
            case ULONG:
            case DOUBLE:
                builder.setIsAutoIncrement(RAND.nextBoolean());
                break;
            case DECIMAL:
                int precision = RAND.nextInt(66);
                int scale = RAND.nextInt(Math.max(31, precision));
                builder.setPrecision(precision).setScale(scale);
                break;
            default:
                break;
        }
        return builder.build();
    }
}
