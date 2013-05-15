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

import com.google.common.collect.ImmutableList;
import com.nearinfinity.honeycomb.mysql.QueryKey;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.gen.QueryType;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import net.java.quickcheck.Generator;
import net.java.quickcheck.generator.PrimitiveGenerators;

public class QueryKeyGenerator implements Generator<QueryKey> {
    private static final Generator<QueryType> queryTypes =
            PrimitiveGenerators.enumValues(QueryType.class);
    private final Generator<Row> rows;
    private final Generator<String> indices;


    public QueryKeyGenerator(TableSchema schema) {
        super();
        this.rows = new RowGenerator(schema);
        ImmutableList.Builder<String> indices = ImmutableList.builder();
        for (IndexSchema indexSchema : schema.getIndices()) {
            indices.add(indexSchema.getIndexName());
        }
        this.indices = PrimitiveGenerators.fixedValues(indices.build());
    }

    @Override
    public QueryKey next() {
        return new QueryKey(indices.next(), queryTypes.next(), rows.next().getRecords());
    }
}