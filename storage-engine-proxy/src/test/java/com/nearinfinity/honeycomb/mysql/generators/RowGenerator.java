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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import net.java.quickcheck.Generator;

import com.google.common.collect.ImmutableMap;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;

public class RowGenerator implements Generator<Row> {
    private static final Generator<UUID> uuids = new UUIDGenerator();
    private final Map<String, Generator<ByteBuffer>> recordGenerators;

    public RowGenerator(TableSchema schema) {
        super();
        ImmutableMap.Builder<String, Generator<ByteBuffer>> recordGenerators = ImmutableMap.builder();
        for (ColumnSchema column : schema.getColumns()) {
            recordGenerators.put(column.getColumnName(), new FieldGenerator(column));
        }
        this.recordGenerators = recordGenerators.build();
    }

    @Override
    public Row next() {
        ImmutableMap.Builder<String, ByteBuffer> records = ImmutableMap.builder();
        for (Map.Entry<String, Generator<ByteBuffer>> record : recordGenerators.entrySet()) {
            ByteBuffer nextValue = record.getValue().next();
            if (nextValue != null) {
                records.put(record.getKey(), nextValue);
            }
        }
        return new Row(records.build(), uuids.next());
    }
}