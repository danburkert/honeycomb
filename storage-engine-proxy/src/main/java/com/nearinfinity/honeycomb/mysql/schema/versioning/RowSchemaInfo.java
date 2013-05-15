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

import org.apache.avro.Schema;

import com.nearinfinity.honeycomb.mysql.gen.AvroRow;

/**
 *  Maintains all schema versioning information corresponding to {@link AvroRow}
 */
public final class RowSchemaInfo extends  BaseSchemaInfo {
    /**
     * The current version number associated with {@link AvroRow}
     */
    public static final int VER_CURRENT = 0;

    /**
     * Lookup table used to find the writer {@link Schema} used by the schema version
     * that corresponds to the position in the container
     */
    private static final Schema[] SCHEMA_CONTAINER = {
        new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"AvroRow\",\"namespace\":\"com.nearinfinity.honeycomb.mysql.gen\",\"fields\":[{\"name\":\"version\",\"type\":\"int\",\"doc\":\"Schema version number\",\"default\":0},{\"name\":\"uuid\",\"type\":{\"type\":\"fixed\",\"name\":\"UUIDContainer\",\"size\":16}},{\"name\":\"records\",\"type\":{\"type\":\"map\",\"values\":\"bytes\",\"avro.java.string\":\"String\"}}]}")
    };

    @Override
    Schema[] getSchemaContainer() {
        return SCHEMA_CONTAINER;
    }
}
