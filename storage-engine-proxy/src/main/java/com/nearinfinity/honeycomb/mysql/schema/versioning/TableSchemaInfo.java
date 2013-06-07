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

import org.apache.avro.Schema;

import com.nearinfinity.honeycomb.mysql.gen.AvroTableSchema;

/**
 *  Maintains all schema versioning information corresponding to {@link AvroTableSchema}
 */
public final class TableSchemaInfo extends BaseSchemaInfo {
    /**
     * The current version number associated with {@link AvroTableSchema}
     */
    public static final int VER_CURRENT = 0;

    /**
     * Lookup table used to find the writer {@link Schema} used by the schema version
     * that corresponds to the position in the container
     */
    private static final Schema[] SCHEMA_CONTAINER = {
        new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"AvroTableSchema\",\"namespace\":\"com.nearinfinity.honeycomb.mysql.gen\",\"fields\":[{\"name\":\"version\",\"type\":\"int\",\"doc\":\"Schema version number\",\"default\":0},{\"name\":\"columns\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"AvroColumnSchema\",\"fields\":[{\"name\":\"type\",\"type\":{\"type\":\"enum\",\"name\":\"ColumnType\",\"symbols\":[\"STRING\",\"BINARY\",\"ULONG\",\"LONG\",\"DOUBLE\",\"DECIMAL\",\"TIME\",\"DATE\",\"DATETIME\"]}},{\"name\":\"isNullable\",\"type\":\"boolean\",\"default\":true},{\"name\":\"isAutoIncrement\",\"type\":\"boolean\",\"default\":false},{\"name\":\"maxLength\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"scale\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"precision\",\"type\":[\"null\",\"int\"],\"default\":null}]},\"avro.java.string\":\"String\"}},{\"name\":\"indices\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"AvroIndexSchema\",\"fields\":[{\"name\":\"columns\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"string\",\"avro.java.string\":\"String\"}}},{\"name\":\"isUnique\",\"type\":\"boolean\",\"default\":false}]},\"avro.java.string\":\"String\"}}]}")
    };

    @Override
    Schema[] getSchemaContainer() {
        return SCHEMA_CONTAINER;
    }
}
