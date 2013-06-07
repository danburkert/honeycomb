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

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.avro.Schema;

/**
 * Base class used by {@link SchemaInfo} implementations to provide standard
 * schema lookup behavior
 */
public abstract class BaseSchemaInfo implements SchemaInfo {

    /**
     * Default container used for versioned schema retrievals
     */
    private static final Schema[] EMPTY_CONTAINER = new Schema[0];

    /**
     * Provides the container of versioned {@link Schema} objects to use
     * during schema retrieval
     *
     * @return The schema storage container
     */
    Schema[] getSchemaContainer() {
        return EMPTY_CONTAINER;
    }

    @Override
    public Schema findSchema(final int version) {
        final Schema[] container = getSchemaContainer();

        checkArgument(version >= 0 && version < container.length);
        return container[version];
    }
}
