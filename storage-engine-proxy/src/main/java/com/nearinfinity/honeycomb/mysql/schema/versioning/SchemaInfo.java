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

/**
 * Interface that all serialized data types that require versioning support must
 * use to maintain schema version information over time
 */
public interface SchemaInfo {
    /**
     * Attempts to find the writer {@link Schema} used for the specified schema version
     *
     * @param version The version number of interest, must be non-negative and within the range of available schemas
     * @return The writer schema used for the specified version
     */
    public Schema findSchema(final int version);
}
