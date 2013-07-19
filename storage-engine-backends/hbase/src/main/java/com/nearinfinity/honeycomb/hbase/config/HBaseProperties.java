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


package com.nearinfinity.honeycomb.hbase.config;

import com.nearinfinity.honeycomb.config.BackendType;
import com.nearinfinity.honeycomb.config.Constants;

/**
 * Stores the name of HBase backend configuration properties in
 * honeycomb-site.xml and honeycomb-default.xml.  See those files
 * (or hbase-size.xml) for specific descriptions and usage.
 */
public final class HBaseProperties {

    private HBaseProperties() { throw new AssertionError(); }

    private static final String NAMESPACE = Constants.HONEYCOMB_NAMESPACE + "." + BackendType.HBASE.getName() + ".";

    public static final String AUTO_FLUSH      = NAMESPACE + "flushChangesImmediately";
    public static final String TABLE_POOL_SIZE = NAMESPACE + "tablePoolSize";
    public static final String TABLE_NAME      = NAMESPACE + "tableName";
    public static final String COLUMN_FAMILY   = "honeycomb.hbase.columnFamily";
    public static final String WRITE_BUFFER    = "hbase.client.write.buffer";

    public static final int DEFAULT_WRITE_BUFFER = 2^20; // 1 MB
    public static final boolean DEFAULT_AUTO_FLUSH = true;
    public static final int DEFAULT_TABLE_POOL_SIZE = 5;
}