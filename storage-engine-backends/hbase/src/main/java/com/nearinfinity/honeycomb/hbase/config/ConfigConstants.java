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

import com.nearinfinity.honeycomb.config.AdapterType;
import com.nearinfinity.honeycomb.config.Constants;

/**
 * Stores the name of the configuration option tags in honeycomb.xml
 */
public final class ConfigConstants {

    private ConfigConstants() {
        throw new AssertionError();
    }

    private static final String NAMESPACE = Constants.HONEYCOMB_NAMESPACE + "." + AdapterType.HBASE.getName() + ".";
    /**
     * Property name of auto flushing writes to HBase
     */
    public static final String AUTO_FLUSH = NAMESPACE + "flushChangesImmediately";
    /**
     * Default behavior for auto flushing
     */
    public static final boolean DEFAULT_AUTO_FLUSH = false;
    /**
     * Property name for setting the HTable pool size
     */
    public static final String TABLE_POOL_SIZE = NAMESPACE + "tablePoolSize";

    /**
     * Default number of references to keep active for a table
     */
    public static final int DEFAULT_TABLE_POOL_SIZE = 5;
    /**
     * Property name for setting the name of the HTable
     */
    public static final String TABLE_NAME = NAMESPACE + "tableName";
    /**
     * Property name for setting the column family of the HBase HTable
     */
    // This needs to change whenever NAMESPACE changes. Due to Guice issues with injecting named non-consts.
    public static final String COLUMN_FAMILY = "honeycomb.hbase.columnFamily";
    /**
     * Property name for setting the HBase write buffer
     */
    public static final String WRITE_BUFFER = "hbase.client.write.buffer";
    /**
     * Default value of the HBase write buffer
     */
    public static final long DEFAULT_WRITE_BUFFER = 2097152;
    /**
     * Property name for pre splitting flag
     */
    public static final String PRE_SPLIT = NAMESPACE + "import.preSplitTable";
    /**
     * Pre split tables during import by default
     */
    public static boolean DEFAULT_PRE_SPLIT = true;
    /**
     * Property name for field separator in bulk import input
     */
    public static final String INPUT_SEPARATOR = NAMESPACE + "import.separator";
    /**
     * Default input separator
     */
    public static final String DEFAULT_INPUT_SEPARATOR = ",";
    /**
     * Property name for intermediate file storage directory during bulk import
     */
    public static final String OUTPUT_PATH = NAMESPACE + "import.output";
    /**
     * Default output path for map reduce job
     */
    public static final String DEFAULT_OUTPUT_PATH = "hdfs:///tmp";
    /**
     * Property name for MySQL table columns
     */
    public static final String SQL_COLUMNS = NAMESPACE + "import.sql.columns";
    /**
     * Property name for full MySQL database / table name
     */
    public static final String SQL_DB_TABLE = NAMESPACE + "import.sql.db-table";
}