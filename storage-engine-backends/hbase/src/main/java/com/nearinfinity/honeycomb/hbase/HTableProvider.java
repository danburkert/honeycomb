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


package com.nearinfinity.honeycomb.hbase;

import com.google.inject.Provider;
import com.nearinfinity.honeycomb.hbase.config.ConfigConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * Constructs new {@link HTableInterface} instances from a pool.
 */
public class HTableProvider implements Provider<HTableInterface> {

    private final HTablePool tablePool;
    private final String tableName;

    public HTableProvider(final Configuration configuration) {
        String hTableName = configuration.get(ConfigConstants.TABLE_NAME);
        long writeBufferSize = configuration.getLong(ConfigConstants.WRITE_BUFFER,
                ConfigConstants.DEFAULT_WRITE_BUFFER);
        int poolSize = configuration.getInt(ConfigConstants.TABLE_POOL_SIZE,
                ConfigConstants.DEFAULT_TABLE_POOL_SIZE);
        boolean autoFlush = configuration.getBoolean(ConfigConstants.AUTO_FLUSH,
                ConfigConstants.DEFAULT_AUTO_FLUSH);

        tableName = hTableName;
        tablePool = new HTablePool(configuration, poolSize,
                new HTableFactory(writeBufferSize, autoFlush));
    }

    @Override
    public HTableInterface get() {
        return tablePool.getTable(tableName);
    }
}
