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


package com.nearinfinity.honeycomb.hbase.util;

import com.google.inject.Provider;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provider of HTables from a pool.
 */
public class PoolHTableProvider implements Provider<HTableInterface> {
    private final HTablePool pool;
    private final String tableName;

    public PoolHTableProvider(String tableName, HTablePool pool) {
        checkNotNull(tableName);
        checkNotNull(pool);
        this.pool = pool;
        this.tableName = tableName;
    }

    @Override
    public HTableInterface get() {
        return pool.getTable(tableName);
    }
}