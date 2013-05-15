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


package com.nearinfinity.honeycomb.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;

import java.io.IOException;

public class HTableFactory implements HTableInterfaceFactory {
    private final long writeBufferSize;
    private final boolean autoFlush;

    public HTableFactory(long writeBufferSize, boolean autoFlush) {
        this.writeBufferSize = writeBufferSize;
        this.autoFlush = autoFlush;
    }

    @Override
    public HTableInterface createHTableInterface(Configuration config, byte[] tableName) {
        try {
            HTable table = new HTable(config, tableName);
            table.setAutoFlush(autoFlush);
            table.setWriteBufferSize(writeBufferSize);
            return table;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void releaseHTableInterface(HTableInterface table) throws IOException {
        table.close();
    }
}