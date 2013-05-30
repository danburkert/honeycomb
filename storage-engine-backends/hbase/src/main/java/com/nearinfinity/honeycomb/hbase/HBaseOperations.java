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

import com.google.common.base.Objects;
import com.nearinfinity.honeycomb.exceptions.RuntimeIOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Encapsulate HBase actions such as Get, Put and Delete
 */
public class HBaseOperations {
    private static final Logger logger = Logger.getLogger(HBaseOperations.class);

    public static void performPut(HTableInterface hTable, List<Put> puts) {
        try {
            hTable.put(puts);
        } catch (IOException e) {
            throw createException("HBase table put list failed", e, hTable);
        }
    }

    public static void performDelete(HTableInterface hTable, List<Delete> deletes) {
        try {
            hTable.delete(deletes);
        } catch (IOException e) {
            throw createException("HBase table delete failed", e, hTable);
        }
    }

    public static void performFlush(HTableInterface hTable) {
        try {
            hTable.flushCommits();
        } catch (IOException e) {
            throw createException("HBase table flush failed", e, hTable);
        }
    }

    public static void closeTable(HTableInterface hTable) {
        try {
            hTable.close();
        } catch (IOException e) {
            throw createException("HBase close table failed", e, hTable);
        }
    }

    public static Result performGet(HTableInterface hTable, Get get) {
        try {
            return hTable.get(get);
        } catch (IOException e) {
            String msg = String.format("HBase table get failed for get %s", get.toString());
            throw createException(msg, e, hTable);
        }
    }

    public static long performIncrementColumnValue(HTableInterface hTable, byte[] row, byte[] columnFamily, byte[] identifier, long amount) {
        try {
            return hTable.incrementColumnValue(row, columnFamily, identifier, amount);
        } catch (IOException e) {
            String msg = String.format("HBase table increment column threw exception. Row (%s) / Column Family (%s) / Identifier (%s) / Amount (%d)",
                    Bytes.toStringBinary(row),
                    Bytes.toStringBinary(columnFamily),
                    Bytes.toStringBinary(identifier),
                    amount);
            throw createException(msg, e, hTable);
        }
    }

    public static ResultScanner getScanner(HTableInterface hTable, Scan scan) {
        try {
            return hTable.getScanner(scan);
        } catch (IOException e) {
            throw createException("HBase table get scanner failed", e, hTable);
        }
    }

    private static RuntimeException createException(String errorMessage, IOException e, HTableInterface hTable) {
        Configuration configuration = hTable.getConfiguration();
        String configSettings = Objects.toStringHelper(configuration)
                .add(HConstants.ZOOKEEPER_QUORUM, configuration.get(HConstants.ZOOKEEPER_QUORUM))
                .toString();
        final String hTableName = new String(hTable.getTableName());
        String format = String.format("%s %s (HTable name %s)", errorMessage, configSettings, hTableName);
        logger.error(format, e);
        return new RuntimeIOException(format, e);
    }
}
