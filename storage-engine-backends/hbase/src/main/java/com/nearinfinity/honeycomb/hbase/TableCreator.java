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

import com.nearinfinity.honeycomb.hbase.config.ConfigConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Creates the HTable used by Honeycomb
 */
public class TableCreator {
    private static final Logger logger = Logger.getLogger(TableCreator.class);

    /**
     * Creates a table in HBase to store all Honeycomb tables
     *
     * @param configuration Configuration of the HTable
     * @throws IOException
     */
    public static void createTable(Configuration configuration)
            throws IOException {
        HTableDescriptor tableDescriptor;
        try {
            HBaseAdmin.checkHBaseAvailable(configuration);
        } catch (MasterNotRunningException e) {
            logger.fatal(String.format("HMaster doesn't appear to be running. Zookeeper quorum: %s", configuration.get("hbase.zookeeper.quorum")), e);
            throw e;
        } catch (ZooKeeperConnectionException e) {
            logger.fatal("Failed to connect to zookeeper when checking HBase. Zookeeper quorum: " + configuration.get("hbase.zookeeper.quorum"), e);
            throw e;
        }

        String columnFamily = configuration.get(ConfigConstants.COLUMN_FAMILY);
        byte[] tableName = configuration.get(ConfigConstants.TABLE_NAME).getBytes();

        HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
        HBaseAdmin admin = new HBaseAdmin(configuration);

        columnDescriptor.setBloomFilterType(StoreFile.BloomType.ROW)
                .setDataBlockEncoding(DataBlockEncoding.PREFIX)
//                .setCompressionType(Compression.Algorithm.SNAPPY)
                .setMaxVersions(1);

        if (!admin.tableExists(tableName)) {
            logger.info("Creating HBase table");
            tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.addFamily(columnDescriptor);

            admin.createTable(tableDescriptor);
        }

        tableDescriptor = admin.getTableDescriptor(tableName);
        if (!tableDescriptor.hasFamily(columnFamily.getBytes())) {
            logger.info("Adding column family to HBase table");

            if (!admin.isTableDisabled(tableName)) {
                logger.info("Disabling HBase table");
                admin.disableTable(tableName);
            }

            admin.addColumn(tableName, columnDescriptor);
        }

        if (admin.isTableDisabled(tableName)) {
            logger.info("Enabling HBase table");
            admin.enableTable(tableName);
        }

        try {
            admin.flush(tableName);
        } catch (InterruptedException e) {
            logger.warn("HBaseAdmin flush was interrupted. Retrying.");
            try {
                admin.flush(tableName);
            } catch (InterruptedException e1) {
                throw new RuntimeException(e1);
            }
        }

        logger.info("HBase table successfully initialized.");
    }
}