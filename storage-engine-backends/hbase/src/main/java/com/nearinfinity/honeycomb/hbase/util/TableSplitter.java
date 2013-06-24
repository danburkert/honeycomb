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

import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.hbase.HBaseMetadata;
import com.nearinfinity.honeycomb.hbase.HBaseStore;
import com.nearinfinity.honeycomb.hbase.MetadataCache;
import com.nearinfinity.honeycomb.hbase.config.ConfigConstants;
import com.nearinfinity.honeycomb.hbase.config.ConfigUtil;
import com.nearinfinity.honeycomb.hbase.rowkey.DataRowKey;
import com.nearinfinity.honeycomb.hbase.rowkey.IndexRowKeyBuilder;
import com.nearinfinity.honeycomb.hbase.rowkey.SortOrder;
import com.nearinfinity.honeycomb.mysql.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class TableSplitter extends Configured implements Tool {
    static final Logger LOG = Logger.getLogger(TableSplitter.class);

    public static void split(Configuration conf, HBaseStore store, String tableName)
            throws IOException, InterruptedException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        long tableId = store.getTableId(tableName);
        String hbaseTable = conf.get(ConfigConstants.TABLE_NAME);
        List<byte[]> splits = splitPoints(tableId, store.getIndices(tableId).values());

        for (byte[] split : splits) {
            while(!splitExists(split, reallyGetTableRegions(admin, hbaseTable))) {
                String msg = "Attempting to split table " + hbaseTable + " on key: " + Util.generateHexString(split);
                if (LOG.isInfoEnabled()) {
                    LOG.info(msg);
                }
                admin.split(hbaseTable.getBytes(), split);
            }
        }
    }

    /**
     * This method should not exist.  Alas, the HBase client is a pile of crap.
     */
    private static List<HRegionInfo> reallyGetTableRegions(HBaseAdmin admin, String tableName)
            throws IOException {
        List<HRegionInfo> regions = null;
        while (regions == null) {
            regions = admin.getTableRegions(tableName.getBytes());
        }
        return regions;
    }

    /**
     * Check that the split key exists as start key of a region in the list.
     * @param split
     * @param regions
     * @return
     */
    private static boolean splitExists(byte[] split, List<HRegionInfo> regions) {
        for (HRegionInfo region : regions) {
            if (Arrays.equals(split, region.getStartKey())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Determine split points appropriate for MySQL table.  Splits at the start
     * of the tables data rows, and each index.
     * @param tableId Id of table
     * @param indexIds Ids of indices on table
     * @return
     */
    private static List<byte[]> splitPoints(long tableId, Set<Long> indexIds) {
        List<byte[]> splits = Lists.newArrayList();
        splits.add(new DataRowKey(tableId).encode());

        for (long indexId : indexIds) {
            IndexRowKeyBuilder builder = IndexRowKeyBuilder.newBuilder(tableId, indexId);
            splits.add(builder.withSortOrder(SortOrder.Ascending).build().encode());
            splits.add(builder.withSortOrder(SortOrder.Descending).build().encode());
        }
        return splits;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        if (!validateConfiguration(args, conf)) {
            return 1;
        }

        String hbaseTable = conf.get(ConfigConstants.TABLE_NAME);

        String tableName = args[0];

        if (LOG.isInfoEnabled()) {
            LOG.info("Splitting HBase table \"" + hbaseTable +
                    "\" for MySQL table \"" + tableName + "\".");
        }

        HBaseMetadata metadata = new HBaseMetadata(
                new PoolHTableProvider(hbaseTable, new HTablePool(conf, 1)));
        metadata.setColumnFamily(conf.get(ConfigConstants.COLUMN_FAMILY));
        HBaseStore store = new HBaseStore(metadata, null, new MetadataCache(metadata));

        TableSplitter.split(conf, store, tableName);

        if (LOG.isInfoEnabled()) {
            LOG.info("Done splitting HBase table \"" + hbaseTable +
                    "\" for MySQL table \"" + tableName + "\".");
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(ConfigUtil.createConfiguration(), new TableSplitter(), args));
    }

    /**
     * Check job configuration is valid.
     */
    private static boolean validateConfiguration(String[] args, Configuration conf) {
        boolean usage = false;
        if (args.length != 1) {
            System.err.println("Wrong number of arguments: " + args.length);
            usage = true;
        }
        usage |= !ConfigUtil.validateProperty(conf, ConfigConstants.TABLE_NAME, null);
        usage |= !ConfigUtil.validateProperty(conf, HConstants.ZOOKEEPER_QUORUM, null);
        if (usage) {
            usage();
            return false;
        }
        return true;
    }

    private static void usage() {
        String usage =
                "\n" + TableSplitter.class.getSimpleName() + " - Split HBase table for a Honeycomb MySQL table.\n" +
                        "\nThe following properties must be configured in honeycomb.xml, hbase-site.xml, or via the command line:\n" +
                        "\t" + ConfigConstants.TABLE_NAME + "\tHBase table\n" +
                        "\t" + HConstants.ZOOKEEPER_QUORUM + "\tZookeeper quorum list\n" +
                        "example:\n\t" +
                        TableSplitter.class.getName() + " -D " + ConfigConstants.TABLE_NAME
                        + "=sql " + " <MySQL database>/<MySQL table>\n";
        System.err.println(usage);
    }

}