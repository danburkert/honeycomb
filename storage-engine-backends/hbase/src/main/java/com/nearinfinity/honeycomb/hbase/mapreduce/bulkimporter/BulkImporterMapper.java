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


package com.nearinfinity.honeycomb.hbase.mapreduce.bulkimporter;

import com.nearinfinity.honeycomb.hbase.HBaseMetadata;
import com.nearinfinity.honeycomb.hbase.HBaseStore;
import com.nearinfinity.honeycomb.hbase.MetadataCache;
import com.nearinfinity.honeycomb.hbase.MutationFactory;
import com.nearinfinity.honeycomb.hbase.config.ConfigConstants;
import com.nearinfinity.honeycomb.hbase.util.PoolHTableProvider;
import com.nearinfinity.honeycomb.mysql.Row;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import static java.lang.String.format;

/**
 * Mapper for loading large amounts of data into Honeycomb format.
 */
public class BulkImporterMapper
        extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    static final Logger LOG = Logger.getLogger(BulkImporterMapper.class);
    private RowParser rowParser;
    private long tableId;
    private MutationFactory mutationFactory;

    @Override
    protected void setup(Context context)
            throws IOException {
        Configuration conf = context.getConfiguration();

        char separator  = conf.get(ConfigConstants.INPUT_SEPARATOR,
                ConfigConstants.DEFAULT_INPUT_SEPARATOR).charAt(0);
        String[] columns = conf.getStrings(ConfigConstants.SQL_COLUMNS);
        String sqlTable = conf.get(ConfigConstants.SQL_DB_TABLE);
        String hbaseTable  = conf.get(ConfigConstants.TABLE_NAME);
        String columnFamily = conf.get(ConfigConstants.COLUMN_FAMILY);

        LOG.info("Zookeeper: " + conf.get(HConstants.ZOOKEEPER_QUORUM));
        LOG.info("SQL Table: " + sqlTable);
        LOG.info("HBase Table: " + hbaseTable);
        LOG.info("HBase Column Family: " + columnFamily);
        LOG.info("Input separator: '" + separator + "'");

        HBaseMetadata metadata = new HBaseMetadata(
                new PoolHTableProvider(hbaseTable, new HTablePool(conf, 1)));
        metadata.setColumnFamily(conf.get(ConfigConstants.COLUMN_FAMILY));
        HBaseStore store = new HBaseStore(metadata, null, new MetadataCache(metadata));

        tableId = store.getTableId(sqlTable);
        TableSchema schema = store.getSchema(sqlTable);
        mutationFactory = new MutationFactory(store);
        mutationFactory.setColumnFamily(columnFamily);

        rowParser = new RowParser(schema, columns, separator);
    }

    @Override
    public void map(LongWritable offset, Text line, Context context) {
        try {
            Row row = rowParser.parseRow(line.toString());

            List<Put> puts = mutationFactory.insert(tableId, row);

            for (Put put : puts) {
                context.write(new ImmutableBytesWritable(put.getRow()), put);
            }

            context.getCounter(Counters.ROWS).increment(1);
            context.getCounter(Counters.PUTS).increment(puts.size());

        } catch (IOException e) {
            LOG.error("CSVParser unable to parse line: " + line.toString(), e);
            context.getCounter(Counters.FAILED_ROWS).increment(1);
        } catch (IllegalArgumentException e) {
            LOG.error(format("The line %s was incorrectly formatted. Error %s",
                    line.toString(), e.getMessage()));
            context.getCounter(Counters.FAILED_ROWS).increment(1);
        } catch (ParseException e) {
            LOG.error(format("Parsing failed on line %s with message %s",
                    line.toString(), e.getMessage()));
            context.getCounter(Counters.FAILED_ROWS).increment(1);
        } catch (Exception e) {
            LOG.error(format("The following error %s occurred during mapping" +
                    " for line %s", e.getMessage(), line.toString()));
            context.getCounter(Counters.FAILED_ROWS).increment(1);
        }
    }

    /**
     * Counters for the map / reduce job.
     */
    public enum Counters {
        /**
         * Number of rows processed by the job
         */
        ROWS,
        /**
         * Number of rows that failed during processing
         */
        FAILED_ROWS,
        /**
         * Total size of the {@link Put}s generated by the job
         */
        PUTS
    }
}