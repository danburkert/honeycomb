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


package com.nearinfinity.honeycomb.hbase.mapreduce;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.nearinfinity.honeycomb.Store;
import com.nearinfinity.honeycomb.hbase.HBaseMetadata;
import com.nearinfinity.honeycomb.hbase.HBaseStore;
import com.nearinfinity.honeycomb.hbase.MetadataCache;
import com.nearinfinity.honeycomb.hbase.config.ConfigConstants;
import com.nearinfinity.honeycomb.hbase.config.ConfigUtil;
import com.nearinfinity.honeycomb.hbase.mapreduce.bulkimporter.BulkImporterMapper;
import com.nearinfinity.honeycomb.hbase.util.PoolHTableProvider;
import com.nearinfinity.honeycomb.hbase.util.TableSplitter;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class BulkImporter extends Configured implements Tool {
    static final Logger LOG = Logger.getLogger(BulkImporter.class);

    private static Job createJob(Configuration conf, String inputPath, String outputPath)
            throws IOException {
        TableMapReduceUtil.addDependencyJars(conf,
                Store.class, // honeycomb jar
                HBaseStore.class // honeycomb-hbase jar
        );
        Job job = new Job(conf, "Honeycomb BulkImport table " + conf.get(ConfigConstants.SQL_DB_TABLE));
        TableMapReduceUtil.addDependencyJars(job);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        FileInputFormat.setInputPaths(job, new Path(inputPath));

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(BulkImporterMapper.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        HTable hTable = new HTable(conf, conf.get(ConfigConstants.TABLE_NAME));
        HFileOutputFormat.configureIncrementalLoad(job, hTable);
        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        if (!validateConfiguration(args, conf)) {
            return 1;
        }
        String sqlTable = args[1];
        conf.set(ConfigConstants.SQL_DB_TABLE, sqlTable);
        conf.setStrings(ConfigConstants.SQL_COLUMNS, Arrays.copyOfRange(args, 2, args.length));
        SchemaMetrics.configureGlobally(conf);

        String hbaseTable = conf.get(ConfigConstants.TABLE_NAME);

        HBaseMetadata metadata = new HBaseMetadata(new PoolHTableProvider(hbaseTable, new HTablePool(conf, 1)));
        metadata.setColumnFamily(conf.get(ConfigConstants.COLUMN_FAMILY));
        HBaseStore store = new HBaseStore(metadata, null, new MetadataCache(metadata));
        if (!validateColumns(store, conf)) {
            return 1;
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Bulk Importing into HBase table \"" + hbaseTable +
                    "\" and MySQL table \"" + sqlTable + "\".");
        }

        if(conf.getBoolean(ConfigConstants.PRE_SPLIT, ConfigConstants.DEFAULT_PRE_SPLIT)) {
            TableSplitter splitter = new TableSplitter();
            splitter.setConf(conf);
            splitter.run(new String[] {sqlTable});
        }

        String outputPath = conf.get(ConfigConstants.OUTPUT_PATH, ConfigConstants.DEFAULT_OUTPUT_PATH)
                + "/" + sqlTable + "-" + System.currentTimeMillis();

        Job job = createJob(conf, args[0], outputPath);

        boolean mrResult = job.waitForCompletion(true);

        if (mrResult) {
            LoadIncrementalHFiles load = new LoadIncrementalHFiles(conf);
            int loadResult = load.run(
                    new String[] {outputPath, hbaseTable});

            if (loadResult != 0) {
                System.err.println("Map/Reduce job succeeded, but LoadIncrementalHFiles failed.\n" +
                        "The HFiles are in " + outputPath);
                return 1;
            } else {
                if (FileUtil.fullyDelete(new File(outputPath))) {
                    System.out.println("Bulk import complete!");
                    return 0;
                } else {
                    System.err.println("Bulk import succeeded, but intermediate files in " +
                    outputPath + " could not be cleaned up.");
                    return 1;
                }
            }
        } else {
            System.err.println("BulkImporter map/reduce job failed.  Removing intermediate directory.");
            if (!FileUtil.fullyDelete(new File(outputPath))) {
                System.err.println("Intermediate files in " +
                        outputPath + " could not be cleaned up.");
            }
            return 1;
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(ConfigUtil.createConfiguration(), new BulkImporter(), args));
    }

    private static boolean validateColumns(HBaseStore store, Configuration conf) {
        String sqlTable = conf.get(ConfigConstants.SQL_DB_TABLE);
        TableSchema schema = store.getSchema(sqlTable);
        Set<String> expectedColumns = Sets.newHashSet();
        for (ColumnSchema column : schema.getColumns()) {
            expectedColumns.add(column.getColumnName());
        }
        List<String> invalidColumns = Lists.newLinkedList();
        for (String column : conf.getStrings(ConfigConstants.SQL_COLUMNS)) {
            if (!expectedColumns.contains(column)) {
                LOG.error("Found non-existent column " + column);
                invalidColumns.add(column);
            }
        }
        if (invalidColumns.size() > 0) {
            String expectedColumnString = Joiner.on(",").join(expectedColumns);
            String invalidColumnString = Joiner.on(",").join(invalidColumns);
            String message = String.format("In table %s following columns (%s)" +
                    " are not valid columns. Expected columns (%s)",
                    sqlTable, invalidColumnString, expectedColumnString);
            System.err.println(message);
            return false;
        }
        return true;
    }

    /**
     * Check job configuration is valid.
     */
    private static boolean validateConfiguration(String[] args, Configuration conf) {
        boolean exit = false;
        if (args.length <= 2) {
            System.err.println("Wrong number of arguments: " + args.length);
            exit = true;
        }
        exit |= !ConfigUtil.validateProperty(conf, ConfigConstants.TABLE_NAME, null);
        exit |= !ConfigUtil.validateProperty(conf, ConfigConstants.COLUMN_FAMILY, null);
        exit |= !ConfigUtil.validateProperty(conf, HConstants.ZOOKEEPER_QUORUM, null);
        ConfigUtil.validateProperty(conf, ConfigConstants.PRE_SPLIT, ConfigConstants.DEFAULT_PRE_SPLIT);
        ConfigUtil.validateProperty(conf, ConfigConstants.INPUT_SEPARATOR, ConfigConstants.DEFAULT_INPUT_SEPARATOR);
        ConfigUtil.validateProperty(conf, ConfigConstants.OUTPUT_PATH, ConfigConstants.DEFAULT_OUTPUT_PATH);
        if (exit) {
            usage();
            return false;
        }
        return true;
    }

    private static void usage() {
        String usage =
            "\n" + BulkImporter.class.getSimpleName() + " - Import data into Honeycomb.\n" +
                "Takes the input directory, the MySQL database and table seperated by a forward slash, and the columns in \n" +
                "input file order" +
                "\nThe following properties must be configured in honeycomb.xml, hbase-site.xml, or via the command line:\n" +
                "\t" + ConfigConstants.TABLE_NAME + "\tHBase table\n" +
                "\t" + ConfigConstants.COLUMN_FAMILY + "\tHBase column family\n" +
                "\t" + ConfigConstants.PRE_SPLIT + "\t[boolean] (default "
                    + ConfigConstants.DEFAULT_PRE_SPLIT + ") whether to pre split the HBase table\n" +
                "\t" + ConfigConstants.INPUT_SEPARATOR + "\t[char] (default '"
                    + ConfigConstants.DEFAULT_INPUT_SEPARATOR + "') field separator in input files\n" +
                "\t" + ConfigConstants.OUTPUT_PATH + "\t[path] (default \""
                    + ConfigConstants.DEFAULT_OUTPUT_PATH +
                    "\") directory to store intermediate files\n" +
                "\t" + HConstants.ZOOKEEPER_QUORUM + "\tZookeeper quorum list" +
                "example:\n\n" +
                BulkImporter.class.getName() + " -D " + ConfigConstants.TABLE_NAME
                    + "=sql " + "hdfs:///path/to/input database_name/table_name col1 col2 col3\n";
        System.err.println(usage);
    }
}
