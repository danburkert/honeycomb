package com.nearinfinity.bulkloader;

import au.com.bytecode.opencsv.CSVReader;
import com.nearinfinity.hbaseclient.ColumnMetadata;
import com.nearinfinity.hbaseclient.HBaseClient;
import com.nearinfinity.hbaseclient.PutListFactory;
import com.nearinfinity.hbaseclient.TableInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.util.*;

import static java.lang.String.format;

public class BulkLoader extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(BulkLoader.class);

    public enum Counters {ROWS, FAILED_ROWS}

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BulkLoader(), args);
        System.exit(exitCode);
    }

    public static List<Put> createPuts(Text line, TableInfo tableInfo, String[] columnNames) throws IOException, ParseException {
        CSVReader reader = new CSVReader(new StringReader(line.toString()));
        String[] columnData = reader.readNext();

        if (columnData.length != columnNames.length) {
            throw new IllegalStateException(format("Row has wrong number of columns. Expected %d got %d. Line: %s", columnNames.length, columnData.length, line.toString()));
        }

        Map<String, byte[]> valueMap = new TreeMap<String, byte[]>();

        String name;
        byte[] val;
        ColumnMetadata meta;
        for (int i = 0; i < columnData.length; i++) {
            name = columnNames[i];
            meta = tableInfo.getColumnMetadata(name);
            val = ValueParser.parse(columnData[i], meta);
            valueMap.put(name, val);
        }

        return PutListFactory.createPutList(valueMap, tableInfo);
    }

    public static TableInfo extractTableInfo(Configuration conf) throws IOException {
        String zkQuorum = conf.get("zk_quorum");
        String sqlTableName = conf.get("sql_table_name");
        String hbaseTableName = conf.get("hb_table");

        HBaseClient client = new HBaseClient(hbaseTableName, zkQuorum);

        return client.getTableInfo(sqlTableName);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: com.nearinfinity.bulkloader.BulkLoader [generic arguments] <input path> <MySQL table name>" +
                    " <comma separated MySQL column names>");
            return -1;
        }

        Map<String, String> params = readConfigOptions();

        Configuration argConf = getConf();
        Configuration conf = HBaseConfiguration.create();
        HBaseConfiguration.merge(conf, argConf);

        updateConfiguration(conf, args, params);

        HBaseAdmin admin = new HBaseAdmin(conf);

        HBaseAdminColumn.addDummyFamily(admin);

        createSamplingJob(conf);

        createSplits(conf, admin);

        createBulkLoadJob(conf);

        deleteDummyData(conf);

        HBaseAdminColumn.deleteDummyFamily(admin);

        return 0;
    }

    private static void printQuorum(Job job) {
        LOG.info(format("*** Using quorum %s ***", job.getConfiguration().get("hbase.zookeeper.quorum")));
    }

    private static void createSamplingJob(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
        LOG.info("Setting up the sampling job");
        String outputPath = conf.get("output_path");
        String inputPath = conf.get("input_path");
        int columnCount = conf.getInt(SamplingPartitioner.COLUMN_COUNT, 3);
        Job job = new Job(conf, "Sample data in " + inputPath);
        job.setJarByClass(SamplingMapper.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(SamplingMapper.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setNumReduceTasks(2 * columnCount + 1);
        TableMapReduceUtil.initTableReducerJob(conf.get("hb_table"), SamplingReducer.class, job, SamplingPartitioner.class);
        printQuorum(job);
        job.waitForCompletion(true);
        deletePath(conf, outputPath);
    }

    private static void createBulkLoadJob(Configuration conf) throws Exception {
        String outputPath = conf.get("output_path");
        String inputPath = conf.get("input_path");
        HTable table = new HTable(conf, conf.get("hb_table"));

        Job job = new Job(conf, "Import from file " + inputPath + " into table " + conf.get("hb_table"));

        job.setJarByClass(BulkLoader.class);
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(BulkLoaderMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setReducerClass(PutSortReducer.class);
        Path outputDir = new Path(outputPath);
        FileOutputFormat.setOutputPath(job, outputDir);

        HFileOutputFormat.configureIncrementalLoad(job, table);

        printQuorum(job);
        if (job.waitForCompletion(true)) {
            SequentialLoadIncrementalHFiles fileLoader = new SequentialLoadIncrementalHFiles(conf);
            fileLoader.doBulkLoad(outputDir, table);
            long count = job.getCounters().findCounter(Counters.ROWS).getValue();
            HBaseClient client = new HBaseClient(conf.get("hb_table"), conf.get("zk_quorum"));
            client.incrementRowCount(conf.get("sql_table_name"), count);
        } else {
            LOG.error("*** Bulk load job failed during run. Not loading data into HBase.***");
        }

        deletePath(conf, outputPath);
    }

    private static void deleteDummyData(Configuration conf) throws IOException {
        HTable table = new HTable(conf, conf.get("hb_table"));
        ResultScanner splitScanner = table.getScanner("dummy".getBytes());
        List<Delete> deleteList = new LinkedList<Delete>();
        for (Result result : splitScanner) {
            deleteList.add(new Delete(result.getRow()));
        }

        table.delete(deleteList);
    }

    private static void createSplits(Configuration conf, HBaseAdmin admin) throws IOException {
        String tableName = conf.get("hb_table");
        HTable table = new HTable(conf, tableName);
        ResultScanner splitScanner = table.getScanner("dummy".getBytes());
        List<byte[]> retrySplits = new LinkedList<byte[]>();
        for (Result result : splitScanner) {
            try {
                admin.split(tableName.getBytes(), result.getRow());
            } catch (Exception e) {
                retrySplits.add(result.getRow());
                LOG.warn("Split " + Bytes.toStringBinary(result.getRow()) + " threw exception");
            }
        }

        if (!retrySplits.isEmpty()) {
            retryFailedSplits(admin, tableName, retrySplits);
        }
    }

    private static void retryFailedSplits(HBaseAdmin admin, String tableName, List<byte[]> retrySplits) {
        int retryCount = 3;
        for (int x = 0; x < retrySplits.size(); x++) {
            try {
                LOG.info("Retrying split " + Bytes.toStringBinary(retrySplits.get(x)));
                admin.split(tableName.getBytes(), retrySplits.get(x));
            } catch (Exception e) {
                retryCount--;
                if (retryCount == 0) {
                    LOG.info(format("Skipping %s too many failed retries.", Bytes.toStringBinary(retrySplits.get(x))));
                    retryCount = 3;
                } else {
                    x--;
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }
    }

    private static void deletePath(Configuration conf, String path) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.delete(new Path(path), true);
        fileSystem.close();
    }

    private static void updateConfiguration(Configuration conf, String[] args, Map<String, String> params) throws IOException {
        String inputPath = args[0];
        String sqlTable = args[1];
        String columns = args[2];
        int columnCount = columns.split(",").length;
        conf.setIfUnset("sql_table_name", sqlTable);
        conf.setIfUnset("my_columns", columns);
        conf.setIfUnset("hb_family", params.get("hbase_family"));
        conf.setIfUnset("zk_quorum", params.get("zk_quorum"));
        if ("localhost".equalsIgnoreCase(conf.get("hbase.zookeeper.quorum"))) {
            conf.set("hbase.zookeeper.quorum", params.get("zk_quorum"));
        } else {
            conf.setIfUnset("hbase.zookeeper.quorum", params.get("zk_quorum"));
        }
        conf.setIfUnset("hb_table", params.get("hbase_table_name"));
        conf.setIfUnset("output_path", "bulk_loader_output_" + System.currentTimeMillis() + ".tmp");
        conf.setIfUnset("input_path", inputPath);

        SamplingPartitioner.setColumnCount(conf, columnCount);
        updateDataInfo(conf, inputPath);
    }

    private static void updateDataInfo(Configuration conf, String inputPath) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);
        ContentSummary summary = fileSystem.getContentSummary(new Path(inputPath));
        long dataLength = summary.getLength();
        fileSystem.close();
        conf.setLong("data_size", dataLength);

        long hfileSize = SamplingReducer._1GB;
        long hfilesExpected = calculateExpectedHFileCount(dataLength, hfileSize);

        conf.setLong("hbase.hregion.max.filesize", hfileSize);

        LOG.info(format("HBase HFile size %s", conf.get("hbase.hregion.max.filesize")));
        LOG.info(format("Size of data to load %d", dataLength));
        LOG.info(format("*** Expected number hfiles created: %d per reducer/%d total. ***", hfilesExpected, hfilesExpected * 3));

        conf.setLong("hfiles_expected", hfilesExpected);
        long sampleSize = Math.round(dataLength * 0.30);
        double samplePercent = sampleSize / (float) dataLength;
        conf.setIfUnset("sample_size", Long.toString(sampleSize));
        conf.set("sample_percent", Double.toString(samplePercent));

        LOG.info(format("Sample size %d, Data size %d, Sample Percent %f", sampleSize, dataLength, samplePercent));
    }

    public static long calculateExpectedHFileCount(long dataSize, long hfileSize) {
        long expandedSize = 62 * dataSize;
        if (hfileSize > expandedSize) {
            return 1;
        }

        long expectedHFiles = expandedSize / hfileSize;
        long hfilesPerPartition = expectedHFiles / 3;
        return 10 * Math.max(hfilesPerPartition, 1);
    }

    private static Map<String, String> readConfigOptions() throws FileNotFoundException {
        //Read config options from adapter.conf
        Scanner confFile = new Scanner(new File("/etc/mysql/adapter.conf"));
        Map<String, String> params = new TreeMap<String, String>();
        while (confFile.hasNextLine()) {
            Scanner line = new Scanner(confFile.nextLine());
            params.put(line.next(), line.next());
        }
        return params;
    }
}
