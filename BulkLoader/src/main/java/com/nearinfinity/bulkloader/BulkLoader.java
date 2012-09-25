package com.nearinfinity.bulkloader;

import au.com.bytecode.opencsv.CSVReader;
import com.nearinfinity.hbaseclient.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

import static java.lang.String.format;

public class BulkLoader {
    private static final Log LOG = LogFactory.getLog(BulkLoader.class);

    public enum Counters {ROWS, FAILED_ROWS}

    public static List<Put> createPuts(Text line, Mapper.Context context, TableInfo tableInfo, String[] columnNames) throws Exception {
        CSVReader reader = new CSVReader(new StringReader(line.toString()));
        String[] columnData = reader.readNext();

        if (columnData.length != columnNames.length) {
            throw new Exception(format("Row has wrong number of columns. Expected %d got %d. Line: %s", columnNames.length, columnData.length, line.toString()));
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

    private static void addDummyFamily(HBaseAdmin admin) throws IOException, InterruptedException {
        HColumnDescriptor dummyColumn = new HColumnDescriptor(SampleReducer.DUMMY_FAMILY);
        HTableDescriptor sqlTableDescriptor = admin.getTableDescriptor(Constants.SQL);
        if (!sqlTableDescriptor.hasFamily(SampleReducer.DUMMY_FAMILY)) {
            if (!admin.isTableDisabled(Constants.SQL)) {
                admin.disableTable(Constants.SQL);
            }

            admin.addColumn(Constants.SQL, dummyColumn);
        }

        if (admin.isTableDisabled(Constants.SQL)) {
            admin.enableTable(Constants.SQL);
        }

        admin.flush(Constants.SQL);
    }

    private static void deleteDummyFamily(HBaseAdmin admin) throws IOException, InterruptedException {
        HTableDescriptor sqlTableDescriptor = admin.getTableDescriptor(Constants.SQL);
        if (!sqlTableDescriptor.hasFamily(SampleReducer.DUMMY_FAMILY)) {
            if (!admin.isTableDisabled(Constants.SQL)) {
                admin.disableTable(Constants.SQL);
            }

            admin.deleteColumn(Constants.SQL, SampleReducer.DUMMY_FAMILY);
        }

        if (admin.isTableDisabled(Constants.SQL)) {
            admin.enableTable(Constants.SQL);
        }

        admin.flush(Constants.SQL);
    }

    public static void createSamplingJob(Configuration conf, String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        int columnCount = conf.get("my_columns").split(",").length;
        SamplePartitioner.setColumnCount(conf, columnCount);
        FileSystem fileSystem = FileSystem.get(conf);
        ContentSummary summary = fileSystem.getContentSummary(new Path(inputPath));
        long dataLength = summary.getLength();
        conf.setLong("data_size", dataLength);
        fileSystem.close();
        conf.setLong("sample_size", dataLength / 10);
        Job job = new Job(conf, "Sample data in " + inputPath);
        job.setJarByClass(SampleMapper.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(SampleMapper.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setNumReduceTasks(2 * columnCount + 1);
        TableMapReduceUtil.initTableReducerJob(conf.get("hb_table"), SampleReducer.class, job, SamplePartitioner.class);
        job.waitForCompletion(true);
        deletePath(conf, outputPath);
    }

    private static void createBulkLoadJob(Configuration conf, String inputPath, String outputPath) throws Exception {
        HTable table = new HTable(conf, conf.get("hb_table"));

        Job job = new Job(conf, "Import from file " + inputPath + " into table " + conf.get("hb_table"));

        job.setJarByClass(BulkLoader.class);
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(BulkLoaderMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setReducerClass(PutSortReducer.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        HFileOutputFormat.configureIncrementalLoad(job, table);

        if (job.waitForCompletion(true)) {
            LoadIncrementalHFiles fileLoader = new LoadIncrementalHFiles(conf);
            fileLoader.doBulkLoad(new Path(outputPath), table);

            long count = job.getCounters().findCounter(Counters.ROWS).getValue();
            HBaseClient client = new HBaseClient(conf.get("hb_table"), conf.get("zk_quorum"));
            client.incrementRowCount(conf.get("sql_table_name"), count);
        }

        // Delete temporary output folder after completion
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
        int splitCount = 0;
        for (Result result : splitScanner) {
            try {
                splitCount++;
                admin.split(tableName.getBytes(), result.getRow());
            } catch (Exception e) {
                LOG.warn("Split " + Bytes.toStringBinary(result.getRow()) + " threw exception", e);
            }
        }

        LOG.info("Splits " + splitCount);
    }

    private static void deletePath(Configuration conf, String path) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.delete(new Path(path), true);
        fileSystem.close();
    }

    public static void main(String[] args) throws Exception {
        // Check for the correct number of arguments supplied
        if (args.length != 3) {
            System.err.println("Usage: com.nearinfinity.bulkloader.BulkLoader <input path> <MySQL table name>" +
                    " <comma separated MySQL column names>");
            System.exit(-1);
        }

        //Read config options from adapter.conf
        Scanner confFile = new Scanner(new File("/etc/mysql/adapter.conf"));
        Map<String, String> params = new TreeMap<String, String>();
        while (confFile.hasNextLine()) {
            Scanner line = new Scanner(confFile.nextLine());
            params.put(line.next(), line.next());
        }

        // Get arguments and setup configuration variables
        String inputPath = args[0];
        String outputPath = "bulk_loader_output_" + System.currentTimeMillis() + ".tmp";

        Configuration conf = HBaseConfiguration.create();
        conf.set("sql_table_name", args[1]);
        conf.set("my_columns", args[2]);
        conf.set("hb_family", params.get("hbase_family"));
        conf.set("zk_quorum", params.get("zk_quorum"));
        conf.set("hb_table", params.get("hbase_table_name"));
        conf.set("region_size", params.get("region_size"));

        HBaseAdmin admin = new HBaseAdmin(conf);

        addDummyFamily(admin);

        createSamplingJob(conf, inputPath, outputPath);

        createSplits(conf, admin);

        createBulkLoadJob(conf, inputPath, outputPath);

        deleteDummyData(conf);

        deleteDummyFamily(admin);
    }
}
