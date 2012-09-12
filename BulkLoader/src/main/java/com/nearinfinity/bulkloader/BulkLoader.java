package com.nearinfinity.bulkloader;

import com.nearinfinity.hbaseclient.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.sql.Time;
import java.util.Map;
import java.util.TreeMap;
import java.util.Scanner;

public class BulkLoader {
    private static final Log LOG = LogFactory.getLog(com.nearinfinity.bulkloader.BulkLoader.class);

    public enum Counters { ROWS, FAILED_ROWS }

    static class BulkLoaderMapper
            extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        private byte[] family = null;
        private TableInfo tableInfo = null;
        private String[] columnNames = null;
        private final ValueTransformer valueTransformer = new ValueTransformer();


        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            String zkQuorum = conf.get("zk_quorum");
            String sqlTableName = conf.get("sql_table_name");
            String hbaseTableName = conf.get("hb_table");

            HBaseClient client = new HBaseClient(hbaseTableName, zkQuorum);

            tableInfo = client.getTableInfo(sqlTableName);
            family = conf.get("hb_family").getBytes();
            columnNames = conf.get("my_columns").split(",");
        }

        @Override
        public void map(LongWritable offset, Text line, Context context) {
            try {
                String[] columnData = line.toString().split(",");

                if(columnData.length != columnNames.length) {
                    throw new Exception("Row has wrong number of columns. Expected " +
                        columnNames.length + " got " + columnData.length + ". Line: " + line.toString());
                }

                Map<String, byte []> valueMap = new TreeMap<String, byte []>();

                String name;
                byte[] val = null;
                ColumnMetadata m;
                for (int i = 0; i < columnData.length; i++) {
                    name = columnNames[i];
                    m = tableInfo.getColumnMetadata(name);
                    val = ValueTransformer.transform(columnData[i], m);
                    valueMap.put(name, val);
                }

                java.util.List<Put> puts = PutListFactory.createPutList(valueMap, tableInfo);

                for (Put put : puts) {
                    context.write(new ImmutableBytesWritable(put.getRow()), put);
                }

                context.getCounter(Counters.ROWS).increment(1);
            } catch (Exception e) {
                Writer trace_writer = new StringWriter();
                PrintWriter print_writer = new PrintWriter(trace_writer);
                e.printStackTrace(print_writer);
                context.setStatus(e.getMessage() + ". See logs for details. Stack Trace: " + trace_writer.toString());
                context.getCounter(Counters.FAILED_ROWS).increment(1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: com.nearinfinity.bulkloader.BulkLoader <input path> <MySQL table name>" +
                    " <comma seperated MySQL column names>");
            System.exit(-1);
        }

        //Read config options from adapter.conf
        Scanner confFile = new Scanner(new File("/etc/mysql/adapter.conf"));
        Map<String, String> params = new TreeMap<String, String>();
        while (confFile.hasNextLine()) {
            Scanner line = new Scanner(confFile.nextLine());
            params.put(line.next(), line.next());
        }

        String inputPath = args[0];
        String sqlTableName = args[1];
        String columnNames = args[2];
        String outputPath = "bulk_loader_output_" + System.currentTimeMillis() + ".tmp";

        String hbaseTableName = params.get("hbase_table_name");
        String hbaseFamilyName = params.get("hbase_family");
        String zkQuorum = params.get("zk_quorum");

        Configuration conf = HBaseConfiguration.create();

        conf.set("hb_family", hbaseFamilyName);
        conf.set("hb_table", hbaseTableName);
        conf.set("sql_table_name", sqlTableName);
        conf.set("zk_quorum", zkQuorum);
        conf.set("my_columns", columnNames);

        HTable table = new HTable(conf, hbaseTableName);
        Job job = new Job(conf, "Import from file " + inputPath + " into table " + hbaseTableName);

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
        }

        // Delete temporary output folder after completion
        FileUtils.deleteDirectory(new File(outputPath));
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.delete(new Path(outputPath), true);
        fileSystem.close();
  }
}
