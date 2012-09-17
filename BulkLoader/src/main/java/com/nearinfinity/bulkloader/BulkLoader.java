package com.nearinfinity.bulkloader;

import com.nearinfinity.hbaseclient.ColumnMetadata;
import com.nearinfinity.hbaseclient.HBaseClient;
import com.nearinfinity.hbaseclient.PutListFactory;
import com.nearinfinity.hbaseclient.TableInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

public class BulkLoader {
    private static final Log LOG = LogFactory.getLog(com.nearinfinity.bulkloader.BulkLoader.class);

    public enum Counters {ROWS, FAILED_ROWS}

    static class BulkLoaderMapper
            extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        private TableInfo tableInfo = null;
        private String[] columnNames = null;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            String zkQuorum = conf.get("zk_quorum");
            String sqlTableName = conf.get("sql_table_name");
            String hbaseTableName = conf.get("hb_table");

            HBaseClient client = new HBaseClient(hbaseTableName, zkQuorum);

            tableInfo = client.getTableInfo(sqlTableName);
            columnNames = conf.get("my_columns").split(",");
        }

        @Override
        public void map(LongWritable offset, Text line, Context context) {
            try {
                // NOTE:  the trim below is a stopgap to get rid of the trailing \t that mapreduce inserts
                //        in the DataCreator.  It should be replaced when we update the CSV parsing.
                String[] columnData = line.toString().trim().split(",");

                if (columnData.length != columnNames.length) {
                    throw new Exception("Row has wrong number of columns. Expected " +
                            columnNames.length + " got " + columnData.length + ". Line: " + line.toString());
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

                java.util.List<Put> puts = PutListFactory.createPutList(valueMap, tableInfo);

                for (Put put : puts) {
                    context.write(new ImmutableBytesWritable(put.getRow()), put);
                }

                context.getCounter(Counters.ROWS).increment(1);
            } catch (Exception e) {
                Writer traceWriter = new StringWriter();
                PrintWriter printWriter = new PrintWriter(traceWriter);
                e.printStackTrace(printWriter);
                context.setStatus(e.getMessage() + ". See logs for details. Stack Trace: " + traceWriter.toString());
                context.getCounter(Counters.FAILED_ROWS).increment(1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Check for the correct number of arguments supplied
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

        // Get arguments and setup configuration variables
        String inputPath = args[0];
        String outputPath = "bulk_loader_output_" + System.currentTimeMillis() + ".tmp";

        Configuration conf = HBaseConfiguration.create();
        conf.set("sql_table_name", args[1]);
        conf.set("my_columns", args[2]);
        conf.set("hb_family", params.get("hbase_family"));
        conf.set("zk_quorum", params.get("zk_quorum"));
        conf.set("hb_table", params.get("hbase_table_name"));

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

        if(job.waitForCompletion(true)) {
            LoadIncrementalHFiles fileLoader = new LoadIncrementalHFiles(conf);
            fileLoader.doBulkLoad(new Path(outputPath), table);

            long count = job.getCounters().findCounter(Counters.ROWS).getValue();
            HBaseClient client = new HBaseClient(conf.get("hb_table"), conf.get("zk_quorum"));
            client.incrementRowCount(conf.get("sql_table_name"), count);
        }

        // Delete temporary output folder after completion
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.delete(new Path(outputPath), true);
        fileSystem.close();
    }
}
