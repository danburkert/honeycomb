package com.nearinfinity.bulkloader;

import com.nearinfinity.hbaseclient.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.Scanner;

public class BulkLoader {
    private static final Log LOG = LogFactory.getLog(com.nearinfinity.bulkloader.BulkLoader.class);

    public enum Counters { ROWS, FAILED_ROWS }

    static class BulkLoaderMapper
            extends Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {

        private byte[] family = null;
        private TableInfo table_info = null;
        private String[] column_names = null;
        private final ValueTransformer valueTransformer = new ValueTransformer();


        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            String zk_quorum = conf.get("zk_quorum");
            String my_table = conf.get("my_table");
            String hb_table = conf.get("hb_table");

            HBaseClient client = new HBaseClient(hb_table, zk_quorum);

            table_info = client.getTableInfo(my_table);
            family = conf.get("hb_family").getBytes();
            column_names = conf.get("my_columns").split(",");
        }

        @Override
        public void map(LongWritable offset, Text line, Context context)
                throws IOException {
            try {
                String[] column_data = line.toString().split(",");

                if(column_data.length != column_names.length) {
                    System.err.println("Row has wrong number of columns. Expected " +
                        column_names.length + " got " + column_data.length + ". Line: " + line.toString());
                    context.setStatus("Row with wrong number of columns: see logs.");
                    context.getCounter(Counters.FAILED_ROWS).increment(1);
                }

                Map<String, byte []> value_map = new TreeMap<String, byte []>();

                String name;
                byte[] val;
                ColumnType t;
                for (int i = 0; i < column_data.length; i++) {
                    name = column_names[i];
                    t = table_info.getColumnTypeByName(name);
                    val = ValueTransformer.transform(column_data[i], t);
                    value_map.put(name, val);
                }

                java.util.List<Put> puts = PutListFactory.createPutList(value_map, table_info);

                for (Put put : puts) {
                    context.write(new ImmutableBytesWritable(put.getRow()), put);
                }

                context.getCounter(Counters.ROWS).increment(1);
            } catch (Exception e) {
                e.printStackTrace();
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

        String hb_table = params.get("hbase_table_name");
        String hb_family = params.get("hbase_family");
        String zk_quorum = params.get("zk_quorum");

        String input_path = args[0];
        String my_table = args[1];
        String my_columns = args[2];

        Configuration conf = HBaseConfiguration.create();

        conf.set("hb_family", hb_family);
        conf.set("hb_table", hb_table);
        conf.set("my_table", my_table);
        conf.set("zk_quorum", zk_quorum);
        conf.set("my_columns", my_columns);

        Job job = new Job(conf, "Import from file " + input_path + " into table " + hb_table);
        job.setJarByClass(com.nearinfinity.bulkloader.BulkLoader.class);
        job.setMapperClass(BulkLoaderMapper.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, hb_table);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Writable.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(input_path));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
