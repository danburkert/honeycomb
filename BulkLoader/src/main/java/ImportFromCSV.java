import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class ImportFromCSV {
    private static final Log LOG = LogFactory.getLog(ImportFromCSV.class);

    public enum Counters { LINES }

    static class CSVMapper
            extends Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {

        private byte[] family = null;
        private byte[] qualifier = null;


        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            String column = context.getConfiguration().get("conf.column");
            family = "nic".getBytes();
            qualifier = column.getBytes();
        }

        @Override
        public void map(LongWritable offset, Text line, Context context)
                throws IOException {
            try {
                String lineString = line.toString();
                System.out.println("Mapper: " + lineString);
                byte[] rowkey = DigestUtils.md5(lineString);
                Put put = new Put(rowkey);
                put.add(family, qualifier, Bytes.toBytes(lineString));
                context.write(new ImmutableBytesWritable(rowkey), put);
                context.getCounter(Counters.LINES).increment(1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: ImportFromCSV <input path>");
            System.exit(-1);
        }

        Configuration conf = HBaseConfiguration.create();
        String table = "sql";
        String column = "boop";
        conf.set("conf.column", column);
        String input_path = args[0];

        Job job = new Job(conf, "Import from file " + input_path + " into table " + table);
        job.setJarByClass(ImportFromCSV.class);
        job.setMapperClass(CSVMapper.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Writable.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(input_path));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
