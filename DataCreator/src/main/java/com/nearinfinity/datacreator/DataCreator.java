package com.nearinfinity.datacreator;

import com.github.javafaker.Faker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class DataCreator {

    private static final String DATA_TYPE_LIST = "data_type_list";
    private static final String NUM_ROWS = "num_rows";
    private static final DateFormat DF = new SimpleDateFormat("yyyyMMdd-HHmmss");

    private static class DataCreatorMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Faker faker;
        private Random random;
        private DataType[] dataTypes;
        private int numRows;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            this.faker = new Faker();
            this.random = new Random();

            Configuration conf = context.getConfiguration();
            this.numRows = Integer.parseInt(conf.get(NUM_ROWS));

            String [] dataTypeStrings = conf.get(DATA_TYPE_LIST).split(",");
            this.dataTypes = getDataTypeArray(dataTypeStrings);
        }

        @Override
        public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
            for (int i = 0 ; i < numRows ; i++) {
                StringBuilder sb = new StringBuilder();
                for (int j = 0 ; j < dataTypes.length ; j++) {
                    if (j > 0) sb.append(",");
                    appendData(dataTypes[j], sb);
                }

                context.write(new Text(sb.toString()), new Text(""));
            }
        }

        private void appendData(DataType dataType, StringBuilder sb) {
            switch (dataType) {
                case NAME:
                    sb.append(faker.name());
                    break;
                case ADDRESS:
                    sb.append(faker.streetAddress(false));
                    break;
                case PHONE:
                    sb.append(faker.phoneNumber());
                    break;
                case LONG:
                    sb.append(random.nextLong());
                    break;
                case DOUBLE:
                    sb.append(random.nextDouble());
                    break;
            }
        }

        private DataType[] getDataTypeArray(String[] dataTypeStrings) {
            DataType[] dataTypes = new DataType[dataTypeStrings.length];
            for (int i = 0 ; i < dataTypeStrings.length ; i++) {
                dataTypes[i] = DataType.valueOf(dataTypeStrings[i].toUpperCase());
            }
            return dataTypes;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: DataCreator.jar <input_path> <num_rows> <data_type_list>");
            System.exit(-1);
        }
        String inputPath = args[0];
        String numRows = args[1];
        String dataTypeList = args[2];
        String outputPath = buildOutputPath(numRows);

        Configuration conf = new Configuration();
        conf.set(DATA_TYPE_LIST, dataTypeList);
        conf.set(NUM_ROWS, numRows);

        Job job = new Job(conf);
        job.setJarByClass(DataCreator.class);
        job.setJobName("Data Creator");

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(DataCreatorMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        if (!job.waitForCompletion(true)) {
            System.out.println("Error in running the job!");
            System.exit(1);
        }

        System.out.println("Successfully completed job with output path: " + outputPath);
    }

    private static String buildOutputPath(String numRows) {
        return "created_data/" + DF.format(new Date()) + "-" + numRows;
    }
}
