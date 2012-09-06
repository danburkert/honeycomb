package com.nearinfinity.datacreator;

import com.github.javafaker.Faker;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DataCreator {

    private static class DataMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
            Faker faker = new Faker();
            for (int i = 0 ; i < 1000000 ; i++) {
                StringBuilder sb = new StringBuilder();
                sb.append(faker.name());
                sb.append(',');
                sb.append(faker.streetAddress(false));
                sb.append(',');
                sb.append(faker.phoneNumber());

                context.write(new Text(sb.toString()), new Text(""));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: DataCreator <input_path> <output_path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(DataCreator.class);
        job.setJobName("Data Creator");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(DataMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
