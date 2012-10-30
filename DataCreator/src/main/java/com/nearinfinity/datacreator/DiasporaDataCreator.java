package com.nearinfinity.datacreator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class DiasporaDataCreator extends Configured implements Tool {
    private static final DateFormat DF = new SimpleDateFormat("yyyyMMdd-HHmmss");

    private static String buildOutputPath(String numRows) {
        return "DiasporaDataCreator/output/" + DF.format(new Date()) + "_" + numRows;
    }

    private static Path createInputFolder(int numMappers, Configuration conf) throws Exception {
        Path path = new Path("DiasporaDataCreator/input/" + System.currentTimeMillis());
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.mkdirs(path);

        FSDataOutputStream os;
        for (int i = 0; i < numMappers; i++) {
            os = fileSystem.create(path.suffix("/" + i));
            os.writeBytes(i+"\n");
        }
        fileSystem.close();
        return path;
    }

    private static String generateUserList(File userFile) throws IOException {
        StringBuilder sb = new StringBuilder();

        Scanner scan = new Scanner(userFile);
        while (scan.hasNextLine()) {
            Scanner line = new Scanner(scan.nextLine());
            sb.append(line.next());
            sb.append(";");
            sb.append(line.next());
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);

        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new DiasporaDataCreator(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: com.nearinfinity.datacreator.DiasporaDataCreator <mappers> <user_file> <num_rows>");
            return -1;
        }

        int numMappers = Integer.parseInt(args[0]);
        File userFile = new File(args[1]);
        String numRows = args[2];
        String outputPath = buildOutputPath(numRows);

        Configuration conf = getConf();
        conf.set("numRows", numRows);
        conf.set("users", generateUserList(userFile));

        Job job = new Job(conf);
        job.setJarByClass(DiasporaDataCreator.class);
        job.setJobName("Diaspora Data Creator");

        Path inputPath = createInputFolder(numMappers, conf);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(DiasporaDataMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        if (!job.waitForCompletion(true)) {
            System.out.println("Error in running the job!");
            System.exit(-1);
        }

        System.out.println("Successfully completed job with output path: " + outputPath);

        // Delete temporary output folder after completion
//        FileSystem fileSystem = FileSystem.get(conf);
//        fileSystem.delete(inputPath, true);
//        fileSystem.close();

        return 0;
    }
}
