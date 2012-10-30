package com.nearinfinity.honeycombserde;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;

public class HoneyCombOutputFormat implements HiveOutputFormat<NullWritable, Row> {
    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf entries, Path path, Class<? extends Writable> aClass, boolean b, Properties properties, Progressable progressable) throws IOException {
        return new HoneyCombWriter();
    }

    @Override
    public RecordWriter<NullWritable, Row> getRecordWriter(FileSystem fileSystem, JobConf entries, String s, Progressable progressable) throws IOException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf entries) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
