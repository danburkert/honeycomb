package com.nearinfinity.honeycombserde;

import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class HoneyCombInputFormat extends HiveInputFormat<LongWritable, MapWritable> {
    @Override
    public RecordReader<LongWritable, MapWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        return new HoneyCombReader();
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        return super.getSplits(job, numSplits);    //To change body of overridden methods use File | Settings | File Templates.
    }
}
