package com.nearinfinity.honeycombserde;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

public class HoneyCombReader implements RecordReader<LongWritable, MapWritable> {

    @Override
    public boolean next(LongWritable longWritable, MapWritable mapWritable) throws IOException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public LongWritable createKey() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public MapWritable createValue() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getPos() throws IOException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void close() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public float getProgress() throws IOException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
