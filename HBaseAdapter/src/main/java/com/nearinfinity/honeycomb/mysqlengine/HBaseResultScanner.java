package com.nearinfinity.honeycomb.mysqlengine;

import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;

public interface HBaseResultScanner {
    public Result next(byte[] valueToSkip) throws IOException;

    public void close();

    public Result getLastResult();

    public void setLastResult(Result result);

    public void setColumnName(String columnName);
}
