package com.nearinfinity.mysqlengine;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/1/12
 * Time: 10:23 AM
 * To change this template use File | Settings | File Templates.
 */
public interface Connection {

    public String getTableName();

    public Result getLastResult();

    public Result getNextResult() throws IOException;

    public void close();

}
