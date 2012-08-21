package com.nearinfinity.mysqlengine.readstrategies;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;

public interface Index {
    ResultScanner getSecondaryIndexScanner(String tableName, String columnName, byte[] value) throws IOException;

    byte[] parseValueFromSecondaryIndexRow(String tableName, String columnName, Result indexResult) throws IOException;

    ResultScanner getValueIndexScanner(String tableName, String columnName, byte[] nextValue) throws IOException;

    ResultScanner getReverseIndexScanner(String tableName, String columnName, byte[] value) throws IOException;

    byte[] parseValueFromReverseIndexRow(String tableName, String columnName, Result indexResult) throws IOException;

    ResultScanner getSecondaryIndexScannerExact(String tableName, String columnName, byte[] value) throws IOException;

    ResultScanner getSecondaryIndexScannerFull(String tableName, String columnName) throws IOException;

    ResultScanner getReverseIndexScannerFull(String tableName, String columnName) throws IOException;

    ResultScanner getNullIndexScanner(String tableName, String columnName) throws IOException;
}
