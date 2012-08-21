package com.nearinfinity.mysqlengine;

import com.nearinfinity.mysqlengine.jni.IndexReadType;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/14/12
 * Time: 11:08 AM
 * To change this template use File | Settings | File Templates.
 */
public class IndexConnection implements Connection {
    private String tableName;
    private ResultScanner scanner;
    private int currentIndex;
    private String columnName;
    private ResultScanner indexScanner;
    private IndexReadType readType;
    private Result lastResult;
    private boolean isNullScan;
    private boolean hasValues;

    public IndexConnection(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.currentIndex = 0;
        this.scanner = null;
        this.indexScanner = null;
        this.readType = IndexReadType.HA_READ_KEY_EXACT;
        this.lastResult = null;
        this.hasValues = true;
    }

    public String getTableName() {
        return this.tableName;
    }

    public Result getLastResult() {
        return this.lastResult;
    }

    public Result getNextResult() throws IOException {
        if (!this.hasValues)
        {
            return null;
        }

        if(this.isNullScan())
        {
            return this.getNextIndexResult();
        }

        Result result = this.scanner.next();
        this.lastResult = result;
        return result;
    }

    public Result getNextIndexResult() throws IOException {
        return this.indexScanner.next();
    }

    public void close() {
        if (this.scanner != null) {
            this.scanner.close();
        }
        if (this.indexScanner != null) {
            this.indexScanner.close();
        }
    }

    public String getColumnName() {
        return this.columnName;
    }

    public void setScanner(ResultScanner scanner) {
        if (this.scanner != null) {
            this.scanner.close();
        }
        this.scanner = scanner;
    }

    public void setIndexScanner(ResultScanner indexScanner) {
        if (this.indexScanner != null) {
            this.indexScanner.close();
        }
        this.indexScanner = indexScanner;
    }

    public void setReadType(IndexReadType readType) {
        this.readType = readType;
    }

    public IndexReadType getReadType() {
        return this.readType;
    }

    public boolean isNullScan() {
        return this.isNullScan;
    }

    public void setNullScan(boolean isNullScan) {
        this.isNullScan = isNullScan;
    }

    public void setOutOfValues() {
        this.hasValues = false;
    }
}
