package com.nearinfinity.hbaseclient.strategy;

import com.nearinfinity.hbaseclient.ScanFactory;
import com.nearinfinity.hbaseclient.TableInfo;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Created with IntelliJ IDEA.
 * User: jedstrom
 * Date: 8/21/12
 * Time: 8:10 AM
 * To change this template use File | Settings | File Templates.
 */
public class ScanStrategyBase implements ScanStrategy {
    protected String tableName;
    protected String columnName;
    protected byte[] value;

    public ScanStrategyBase(String tableName, String columnName, byte[] value) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.value = value;
    }

    @Override
    public Scan getScan(TableInfo info) {
        return ScanFactory.buildScan();
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }
}
