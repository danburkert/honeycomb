package com.nearinfinity.hbaseclient.strategy;

import com.nearinfinity.hbaseclient.ScanFactory;
import com.nearinfinity.hbaseclient.TableInfo;
import org.apache.hadoop.hbase.client.Scan;

import java.util.List;

public class ScanStrategyBase implements ScanStrategy {
    protected String tableName;
    protected List<String> columnName;
    protected byte[] value;

    public ScanStrategyBase(String tableName, List<String> columnName, byte[] value) {
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
