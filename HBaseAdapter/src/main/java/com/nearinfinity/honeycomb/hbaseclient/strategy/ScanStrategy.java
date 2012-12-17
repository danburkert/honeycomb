package com.nearinfinity.honeycomb.hbaseclient.strategy;

import com.nearinfinity.honeycomb.hbaseclient.TableInfo;
import org.apache.hadoop.hbase.client.Scan;

public interface ScanStrategy {
    public Scan getScan(TableInfo info);

    public String getTableName();
}
